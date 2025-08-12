require 'json'
require 'uri'
require 'async'
require 'async/http/server'
require 'async/http/endpoint'
require "async/http/protocol/response"
require 'time'
require_relative 'app/store'
require_relative 'app/payment_job'
require_relative 'app/job_queue'

class PruApp
  def initialize(store:, job_queue:, payment_job:)
    @store = store
    @job_queue = job_queue
    @payment_job = payment_job
  end

  # Handle an incoming Protocol::HTTP::Request and return Protocol::HTTP::Response
  def call(request)
    method = request.method
    path = URI.parse(request.path || '/').path

    case [method, path]
    when ['POST', '/payments']
      handle_payments(request)
    when ['GET', '/payments-summary']
      handle_payments_summary(request)
    when ['GET', '/health']
      json(200, { ok: true })
    when ['POST', '/purge-payments']
      handle_purge_payments(request)
    else
      json(404, { error: 'Not Found' })
    end
  end

  private

  def json(status, object)
    body = object.to_json
    headers = {
      'content-type' => 'application/json',
    }
    ::Protocol::HTTP::Response[status, headers, [body]]
  end

  def handle_payments(request)
    body = request.read
    return json(400, { error: 'Bad Request' }) if body.nil? || body.empty?

  # Enqueue work into background workers (outside the HTTP request fiber).
  enqueue_timeout = ((ENV['ENQUEUE_TIMEOUT_MS'] || '5').to_i / 1000.0)
  enqueued = @job_queue.enqueue_with_timeout(enqueue_timeout) do
      begin
        data = JSON.parse(body)
        correlation_id = data['correlationId']
        amount = data['amount']
    @payment_job.perform_now(correlation_id, amount, Time.now.iso8601(3))
      rescue => e
        puts "❌ Payment job failed: #{e.message}"
      end
    end

    return json(503, { error: 'Service Unavailable', message: 'Queue is full' }) unless enqueued

    json(200, { message: 'enqueued' })
  rescue JSON::ParserError
    json(400, { error: 'Invalid JSON' })
  rescue => e
    json(500, { error: 'Internal Server Error', details: e.message })
  end

  def handle_payments_summary(request)
    # request.path may include query: /payments-summary?from=...&to=...
    uri = URI.parse(request.path || '/payments-summary')
    params = uri.query ? URI.decode_www_form(uri.query).to_h : {}
    from_param = params['from']
    to_param = params['to']

    puts "🐦 Received query params: from=#{from_param}, to=#{to_param}"

    summary = @store.summary(from: from_param, to: to_param)
    json(200, summary)
  end

  def handle_purge_payments(_request)
    @store.purge_all
    json(200, { message: 'purged' })
  end
end

if __FILE__ == $0
  port = Integer(ENV.fetch('PORT', '3000'))
  endpoint = Async::HTTP::Endpoint.parse("http://0.0.0.0:#{port}")
  app = nil

  server = Async::HTTP::Server.for(endpoint) do |request|
    # Basic access log for debugging routing:
    begin
      puts "➡️  #{request.method} #{request.path}"
      app.call(request)

    rescue => e
      puts "❌ Error logging request: #{e.message}"
      # ignore logging issues
    end
  end

  puts "🐦 Pru server starting on port #{port}..."

  Async do |task|
    # Start background workers once when reactor boots:
  redis_pool = RedisPool.new
  store = Store.new(redis_pool: redis_pool)
  job_queue = JobQueue.new(capacity: 512)
  payment_job = PaymentJob.new(store: store, redis_pool: redis_pool)

  job_queue.start_workers(count: 512, parent_task: task)

  app = PruApp.new(store: store, job_queue: job_queue, payment_job: payment_job)
    server.run
  end
end
