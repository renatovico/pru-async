require 'json'
require 'uri'
require 'async'
require 'async/http/server'
require 'async/http/endpoint'
require "async/http/protocol/response"
require 'time'
require_relative 'app/logger'
require 'async/redis'
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
    result = @job_queue.enqueue do
      begin
        data = JSON.parse(body)
        correlation_id = data['correlationId']
        amount = data['amount']
        @payment_job.perform_now(correlation_id, amount, Time.now.iso8601(3))
      rescue => e
        Log.exception(e, 'payment_job_failed', correlation_id: correlation_id, backtrace: e.backtrace)
      end
    end
    if result
      json(200, { message: 'enqueued' })
    else
      json(502, { error: 'Queue in overflow' })
    end

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

  Log.debug('payments_summary_params', from: from_param, to: to_param)

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
  Log.info('server_start', port: port)
  job_queue = JobQueue.new(
        concurrency: (ENV['QUEUE_CONCURRENCY'] || '5192').to_i,
      )
  Async do |task|
    # Single shared Redis client per process:
    redis_endpoint = if defined?(Async::Redis::Endpoint) && Async::Redis::Endpoint.respond_to?(:parse)
      Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
    else
      Async::Redis.local_endpoint
    end
    redis_client = Async::Redis::Client.new(redis_endpoint)

    store = Store.new(redis_client: redis_client)

    payment_job = PaymentJob.new(store: store, redis_client: redis_client)


    # Start background workers once when reactor boots:
    endpoint = Async::HTTP::Endpoint.parse("http://0.0.0.0:#{port}")
    app = PruApp.new(store: store, job_queue: job_queue, payment_job: payment_job)

    server = Async::HTTP::Server.for(endpoint) do |request|
      # Basic access log for debugging routing:
      begin
        Log.debug('request', method: request.method, path: request.path)
        app.call(request)
      rescue => e
        Log.exception(e, 'request_log_error')
        # ignore logging issues
      end
    end

    begin
      server.run
      job_queue.start

    ensure
      job_queue.close
      redis_client.close
    end
  end
end
