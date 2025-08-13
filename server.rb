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
require_relative 'app/health_monitor'

class PruApp
  def initialize(store:, job_queue:, payment_job:)
    @store = store
    @job_queue = job_queue
    @payment_job = payment_job
    @route_status_counts = Hash.new { |h, k| h[k] = Hash.new(0) }
    @total_requests = 0
  @job_status = { 'success' => 0, 'error' => 0, 'in_process' => 0 }
  end

  # Handle an incoming Protocol::HTTP::Request and return Protocol::HTTP::Response
  def call(request)
    method = request.method
    path = URI.parse(request.path || '/').path
    # Route and obtain response
    response = case [method, path]
    when ['POST', '/payments']
      handle_payments(request)
    when ['GET', '/payment-status']
      handle_payment_status(request)
    when ['GET', '/payments-summary']
      handle_payments_summary(request)
    when ['GET', '/health']
      handle_health
    when ['POST', '/purge-payments']
      handle_purge_payments(request)
    else
      json(404, { error: 'Not Found' })
    end

    # Track counts per route and status code after we have the response
    begin
      status = response.status
      @route_status_counts[path][status] += 1
      @total_requests += 1
    rescue => e
      # If for any reason status can't be read, ignore metrics update
      Log.warn('metrics_update_failed', error: e.message)
    end

    response
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
    data = JSON.parse(body)
    correlation_id = data['correlationId']
    amount = data['amount']
    return json(400, { error: 'Missing correlationId' }) if correlation_id.nil? || correlation_id.to_s.empty?

    # if @payment_job.circuit_closed?
    #   Log.debug('circuit_closed', correlation_id: correlation_id)
    #   return json(503, { error: 'Service Unavailable' })
    # end


    requested_at = Time.now.iso8601(3)
    accepted = @store.begin_payment(correlation_id: correlation_id, amount: amount, requested_at: requested_at)
    unless accepted
      Log.warn('duplicate_payment_rejected', correlation_id: correlation_id)
      return json(409, { error: 'Duplicate correlationId', correlationId: correlation_id })
    end

    @store.set_status(correlation_id: correlation_id, status: 'processing')

    # Enqueue work into background workers (outside the HTTP request fiber).
    result = @job_queue.enqueue do
      begin
        @job_status['in_process'] += 1
        ok = @payment_job.perform_now(correlation_id, amount, requested_at)
        if ok
          @job_status['success'] += 1
        else
          @job_status['error'] += 1
        end
        true
      rescue => e
        Log.exception(e, 'payment_job_failed', correlation_id: correlation_id)
        @job_status['error'] += 1
        false
      ensure
        @job_status['in_process'] -= 1
        @job_status['in_process'] = 0 if @job_status['in_process'] < 0
      end
    end

    if result
      json(201, { message: 'Payment created', correlationId: correlation_id })
    else
      @store.set_status(correlation_id: correlation_id, status: 'failed')
      json(501, { error: 'Payment enqueue failed', correlationId: correlation_id })
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

    # Pause the queue and wait for all in-flight jobs to finish before summarizing.
    begin
      @job_queue.pause! if @job_queue.respond_to?(:pause!)

      # Wait until inflight == 0
      if @job_queue.respond_to?(:inflight)
        # Optional max wait via ENV to prevent indefinite block; default is no timeout.
        deadline = nil
        max_wait = (ENV['SUMMARY_MAX_WAIT'] || '0.3').to_f
        deadline = Time.now.to_f + max_wait if max_wait > 0

        loop do
          current = @job_queue.inflight
          break if current <= 0
          if deadline && Time.now.to_f >= deadline
            Log.warn('payments_summary_drain_timeout', inflight: current)
            break
          end
          Async::Task.current.sleep(0.01)
        end
      end

      summary = @store.summary(from: from_param, to: to_param)
      json(200, summary)
    ensure
      @job_queue.resume! if @job_queue.respond_to?(:resume!)
    end
  end

  def handle_purge_payments(_request)
    @store.purge_all
    json(200, { message: 'purged' })
  end

  def handle_health
    # Deep copy and normalize status code keys to strings for JSON stability
    routes = {}
    @route_status_counts.each do |route, statuses|
      routes[route] = {}
      statuses.each do |code, count|
        routes[route][code.to_s] = count
      end
    end

  queue_state = {}
  begin
    queue_state = {
      'paused' => (@job_queue.respond_to?(:paused?) ? @job_queue.paused? : false),
      'inflight' => (@job_queue.respond_to?(:inflight) ? @job_queue.inflight : 0),
      'total' => @job_queue.queue.size
    }
  rescue
    queue_state = { 'paused' => false, 'inflight' => 0 }
  end

  json(200, { ok: true, totalRequests: @total_requests, routes: routes, job_status: @job_status.dup, queue: queue_state })
  end

  def handle_payment_status(request)
    # /payment-status?correlationId=...
    uri = URI.parse(request.path || '/payment-status')
    params = uri.query ? URI.decode_www_form(uri.query).to_h : {}
    cid = params['correlationId']
    return json(400, { error: 'Missing correlationId' }) unless cid
    status = @store.get_status(cid)
    return json(404, { error: 'Not Found' }) if status.nil? || status.empty?
    json(200, status)
  end
end

if __FILE__ == $0
  Sync do
    port = Integer(ENV.fetch('PORT', '3000'))
    Log.info('server_start', port: port)

      # Single shared Redis client per process:
      redis_endpoint = if defined?(Async::Redis::Endpoint) && Async::Redis::Endpoint.respond_to?(:parse)
        Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
      else
        Async::Redis.local_endpoint
      end
      redis_client = Async::Redis::Client.new(redis_endpoint)

      store = Store.new(redis_client: redis_client)

      health_monitor = HealthMonitor.new(redis_client: redis_client)
      payment_job = PaymentJob.new(store: store, redis_client: redis_client, health_monitor: health_monitor)

      job_queue = JobQueue.new(
        concurrency: (ENV['QUEUE_CONCURRENCY'] || '128').to_i,
        redis_client: redis_client,
      )

      # Start background workers once when reactor boots:
      endpoint = Async::HTTP::Endpoint.parse("http://0.0.0.0:#{port}")
      app = PruApp.new(store: store, job_queue: job_queue, payment_job: payment_job)

      server = Async::HTTP::Server.for(endpoint) do |request|
        # Basic access log for debugging routing:
        begin
          Log.debug('request', method: request.method, path: request.path)
          app.call(request)
        rescue => e
          Log.warn('request_failed', method: request.method, path: request.path, error: e.message, backtrace: e.backtrace)
          # ignore logging issues
        end
      end

    Async(transient: true) do |task|
        server.run
    end

    Async do |task|
      job_queue.start
    end

    # Start health monitor background polling
    Async do
      health_monitor.start
    end
  end
end
