$stdout.sync = true

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

DEBUG = false
class PruApp
  def initialize(store:, job_queue:)
    @store = store
    @job_queue = job_queue
    @route_status_counts = Hash.new { |h, k| h[k] = Hash.new(0) }
    @total_requests = 0
    @total_duration_ms = 0.0
    @route_duration_ms = Hash.new(0.0)
  end

  # Handle an incoming Protocol::HTTP::Request and return Protocol::HTTP::Response
  def call(request)
    start_t = Process.clock_gettime(Process::CLOCK_MONOTONIC) if DEBUG
    method = request.method
    path = URI.parse(request.path || '/').path
    # Route and obtain response
    response = case [method, path]
    when ['POST', '/payments']
      handle_payments(request)
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
  ensure
    # Always record latency, even if routing raises later.
    if DEBUG
      begin
        elapsed_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_t) * 1000.0
        @total_duration_ms += elapsed_ms
        @route_duration_ms[path] += elapsed_ms
      rescue
        # ignore timing errors
      end
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
    data = JSON.parse(body)
    data['retries'] = 0
    correlation_id = data['correlationId']
    amount = data['amount']
    return json(400, { error: 'Missing correlationId' }) if correlation_id.nil? || correlation_id.to_s.empty?
    return json(400, { error: 'Missing amount' }) if amount.nil? || amount.to_s.empty?

    accepted = @store.begin_payment(correlation_id: correlation_id)
    unless accepted
      Log.warn('duplicate_payment_rejected', correlation_id: correlation_id)
      return json(409, { error: 'Duplicate correlationId', correlationId: correlation_id })
    end

    if @job_queue.enqueue data
      json(201, { message: 'Payment created' })
    else
      @store.remove_payment(correlation_id: correlation_id)
      json(501, { error: 'Payment enqueue failed', correlationId: correlation_id })
    end

  rescue JSON::ParserError
    json(400, { error: 'Invalid JSON' })
  rescue => e
    Log.exception(e, 'payment_request_failed', correlationId: correlation_id, error: e.message)
    json(500, { error: 'Internal Server Error', details: e.message })
  end

  def handle_payments_summary(request)
    # request.path may include query: /payments-summary?from=...&to=...
    uri = URI.parse(request.path || '/payments-summary')
    params = uri.query ? URI.decode_www_form(uri.query).to_h : {}

    json(200, @store.summary(from: params['from'], to: params['to']))
  rescue => e
    Log.error('payments_summary_failed', error: e.message)
    json(500, { error: 'Internal Server Error' })
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

    queue_state = {
      'inflight' =>  @job_queue.inflight,
      'done' => @job_queue.done,
      'errors' => @job_queue.errors,
      'total' => @job_queue.queue.size
    }

    mean_ms = if @total_requests > 0
      @total_duration_ms / @total_requests
    else
      0.0
    end

    route_means = {}
    @route_status_counts.each do |route, statuses|
      count = statuses.values.inject(0, :+)
      if count > 0
        route_means[route] = @route_duration_ms[route] / count
      else
        route_means[route] = 0.0
      end
    end

    json(200, {
      ok: true,
      totalRequests: @total_requests,
      meanResponseTimeMs: mean_ms,
      routes: routes,
      routesMeanResponseTimeMs: route_means,
      queue: queue_state
    })
  end
end

if __FILE__ == $0
  Sync do
    port = Integer(ENV.fetch('PORT', '3000'))
    Log.info('server_start', port: port)

    redis_endpoint = Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
    redis_client = Async::Redis::Client.new(redis_endpoint)
    store = Store.new(redis_client: redis_client)
    health_monitor = HealthMonitor.new(redis_client: redis_client)
    job_queue = JobQueue.new(
      concurrency: (ENV['QUEUE_CONCURRENCY'] || '512').to_i,
      redis_client: redis_client,
      store: store,
      health_monitor: health_monitor
    )


    endpoint = Async::HTTP::Endpoint.parse("http://0.0.0.0:#{port}")
    app = PruApp.new(store: store, job_queue: job_queue)

    # Start background workers once when reactor boots:
    server = Async::HTTP::Server.for(endpoint) do |request|
      begin
        Log.debug('request', method: request.method, path: request.path)
        app.call(request)
      rescue => e
        Log.warn('request_failed', method: request.method, path: request.path, error: e.message, backtrace: e.backtrace)
        # ignore logging issues
      end
    end

    Async do |task|
        job_queue.start
        health_monitor.start
        server.run
    end
  end
end
