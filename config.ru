# frozen_string_literal: true

require_relative './server'

notify = Async::Container::Notify::Console.open!

# Boot dependencies (Redis, Store, HealthMonitor, JobQueue) and start background services.

redis_endpoint = Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
redis_client = Async::Redis::Client.new(redis_endpoint)
store = Store.new(redis_client: redis_client)
job_queue = JobQueue.new(
  redis_client: redis_client,
  store: store
)

# Start background tasks using Async in a non-blocking background thread and wait on them.
Async do
  1.upto(2) do |i|
    notify&.send(status: "job_queue_start", size: i)
    Async do
      job_queue.start
    end
  end
end

APP = PruApp.new(store: store, job_queue: job_queue, notify: notify)

run lambda { |env|
  # Rack env to Protocol::HTTP::Request adapter is handled by Falcon internally via protocol-rack.
  # Our app expects Async Protocol::HTTP request, so we provide a minimal Rack shim here.
  # Convert Rack env to simplified request-like struct that exposes method, path, read.
  req = Struct.new(:method, :path) do
    def read; @body; end
    def set_body(b); @body = b; end
  end

  request = req.new(env['REQUEST_METHOD'], env['PATH_INFO'] + (env['QUERY_STRING'].to_s.empty? ? '' : "?#{env['QUERY_STRING']}"))
  # Read entire body
  input = env['rack.input']
  body = input ? input.read : nil
  request.set_body(body)

  # Call our existing app which returns Protocol::HTTP::Response-like object.
  response = APP.call(request)

  # Convert Protocol::HTTP::Response to Rack response [status, headers, body]
  status = response.status
  headers = {}
  response.headers.each do |k, v|
    # response.headers yields headers in Array form sometimes; normalize to string
    headers[k.to_s] = Array(v).join(', ')
  end
  body_enum = []
  response.body.each do |chunk|
    body_enum << chunk
  end

  [status, headers, body_enum]
}
