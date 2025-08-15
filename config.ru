# frozen_string_literal: true

require_relative './server'

notify = Async::Container::Notify::Console.open!



redis_endpoint = Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
redis_client = Async::Redis::Client.new(redis_endpoint)
store = Store.new(redis_client: redis_client)
job_queue = JobQueue.new(
  redis_client: redis_client,
  store: store
)

# Run the reactor for 1 second:
#Async do
# notify&.send(status: "job_queue_start")
#job_queue.start.wait
#notify&.send(status: "Finished job queue")
#end

# Boot dependencies (Redis, Store, HealthMonitor, JobQueue) and start background services.
app = PruApp.new(notify: notify, job_queue: job_queue, store: store)
run app
