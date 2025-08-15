require 'async'
require 'async/semaphore'
require 'async/queue'
require_relative 'payment_job'

class JobQueue
  PAUSE_KEY = 'job_queue:paused'.freeze
  INFLIGHT_KEY = 'job_queue:inflight'.freeze

  def initialize(redis_client: nil, store: nil, notify: nil)
    @queue = Async::Queue.new
    @redis = redis_client
    @store = store
    @payment_job = PaymentJob.new(store: store, redis_client: redis_client)
    @inflight = 0
    @done = 0
    @errors = 0
    @failure_threshold = 150
    @failure_retry_threshold = 50
    @failure_backoff_seconds = 1
    @failure_events = 0
    @notify = notify
  end

  def inflight
    @inflight
  end

  def done
    @done
  end

  def errors
    @errors
  end

  # Start worker tasks that consume from the queue.
  def start()
    @notify&.send(status: "job_queue_start")
    # Process items from the queue:
    #idler = Async::Semaphore.new(128) # Limit concurrency to 128 workers

    while (job = @queue.pop)
      maybe_backoff_due_to_failures
      # idler.async do
        if job['retries'] > @failure_retry_threshold
            @notify&.send(status: "job_failed_permanently", job_id: job['correlationId'], retries: job['retries'])
            @store.remove_payment(correlation_id: job['correlationId'])
            @errors += 1
        else
          begin
            @inflight += 1

            if @payment_job.perform_now(job)
              @notify&.send(status: "job_completed", job_id: job['correlationId'])
              @done += 1
              @failure_events = 0
            else
              job['retries'] += 1
              @notify&.send(status: "job_failed", job_id: job['correlationId'], retries: job['retries'])
              @failure_events += 1
              # Add exponential backoff based on retry count
              if job['retries'] > 1
                Async do
                  Async::Task.current.sleep([job['retries'] * 0.02, 3].min)
                  @queue.push(job)
                end
              else
                @queue.push(job)
              end
            end
          rescue => e
            @notify&.send(status: "job_failed", job_id: job['correlationId'], error: e.message, exception: e.class.name)
            @failure_events += 1
          ensure
            @inflight -= 1
          end
        end
      #end
    end
  end

  def queue
    @queue
  end

  # Enqueue a job (callable). Returns true if enqueued, false if closed.
  def enqueue(data)
    @queue.push(data)
    true
  rescue => e
    @notify&.send(status: "job_enqueue_error", error: e.message)
    false
  end

  # Close the queue: signal workers to stop after draining.
  def close
    @queue.close
  rescue => e
    @notify&.send(status: "queue_close_error", error: e.message)
  end

  private

  def maybe_backoff_due_to_failures
    if @failure_events > @failure_threshold
      # Progressive backoff based on the number of failures
      backoff_time = [@failure_backoff_seconds * (@failure_events / @failure_threshold), 1].min
      Async::Task.current.sleep(backoff_time)
    end
  end
end
