require 'async'
require 'async/semaphore'
require 'async/queue'
require_relative 'payment_job'

class JobQueue
  def initialize(redis_client: nil, store: nil, notify: nil)
    @queue = Async::Queue.new
    @redis = redis_client
    @store = store
    @payment_job = PaymentJob.new(store: store, redis_client: redis_client)
    @inflight = 0
    @done = 0
    @errors = 0
    @failure_threshold = 20
    @failure_retry_threshold = 100
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
  def start(thread_id: nil)
    @notify&.send(status: "job_queue_start", thread_id: thread_id)
    # Process items from the queue:
    # semaphore = Async::Semaphore.new(4)


    while (job = @queue.pop)
      maybe_backoff_due_to_failures
      # semaphore.async do |task|
      process_job(job, thread_id)
      # end
    end

    @notify&.send(status: "job_queue_finish", thread_id: thread_id)
  end

  def queue
    @queue
  end

  # Enqueue a job (callable). Returns true if enqueued, false if closed.
  def enqueue(data)
    @queue.push([0, data])
    @notify&.send(status: "job_enqueued", data: data)
  end

  # Close the queue: signal workers to stop after draining.
  def close
    @queue.close
  rescue => e
    @notify&.send(status: "queue_close_error", error: e.message)
  end

  private

  def process_job(job, thread_id)
    Sync do
      if job[0] > @failure_retry_threshold
        @errors += 1
      else
        begin
          @inflight += 1

          result = @payment_job.perform_now(job)

          if result
            @done += 1
            @failure_events = 0
          else
            job[0] += 1
            @failure_events += 1
            @queue.push(job)
          end
        rescue => e
          puts "Error processing job: #{e.message} on thread #{thread_id} - #{e.backtrace.join("\n")}"
          @notify&.send(status: "job_failed", error: e.message, backtrace: e.backtrace, thread_id: thread_id)
          @failure_events += 1
          @queue.push(job) # Requeue the job for retry
        ensure
          @inflight -= 1
        end
      end

    end
  end

  def maybe_backoff_due_to_failures
    if @failure_events > @failure_threshold
      # Progressive backoff based on the number of failures
      backoff_time = [@failure_backoff_seconds * (0.1 + (@failure_events / @failure_threshold)), 5].min
      Async::Task.current.sleep(backoff_time)
    end
  end
end
