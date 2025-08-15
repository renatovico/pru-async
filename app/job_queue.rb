require 'async'
require 'async/idler'
require 'async/semaphore'
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
    @failure_retry_threshold = 60
    @failure_backoff_seconds = 0.2
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
  def start(parent)
    # Process items from the queue:
    idler = Async::Semaphore.new(30, parent: parent)

    while (job = @queue.pop)
      idler.async do
        process_job(job)
      end
    end

  end

  def queue
    @queue
  end

  # Enqueue a job (callable). Returns true if enqueued, false if closed.
  def enqueue(data)
    @queue.push([0, data])
  end

  # Close the queue: signal workers to stop after draining.
  def close
    @queue.close
  rescue => e
    puts "Error closing queue: #{e.message} - #{e.backtrace.join("\n")}"
  end

  private

  def process_job(job)
      if job[0] > @failure_retry_threshold
        @errors += 1
      else
        # puts "Processing job: #{job.inspect}" if job[0] > 0
        begin
          @inflight += 1

          result = @payment_job.perform_now(job)

          if result
            @done += 1
            @failure_events = 0
          else
            job[0] += 1
            @failure_events += 1
            # Async::Task.current.sleep(0.01 * job[0] ** 2  ) # Short sleep to avoid tight loop
            @queue.push(job)
          end
        rescue => e
          puts "Error processing job: #{e.message}  - #{e.backtrace.join("\n")}"
          @failure_events += 1
          @queue.push(job) # Requeue the job for retry
        ensure
          @inflight -= 1
        end
      end
  end

  def maybe_backoff_due_to_failures
    if @failure_events > @failure_threshold
      # Progressive backoff based on the number of failures
      backoff_time = [@failure_backoff_seconds * (0.1 + (@failure_events / @failure_threshold)), 1].min
      Async::Task.current.sleep(backoff_time)
    end
  end
end
