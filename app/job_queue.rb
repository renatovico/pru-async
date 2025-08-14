require 'async'
require 'async/semaphore'
require 'async/queue'
require_relative 'logger'
require_relative 'payment_job'

class JobQueue
  PAUSE_KEY = 'job_queue:paused'.freeze
  INFLIGHT_KEY = 'job_queue:inflight'.freeze

  def initialize(concurrency: 10, redis_client: nil, store: nil, health_monitor: nil)
    @queue = Async::Queue.new
    @redis = redis_client
    @concurrency = concurrency
    @store = store
    @payment_job = PaymentJob.new(store: store, redis_client: redis_client, health_monitor: health_monitor)
    @inflight = 0
    @done = 0
    @errors = 0
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
    Log.info('job_queue_start')
    # Process items from the queue:
    idler = Async::Semaphore.new(@concurrency)

    while (job = @queue.pop)
      Log.debug('job_started', job_id: job[:correlation_id])
      # If paused, wait until resumed before scheduling the job

      if job['retries'] > 30
        Log.warn('job_failed_permanently', job_id: job[:correlation_id], retries: job['retries'])
        @store.remove_payment(correlation_id: job[:correlation_id])
        @errors += 1
        next
      end

      idler.async do
        begin
          @inflight += 1

          if @payment_job.perform_now(job)
            Log.debug('job_completed', job_id: job[:correlation_id])
            @done += 1
          else
            job['retries'] += 1
            Log.debug('job_failed', job_id: job[:correlation_id], retries: job['retries'])
            @queue.push(job)
          end
        rescue => e
          Log.exception(e, 'job_failed', job_id: job[:correlation_id])
        ensure
          @inflight -= 1
        end
      end.wait
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
    Log.exception(e, 'job_enqueue_error')
    false
  end

  # Close the queue: signal workers to stop after draining.
  def close
    @queue.close
  rescue => e
    Log.warn('queue_close_error', detail: e.message)
  end

end
