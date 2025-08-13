require 'async'
require 'async/semaphore'
require 'async/queue'
require_relative 'logger'

class JobQueue
  PAUSE_KEY = 'job_queue:paused'.freeze
  INFLIGHT_KEY = 'job_queue:inflight'.freeze

  def initialize(concurrency: 10, redis_client: nil)
    @queue = Async::Queue.new
    @redis = redis_client
    @concurrency = concurrency
  end

  # Start worker tasks that consume from the queue.
  def start()
    Log.info('job_queue_start')

    # Process items from the queue:
    idler = Async::Semaphore.new(@concurrency)

    while (job = @queue.pop)
      # If paused, wait until resumed before scheduling the job.
      while paused?
        Async::Task.current.sleep(0.05)
      end

      idler.async do
        begin
          incr_inflight
          safe_call(job)
        ensure
          decr_inflight
        end
      end
    end
  end

  def queue
    @queue
  end

  # Enqueue a job (callable). Returns true if enqueued, false if closed.
  def enqueue(&block)
    @queue.push(block)
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

  # --- Redis-backed controls/state ---
  def pause!
    return unless @redis
    @redis.call('SET', PAUSE_KEY, '1')
  end

  def resume!
    return unless @redis
    @redis.call('DEL', PAUSE_KEY)
  end

  def paused?
    return false unless @redis
    @redis.call('GET', PAUSE_KEY) == '1'
  rescue
    false
  end

  def inflight
    return 0 unless @redis
    (@redis.call('GET', INFLIGHT_KEY) || '0').to_i
  rescue
    0
  end

  private

  def incr_inflight
    return unless @redis
    @redis.call('INCR', INFLIGHT_KEY)
  rescue => e
    Log.debug(e, kind: 'inflight_incr_error')
  end

  def decr_inflight
    return unless @redis
    v = @redis.call('DECR', INFLIGHT_KEY).to_i
    if v < 0
      # Clamp to zero if it ever goes negative due to crashes.
      @redis.call('SET', INFLIGHT_KEY, '0')
    end
  rescue => e
    Log.debug(e, kind: 'inflight_decr_error')
  end

  def safe_call(job)
    job.call
  rescue => e
    Log.exception(e, 'job_failed')
  end
end
