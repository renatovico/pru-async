require 'async'
require 'async/idler'
require 'async/semaphore'
require 'console'
require_relative 'payment_job'

class JobQueue
  DEFAULT_CONCURRENCY = (ENV['JOB_CONCURRENCY'] || 1).to_i
  RETRY_CONCURRENCY = (ENV['JOB_CONCURRENCY_RETRY'] || 1).to_i
  DEAD_CONCURRENCY  = (ENV['JOB_CONCURRENCY_DEAD']  ||  1).to_i

  RETRY_DELAY_BASE_MS = (ENV['RETRY_DELAY_BASE_MS'] || 0.5).to_f # base delay for retry queue (ms)
  DEAD_DELAY_BASE_MS  = (ENV['DEAD_DELAY_BASE_MS']  ||  4).to_f # base delay for dead queue (ms)

  MAX_ATTEMPTS = (ENV['JOB_MAX_ATTEMPTS'] || 50).to_i # after this, drop & count error

  RETRY_QUEUE_START_ATTEMPT = 1   # attempt index after first failure (attempt count before increment)
  DEAD_QUEUE_START_ATTEMPT  = 5   # move to dead queue when attempts > 4 (i.e. 5th attempt and beyond)


  def initialize(redis_client: nil, store: nil, concurrency: DEFAULT_CONCURRENCY, max_queue: nil)
    @primary_queue = Async::Queue.new
    @retry_queue   = Async::Queue.new
    @dead_queue    = Async::Queue.new

    @redis = redis_client
    @store = store
    @payment_job = PaymentJob.new(store: store, redis_client: redis_client)

    # Metrics
    @inflight_primary = 0
    @inflight_retry   = 0
    @inflight_dead    = 0
    @done = 0
    @errors = 0
    @accepted_count = 0
    @processed_events = 0

    # Failure tracking
    @failure_events = 0
    @failure_threshold = 20
    @failure_backoff_seconds = 0.2

    # Concurrency
    @primary_concurrency = concurrency
    @retry_concurrency   = RETRY_CONCURRENCY
    @dead_concurrency    = DEAD_CONCURRENCY

    # Reporting
    @report_bucket_size = (ENV['REPORT_BUCKET'] || 500).to_i
    @last_report_bucket = -1
  end

  # Public metric helpers
  def done; @done; end
  def errors; @errors; end
  def accepted_count; @accepted_count; end
  def primary_size; @primary_queue.size; end
  def retry_size; @retry_queue.size; end
  def dead_size; @dead_queue.size; end
  def primary_concurrency; @primary_concurrency; end
  def retry_concurrency; @retry_concurrency; end
  def dead_concurrency; @dead_concurrency; end

  def start(parent)
    # Primary workers
    start_workers(parent, @primary_queue, @primary_concurrency, :primary)
    # Retry workers
    start_workers(parent, @retry_queue, @retry_concurrency, :retry)
    # Dead workers (slow lane)
    start_workers(parent, @dead_queue, @dead_concurrency, :dead)
  end

  def enqueue(data)
    @primary_queue.push([0, data, nil, nil])
    @accepted_count += 1
    true
  end

  def close
    @primary_queue.close
    @retry_queue.close
    @dead_queue.close
  rescue => e
    Console.error("queue_close_failed", error: e.message)
  end

  private

  def start_workers(parent, queue, concurrency, lane)
    sem = Async::Semaphore.new(concurrency, parent: parent)
    parent.async do
      while (job = queue.pop)
        sem.async { process_job(job, lane) }
      end
    end
  end

  def process_job(job, lane)
    inflight_inc(lane, +1)
    begin
      result = Sync do
        @payment_job.perform_now(job)
      end
      if result
        @done += 1
      else
        handle_failure(job, lane)
      end
    rescue => e
      Console.error("process_job_exception", lane: lane, error: e.message)
      handle_failure(job, lane)
    ensure
      @processed_events += 1
      maybe_report_bucket
      inflight_inc(lane, -1)
    end
  end

  def handle_failure(job, lane)
    job[0] += 1 # increment attempt count AFTER failure
    attempts = job[0]

    # Console.info("job_failed", lane: lane, job: job.inspect, attempts: attempts)

    if attempts > MAX_ATTEMPTS
      Console.warn("job_max_attempts_exceeded", job: job.inspect, attempts: attempts)
      @errors += 1
      return
    end

    # Determine target queue & delay
    if attempts > DEAD_QUEUE_START_ATTEMPT
      schedule_enqueue(@dead_queue, job, DEAD_DELAY_BASE_MS)
    elsif attempts > RETRY_QUEUE_START_ATTEMPT
      schedule_enqueue(@retry_queue, job, RETRY_DELAY_BASE_MS)
    else
      # first failure -> retry queue with base delay
      schedule_enqueue(@retry_queue, job, 0)
    end
  end
  def schedule_enqueue(queue, job, delay)
      Console.debug("enqueue_job", queue: queue.class.name, job: job, delay: delay)
      Async do
        sleep(delay) if delay.positive?
        queue.push(job)
      end
  end

  def inflight_inc(lane, delta)
    case lane
    when :primary then @inflight_primary += delta
    when :retry   then @inflight_retry += delta
    when :dead    then @inflight_dead += delta
    end
  end

  def maybe_report_bucket
    bucket = @processed_events / @report_bucket_size
    return if bucket == @last_report_bucket
    @last_report_bucket = bucket
    log_metrics(bucket)
  end

  def log_metrics(bucket)
    Console.info(
      "metrics",
      bucket: bucket,
      bucket_size: @report_bucket_size,
      accepted: @accepted_count,
      processed_events: @processed_events,
      successes: @done,
      errors: @errors,
      primary_q: @primary_queue.size,
      retry_q: @retry_queue.size,
      dead_q: @dead_queue.size,
      inflight_primary: @inflight_primary,
      inflight_retry: @inflight_retry,
      inflight_dead: @inflight_dead,
      primary_conc: @primary_concurrency,
      retry_conc: @retry_concurrency,
      dead_conc: @dead_concurrency
    )
  rescue => e
    Console.error("metric_log_failed", error: e.message)
  end
end
