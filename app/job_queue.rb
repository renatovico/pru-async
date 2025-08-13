require 'async'
require 'async/idler'
require 'async/queue'
require_relative 'logger'

class JobQueue
  def initialize(concurrency: 128)
    @queue = Async::LimitedQueue.new(concurrency)
  end

  # Start worker tasks that consume from the queue.
  def start()
    Log.info('job_queue_start')

    # Process items from the queue:
    idler = Async::Idler.new(0.5)

    while line = @queue.pop
      idler.async do
        safe_call(line)
      end
    end
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

  private

  def safe_call(job)
    job.call
  rescue => e
    Log.exception(e, 'job_failed')
  end
end
