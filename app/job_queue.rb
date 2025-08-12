require 'async'
require 'async/semaphore'
require 'async/barrier'
require_relative 'logger'

class JobQueue
  def initialize(concurrency: 512)
    @started = false
    @concurrency = concurrency
    @barrier = Async::Barrier.new
    @semaphore = Async::Semaphore.new(@concurrency, parent: @barrier)
  end

  def enqueue(&block)
    raise 'JobQueue not started' unless @semaphore && @barrier
    @semaphore.async(parent: @barrier, transient: true) { safe_call(block) }
    true
  end


  # Optional: close the queue and allow workers to drain and exit.
  def close
    # Stop all scheduled work
    begin
      @barrier&.stop
    rescue => e
      Log.warn('barrier_stop_error', detail: e.message)
    end
  end

  private

  def safe_call(job)
    job.call
  rescue => e
    Log.exception(e, 'job_failed')
  end
end
