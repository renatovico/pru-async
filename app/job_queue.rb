require 'async'
require 'async/queue'
require 'async/limited_queue'
require 'singleton'

class JobQueue
  include Singleton

  def initialize
  # Bounded queue to avoid unbounded memory use; tune capacity as needed:
  @queue = Async::LimitedQueue.new(512)
    @started = false
    @tasks = []
  end

  def self.enqueue(&block)
    instance.enqueue(&block)
  end

  def enqueue(&block)
    @queue.push(block)
  end

  # Try to enqueue with a timeout to avoid blocking the HTTP fiber indefinitely.
  # Returns true if enqueued, false if timed out or queue is closed.
  def enqueue_with_timeout(timeout_seconds, &block)
    Async::Task.current.with_timeout(timeout_seconds) do
      @queue.push(block)
      true
    end
  rescue Async::TimeoutError
    false
  rescue Async::Queue::Closed
    false
  end

  def self.start_workers(count: 5, parent_task: nil)
    instance.start_workers(count: count, parent_task: parent_task)
  end

  def start_workers(count:, parent_task: nil)
    return if @started
    @started = true

    spawner = parent_task || Async

    count.times do
      @tasks << spawner.async(transient: true) do
        while (job = @queue.pop)
          begin
            job.call
          rescue => e
            puts "❌ Job failed: #{e.message}"
          end
        end
      end
    end
  end

  # Optional: close the queue and allow workers to drain and exit.
  def self.close
    instance.close
  end

  def close
    @queue.close
  end
end
