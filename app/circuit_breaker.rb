require 'time'

# A simple in-memory circuit breaker with a sliding time window.
# Not distributed: state is local to the current process.
class CircuitBreaker
  DEFAULT_THRESHOLD = 5
  DEFAULT_WINDOW_SECONDS = 5

  def initialize(threshold: DEFAULT_THRESHOLD, window_seconds: DEFAULT_WINDOW_SECONDS, clock: -> { Time.now.to_f })
    @threshold = threshold
    @window = window_seconds
    @clock = clock
    @failures = Hash.new { |h, k| h[k] = [] } # processor => [timestamps]
  end

  # Returns true if the circuit is open for the given processor.
  def open?(processor)
    now = @clock.call
  list = @failures[processor]
  prune!(list, now)
  list.length >= @threshold
  end

  # Record a failure event for the given processor.
  def record_failure(processor)
    now = @clock.call
  list = @failures[processor]
  list << now
  prune!(list, now)
  end

  # Optional helper to clear failures (not used currently).
  def reset(processor = nil)
    if processor
      @failures.delete(processor)
    else
      @failures.clear
    end
  end

  private

  def prune!(list, now)
    cutoff = now - @window
    # Remove entries older than the window.
    while list.first && list.first < cutoff
      list.shift
    end
  end
end
