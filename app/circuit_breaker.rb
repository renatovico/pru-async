require 'time'

# Minimal in-memory circuit breaker with a sliding time window.
# Open = failures within the last `window_seconds` are GREATER than `threshold`.
class CircuitBreaker
  DEFAULT_THRESHOLD = 5
  DEFAULT_WINDOW_SECONDS = 5

  def initialize(threshold: DEFAULT_THRESHOLD, window_seconds: DEFAULT_WINDOW_SECONDS)
    @threshold = threshold
    @window = window_seconds
    @failures = Hash.new { |h, k| h[k] = [] } # processor => [timestamps]
  end

  # Returns true if failures in the current window are > threshold.
  def open?(processor)
    now = Time.now.to_f
    list = (@failures[processor] ||= [])
    cutoff = now - @window
    list.reject! { |t| t < cutoff }
    list.length > @threshold
  end

  # Record a failure for the processor and prune old entries.
  def record_failure(processor)
    now = Time.now.to_f
    list = (@failures[processor] ||= [])
    list << now
    cutoff = now - @window
    list.reject! { |t| t < cutoff }
  end
end
