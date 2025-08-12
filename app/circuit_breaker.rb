require 'time'

require_relative 'redis_pool'

class CircuitBreaker
  DEFAULT_THRESHOLD = 5
  DEFAULT_WINDOW_SECONDS = 5

  def initialize(threshold: DEFAULT_THRESHOLD, window_seconds: DEFAULT_WINDOW_SECONDS)
    @threshold = threshold
    @window = window_seconds
  end

  # Returns true if the circuit is open for the given processor.
  def open?(processor)
    now = Time.now.to_f
    key = key_for(processor)
    RedisPool.with do |redis|
      prune(redis, key, now)
      failures = redis.zcard(key)
      failures >= @threshold
    end
  end

  # Record a failure event for the given processor.
  def record_failure(processor)
    now = Time.now.to_f
    key = key_for(processor)
    member = "#{Process.pid}-#{Thread.current.object_id}-#{(now * 1_000_000).to_i}"
    RedisPool.with do |redis|
      redis.multi do
        redis.zadd(key, now, member)
        prune(redis, key, now)
      end
    end
  end

  private

  def key_for(processor)
    "cb:errors:#{processor}"
  end

  def prune(redis, key, now)
    cutoff = now - @window
    redis.zremrangebyscore(key, 0, cutoff)
  end
end
