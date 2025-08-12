require 'json'
require 'async'
require 'async/http/internet/instance'

require_relative 'store'
require_relative 'redis_pool'
require_relative 'circuit_breaker'

class PaymentJob

  def self.circuit_breaker
    @circuit_breaker ||= CircuitBreaker.new(threshold: 3, window_seconds: 1)
  end

  def self.perform_now(correlation_id, amount, requested_at, retries: nil)
    return if RedisPool.with { |redis| redis.get("processed:#{correlation_id}") }

    retries ||= 0

    payload = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    store = Store.new

    # Skip default if circuit is open
    unless self.circuit_breaker.open?('default')
      2.times do |attempt|
        if self.try_processor('default', payload, timeout: 0.3)
          store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
          puts "🐦 Payment #{correlation_id} processed by default (attempt #{attempt + 1})"
          return
        end

        # Small non-blocking delay between retries
        Async::Task.current.sleep(0.002 * (attempt + 1))

      end
    else
      puts "⚠️  Circuit open for default, skipping attempts"
    end

    # Try fallback processor if default failed (with aggressive timeout)
    if !self.circuit_breaker.open?('fallback') && self.try_processor('fallback', payload, timeout: 0.1)
      store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
      puts "🐦 Payment #{correlation_id} processed by fallback"
      return
    elsif self.circuit_breaker.open?('fallback')
      puts "⚠️  Circuit open for fallback, skipping attempt"
    end

    Async::Task.current.sleep(retries + 1 ** 2)

    if retries > 2
      raise "Processors both failed for #{correlation_id} in #{retries} retries"
    end

    retries += 1
    self.perform_now(correlation_id, amount, requested_at, retries: retries)
  end

  def self.try_processor(processor_name, payload, timeout: nil)
    # Fast-fail if circuit is open
    if self.circuit_breaker.open?(processor_name)
      return false
    end
    url = "http://payment-processor-#{processor_name}:8080/payments"
    headers = [['content-type', 'application/json']]
    body = payload.to_json

    response = nil
    begin
      if timeout
        Async::Task.current.with_timeout(timeout + 2) do
          response = Async::HTTP::Internet.post(url, headers, body)
        end
      else
        response = Async::HTTP::Internet.post(url, headers, body)
      end
      ok = response.status >= 200 && response.status < 300
      self.circuit_breaker.record_failure(processor_name) unless ok
      ok
    ensure
      response&.close
    end
  rescue => e
    puts "Error processing payment #{payload[:correlationId]} on #{processor_name}: #{e.message}"
    self.circuit_breaker.record_failure(processor_name)
    false
  end
end
