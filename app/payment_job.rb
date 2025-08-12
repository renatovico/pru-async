require 'sidekiq'
require 'redis'
require 'json'
require 'net/http'
require 'timeout'

require_relative 'store'
require_relative 'redis_pool'
require_relative 'circuit_breaker'

class PaymentJob

  def self.circuit_breaker
    @circuit_breaker ||= CircuitBreaker.new(threshold: 5, window_seconds: 1)
  end

  def self.perform_now(correlation_id, amount, requested_at)
    return if RedisPool.with { |redis| redis.get("processed:#{correlation_id}") }

    payload = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    store = Store.new

    # Skip default if circuit is open
    unless self.circuit_breaker.open?('default')
      3.times do |attempt|
      if self.try_processor('default', payload, timeout: 0.3)
        store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
        puts "🐦 Payment #{correlation_id} processed by default (attempt #{attempt + 1})"
        return
      end

      # Small delay between retries
      sleep(0.002 * (attempt + 1)) if attempt < 2
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

    raise "Processors both failed for #{correlation_id}"
  end

  def self.try_processor(processor_name, payload, timeout: nil)
    # Fast-fail if circuit is open
    if self.circuit_breaker.open?(processor_name)
      return false
    end
    endpoint = "http://payment-processor-#{processor_name}:8080/payments"
    uri = URI(endpoint)
    http = Net::HTTP.new(uri.host, uri.port)

    if timeout
      http.open_timeout = timeout
      http.read_timeout = timeout + 2 # Read timeout is longer to allow for processing
    end

    request = Net::HTTP::Post.new(uri.path, 'Content-Type' => 'application/json')
    request.body = payload.to_json
    response = http.request(request)
    ok = response.is_a?(Net::HTTPSuccess)
    unless ok
      self.circuit_breaker.record_failure(processor_name)
    end
    ok
  rescue => e
    puts "Error processing payment #{payload[:correlationId]} on #{processor_name}: #{e.message}"
    self.circuit_breaker.record_failure(processor_name)
    false
  end
end
