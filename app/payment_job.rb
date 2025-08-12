require 'json'
require 'async'
require 'async/http/internet/instance'
require_relative 'logger'

require_relative 'store'
require_relative 'circuit_breaker'

class PaymentJob
  def initialize(store:, redis_client:, circuit_breaker: CircuitBreaker.new(threshold: 50, window_seconds: 1))
    @store = store
    @redis = redis_client
    @circuit_breaker = circuit_breaker
  end

  def perform_now(correlation_id, amount, requested_at, retries: nil)
    return if @redis.call('GET', "processed:#{correlation_id}")

    retries ||= 0

    payload = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    # Skip default if circuit is open
    unless @circuit_breaker.open?('default')
      2.times do |attempt|
        if try_processor('default', payload, correlation_id, timeout: 1)
          @store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
          Log.debug('payment_processed', correlation_id: correlation_id, processor: 'default', attempt: attempt + 1)
          return
        end

        # Small non-blocking delay between retries
        Async::Task.current.sleep(0.001 * (attempt + 1))
      end
    else
      Log.warn('circuit_open_skip', processor: 'default')
    end

  # Try fallback processor if default failed (with aggressive timeout)
    if @circuit_breaker.open?('fallback')
      Log.warn('circuit_open_skip', processor: 'fallback')
    else
      if try_processor('fallback', payload, correlation_id, timeout: 1)
        @store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
        Log.debug('payment_processed', correlation_id: correlation_id, processor: 'fallback')
        return
      end
    end

    Async::Task.current.sleep(retries + 1 ** 2)

    if retries > 10
      raise "Processors both failed for #{correlation_id} in #{retries} retries"
    end

    retries += 1
    perform_now(correlation_id, amount, requested_at, retries: retries)
  end

  def try_processor(processor_name, payload, correlation_id, timeout: nil)
    Async do |task|
      return true if @redis.call('GET', "processed:#{correlation_id}")

      # Fast-fail if circuit is open
      if @circuit_breaker.open?(processor_name)
        return false
      end
      url = "http://payment-processor-#{processor_name}:8080/payments"
      headers = [['content-type', 'application/json']]
      body = payload.to_json

      response = nil
      Async::Task.current.with_timeout(timeout) do
        response = Async::HTTP::Internet.post(url, headers, body)

        ok = response.status >= 200 && response.status < 300
        @circuit_breaker.record_failure(processor_name) unless ok
        ok
      ensure
        response&.close
      end
    rescue => e
      Log.debug(e.message, 'processor_error', processor: processor_name, correlation_id: correlation_id)
      @circuit_breaker.record_failure(processor_name)
      false
    end
  end
end
