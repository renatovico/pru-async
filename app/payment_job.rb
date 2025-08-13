require 'json'
require 'async'
require 'async/http/internet/instance'
require_relative 'logger'

require_relative 'store'
require_relative 'circuit_breaker'

class PaymentJob
  def initialize(store:, redis_client:, circuit_breaker: CircuitBreaker.new(threshold: 20, window_seconds: 1))
    @store = store
    @redis = redis_client
    @circuit_breaker = circuit_breaker
  end

  def circuit_closed?
    @circuit_breaker.open?('default') && @circuit_breaker.open?('fallback')
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
      if Sync do
        try_processor('default', payload, correlation_id, timeout: 1)
      end
        @store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
        Log.debug('payment_processed', correlation_id: correlation_id, processor: 'default')
        return true
      end
    else
      Log.debug('circuit_open_skip', processor: 'default')
    end

  # Try fallback processor if default failed (with aggressive timeout)
    if @circuit_breaker.open?('fallback')
      Log.debug('circuit_open_skip', processor: 'fallback')
    else
      if Sync do
        try_processor('fallback', payload, correlation_id, timeout: 1)
      end
        @store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
        Log.debug('payment_processed', correlation_id: correlation_id, processor: 'fallback')
        return true
      end
    end

    if retries > 5
      Log.debug('payment_failed', correlation_id: correlation_id)
      @store.set_status(correlation_id: correlation_id, status: 'failed', fields: { reason: 'both_processors_failed' })
      return false
    end

    # Retry logic if both processors failed
    Async::Task.current.sleep(retries + 0.05)
    self.perform_now(correlation_id, amount, requested_at, retries: retries.to_i + 1)

  end

  def try_processor(processor_name, payload, correlation_id, timeout: nil)
    Log.debug('try_processor', processor: processor_name, correlation_id: correlation_id)

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
      begin
        response&.close
      rescue
        # ignore if cannot close
      end
    end
  rescue => e
    Log.debug(e, kind: 'processor_error', processor: processor_name, correlation_id: correlation_id)
    @circuit_breaker.record_failure(processor_name)
    false
  end
end
