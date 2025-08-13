require 'json'
require 'async'
require 'async/http/internet/instance'
require_relative 'logger'

require_relative 'store'
require_relative 'circuit_breaker'
require_relative 'health_monitor'

class PaymentJob
  def initialize(store:, redis_client:, health_monitor: nil)
    @store = store
    @redis = redis_client
    @health = health_monitor || HealthMonitor.new(redis_client: @redis)
  end

  def circuit_closed?
    # Consider health failing as open as well.
    default_open = failing?('default')
    fallback_open = failing?('fallback')
    default_open && fallback_open
  end

  def perform_now(correlation_id, amount, requested_at, retries: nil)
    return if @redis.call('GET', "processed:#{correlation_id}")

    retries ||= 0

    payload = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    # Try default processor first if allowed
    unless (failing?('default'))
      processed = Sync do
        to = @health.suggested_timeout('default', default: 1.0)
        try_processor('default', payload, correlation_id, timeout: to)
      end
      if processed
        @store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
        Log.debug('payment_processed', correlation_id: correlation_id, processor: 'default')
        return true
      end
    else
      Log.debug('circuit_open_skip', processor: 'default')
    end

    # Try fallback processor if default failed
    unless (failing?('fallback'))
      processed = Sync do
        to = @health.suggested_timeout('fallback', default: 1.0)
        try_processor('fallback', payload, correlation_id, timeout: to)
      end
      if processed
        @store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
        Log.debug('payment_processed', correlation_id: correlation_id, processor: 'fallback')
        return true
      end
    else
      Log.debug('circuit_open_skip', processor: 'fallback')
    end

    if retries > 2
      Log.debug('payment_failed', correlation_id: correlation_id)
      @store.set_status(correlation_id: correlation_id, status: 'failed', fields: { reason: 'both_processors_failed' })
      return false
    end

    # Retry logic if both processors failed
    Async::Task.current.sleep(retries + 0.05)
    self.perform_now(correlation_id, amount, requested_at, retries: retries.to_i + 1)

  end

  def try_processor(processor_name, payload, correlation_id, timeout: nil)
    Sync do
      Log.debug('try_processor', processor: processor_name, correlation_id: correlation_id)

      url = "http://payment-processor-#{processor_name}:8080/payments"
      headers = [['content-type', 'application/json']]
      body = payload.to_json

      Async::Task.current.with_timeout(timeout) do
        response = Async::HTTP::Internet.post(url, headers, body)

        ok = response.status >= 200 && response.status < 300
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
      false
    end
  end

  private

  def failing?(processor)
    state = @health.get(processor)
    state && state['failing'] == true
  end
end
