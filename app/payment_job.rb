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

  def perform_now(payload)
    correlation_id = payload['correlationId']
    amount = payload['amount']

    to = @health.suggested_timeout('default', default: 1.0)
    requested_at = Time.now.iso8601(3)

    payload_job = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    # Try default processor first if allowed
    unless (failing?('default'))
      result = try_processor('default', payload_job, correlation_id, timeout: to)
      if result
        @store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
        return true
      end
    end

    fallback_to = @health.suggested_timeout('fallback', default: 1.0)

    # Try fallback processor if default failed
    unless (failing?('fallback'))
      result = try_processor('fallback', payload_job, correlation_id, timeout: fallback_to)
      if result
        @store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
        return true
      end
    end

    return false
  end

  def try_processor(processor_name, payload, correlation_id, timeout: nil)
    # Log.debug('try_processor', processor: processor_name, correlation_id: correlation_id, timeout: timeout)

    url = "http://payment-processor-#{processor_name}:8080/payments"
    headers = [['content-type', 'application/json']]
    body = payload.to_json

    # clamp timeout
    to = (timeout || 1.0).to_f
    to = 2.0 if to > 2.0

    response = Async::HTTP::Internet.post(url, headers, body)
    ok = response.status >= 200 && response.status < 300
    ok
  rescue
    # Log.debug(e, kind: 'processor_error', processor: processor_name, correlation_id: correlation_id)
    false
  ensure
    begin
      response&.close
    rescue
      # ignore
    end
  end

  private

  def failing?(processor)
    state = @health.get(processor)
    state && state['failing'] == true
  end
end
