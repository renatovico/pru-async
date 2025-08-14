require 'json'
require 'async'
require 'net/http'
require 'uri'
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

  def perform_now(payload, retries: nil)
    retries ||= 0

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

    endpoint = "http://payment-processor-#{processor_name}:8080/payments"
    uri = URI(endpoint)
    http = Net::HTTP.new(uri.host, uri.port)

    if timeout
      if timeout > 2
        timeout = 2
      end

      http.open_timeout = timeout
      http.read_timeout = timeout + 0.5
    end

    request = Net::HTTP::Post.new(uri.path, 'Content-Type' => 'application/json')
    request.body = payload.to_json
    response = http.request(request)
    response.is_a?(Net::HTTPSuccess)
  rescue => e
    # Log.debug(e, kind: 'processor_error', processor: processor_name, correlation_id: correlation_id)
    false
  end

  private

  def failing?(processor)
    state = @health.get(processor)
    state && state['failing'] == true
  end
end
