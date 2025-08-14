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
    # @health = health_monitor || HealthMonitor.new(redis_client: @redis)
    @failure_default = 0
    @failure_fallback = 0
    @failure_threshold = 100
    @last_failure_default_time = Time.now
    @last_failure_fallback_time = Time.now
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

    # to = @health.suggested_timeout('default', default: 1.0)
    to = 0.5
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
      else
        record_failure('default')
      end
    end

    # fallback_to = @health.suggested_timeout('fallback', default: 1.0)
    fallback_to = 1.0

    # Try fallback processor if default failed
    unless (failing?('fallback'))
      result = try_processor('fallback', payload_job, correlation_id, timeout: fallback_to)
      if result
        @store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
        return true
      else
        record_failure('fallback')
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

    # Sync do
    #   Async.with_timeout(to) do
      response = Async::HTTP::Internet.post(url, headers, body)
      ok = response.status >= 200 && response.status < 300
      ok
    # end
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

  # def failing?(processor)
    # state = @health.get(processor)
    # state && state['failing'] == true
  # end
  #
  def record_failure(processor)
    case processor
    when 'default'
      @failure_default += 1
      @last_failure_default_time = Time.now
    when 'fallback'
      @failure_fallback += 1
      @last_failure_fallback_time = Time.now
    end
  end

  def failing?(processor)
    case processor
    when 'default'
      if @failure_default >= @failure_threshold && Time.now - @last_failure_default_time < 2
        return true
      end
      false
    when 'fallback'
      if @failure_fallback >= @failure_threshold && Time.now - @last_failure_fallback_time < 5
        return true
      end
      false
    end
  end
end
