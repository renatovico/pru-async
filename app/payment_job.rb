require 'json'
require 'async'
require 'async/http/internet/instance'

class PaymentJob
  def initialize(store:, redis_client:)
    @store = store
    @redis = redis_client
    @last_failure_default_time = nil
  end

  def perform_now(payload)
    processor = target_processor
    if processor.nil?
      return false # without processor now
    end

    correlation_id = payload['correlationId']
    amount = payload['amount']

    requested_at = Time.now.iso8601(3)
    payload_job = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    # Try default processor first if allowed
    result = try_processor(processor, payload_job)
    if result
      @store.save(correlation_id: correlation_id, processor: processor, amount: amount, timestamp: requested_at)
      record_success(processor)
      true
    else
      record_failure(processor)
      false
    end
  end

  def try_processor(processor_name, payload)
    url = "http://payment-processor-#{processor_name}:8080/payments"
    headers = [['content-type', 'application/json']]
    body = payload.to_json

    Sync do |task|
      # Request will timeout after 2 seconds
      task.with_timeout(5) do
        response = Async::HTTP::Internet.post(url, headers, body)
        ok = response.status >= 200 && response.status < 300
        return ok
      ensure
        response&.close
      end
    rescue Async::TimeoutError
      return false
    end
  end

  private


  def record_failure(processor)
    case processor
    when 'default'
      @last_failure_default_time = Time.now
    when 'fallback'
      @last_failure_fallback_time = Time.now
    else
      # no-op
    end
  end

  def record_success(processor)
    case processor
    when 'default'
      @last_failure_default_time = nil
    when 'fallback'
      @last_failure_fallback_time = nil
    else
      # no-op
    end
  end

  def target_processor
    # Check if default processor is available
    if @last_failure_default_time.nil? || Time.now - @last_failure_default_time > 20
      return 'default'
    end

    # Check if fallback processor is available
    if @last_failure_fallback_time.nil? || Time.now - @last_failure_fallback_time > 20
      return 'fallback'
    end

    nil # No processor available now
  rescue => e
    nil # In case of error, return nil to avoid processing
  end
end
