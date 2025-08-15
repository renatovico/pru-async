require 'json'
require 'async'
require 'async/http/internet/instance'

class PaymentJob
  def initialize(store:, redis_client:)
    @store = store
    @redis = redis_client
    @last_failure_default_time = nil
    @fallback_mode_start_time = nil
  end

  def perform_now(payload)
    processor = target_processor
    if processor.nil?
      return false # without processor now
    end

    # Enqueue work into background workers (outside the HTTP request fiber).
    data = JSON.parse(payload[1])
    correlation_id = data['correlationId']
    amount = data['amount']
    raise 'Missing correlationId' if correlation_id.nil?
    raise 'Missing amount' if amount.nil?

    payload_job = {
      correlationId: correlation_id,
      amount: amount,
    }

    # Try default processor first if allowed
    result, time = try_processor(processor, payload_job)
    if result
      @store.save(correlation_id: correlation_id, processor: processor, amount: amount, timestamp: time)
      record_success(processor)
      true
    else
      record_failure(processor)
      false
    end
  rescue => e
    puts "Error processing payment general: #{e.message} - #{e.backtrace[0..10].join("\n")}"
    record_failure(processor)
    false
  end

  def try_processor(processor_name, payload)
    Sync do |task|
      task.with_timeout(10) do
        url = "http://payment-processor-#{processor_name}:8080/payments"
        headers = [['content-type', 'application/json']]
        time_request = Time.now.iso8601(3)
        payload['requestedAt'] = time_request
        body = payload.to_json

        # Request will timeout after 2 seconds
        response = Async::HTTP::Internet.post(url, headers, body)
        ok = response.status >= 200 && response.status < 300
        [ok, time_request]
      ensure
        response&.close
      end
    rescue
      [false, nil]
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
      @fallback_mode_start_time = nil # Reset fallback mode when default succeeds
    when 'fallback'
      @last_failure_fallback_time = nil
    else
      # no-op
    end
  end

  def target_processor
    current_time = Time.now

    default_is_outage = !@last_failure_default_time.nil? && current_time - @last_failure_default_time > 10
    fallback_less_of_3_seconds = @fallback_mode_start_time.nil? || (!@fallback_mode_start_time.nil? && current_time - @fallback_mode_start_time <= 3)

    if default_is_outage
      if fallback_less_of_3_seconds
        @fallback_mode_start_time = current_time
        # If fallback is already in use and less than 3 seconds, continue using it
        return 'fallback'
      end
    end

    'default'
  end
end
