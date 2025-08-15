require 'json'
require 'async'
require 'async/http/internet/instance'

class PaymentJob
  def initialize(store:, redis_client:)
    @store = store
    @redis = redis_client
    @last_failure_default_time = nil
    @fallback_mode_start_time = nil
    @semaphore = Async::Semaphore.new(1)
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

    requested_at = Time.now.iso8601(3)
    payload_job = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    Sync do |task|
      @semaphore.async do |task_2|
        # Try default processor first if allowed
        result = try_processor(processor, payload_job, task: task_2)
        if result
          @store.save(correlation_id: correlation_id, processor: processor, amount: amount, timestamp: requested_at)
          record_success(processor)
          true
        else
          record_failure(processor)
          false
        end
      end
    end
  end

  def try_processor(processor_name, payload, task: nil)
    url = "http://payment-processor-#{processor_name}:8080/payments"
    headers = [['content-type', 'application/json']]
    body = payload.to_json

    # Request will timeout after 2 seconds
    task.with_timeout(3) do
      response = Async::HTTP::Internet.post(url, headers, body)
      ok = response.status >= 200 && response.status < 300
      ok
    end
  rescue => e
    puts "Error processing payment with #{processor_name}: #{e.message}"
    return false
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

    # Always prefer default processor unless it's in outage for more than 30 seconds
    if @last_failure_default_time.nil? || current_time - @last_failure_default_time > 30
      @fallback_mode_start_time = nil # Reset fallback mode
      return 'default'
    end

    # If default is in outage, enter fallback mode if not already in it
    if @fallback_mode_start_time.nil?
      @fallback_mode_start_time = current_time
    end

    # Only use fallback for 3 seconds, then try default again
    if current_time - @fallback_mode_start_time <= 2
      # Check if fallback processor is available
      return 'fallback'
    end

    return 'default'
  rescue => e
    return 'default'
  end
end
