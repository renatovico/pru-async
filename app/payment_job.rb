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

  def perform_now(job)
    processor = target_processor
    if processor.nil?
      return false # without processor now
    end

    # Enqueue work into background workers (outside the HTTP request fiber).

    payload = job[2]

    if payload.nil?
      data = JSON.parse(job[1])
      raise 'Missing correlationId' if data['correlationId'].nil?
      raise 'Missing amount' if data['amount'].nil?

      job[2] = {
        correlationId: data['correlationId'],
        amount: data['amount'],
      }
    end

    retries = job[0] || 0

    if retries > 0
      if check_result(job[2], job[3])
        @store.save(correlation_id: job[2][:correlationId], processor: processor, amount: job[2][:amount], timestamp: job[2][:requestedAt])
        record_success(processor)
        return true
      end
    end

    job[3] = processor

    # Try default processor first if allowed
    if try_processor(processor, job)
      @store.save(correlation_id: job[2][:correlationId], processor: processor, amount: job[2][:amount], timestamp: job[2][:requestedAt])
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

  def try_processor(processor_name, job)
    Async::Task.current.with_timeout(0.5) do
      url = "http://payment-processor-#{processor_name}:8080/payments"
      headers = [['content-type', 'application/json']]
      job[2][:requestedAt] = Time.now.iso8601(3)
      body = job[2].to_json

      response = Async::HTTP::Internet.post(url, headers, body)
      response.status >= 200 && response.status < 300
    ensure
      response&.close
    end
  rescue
    check_result(job[2], processor_name)
  end

  protected

  def check_result(payload, processor_name)
    url = "http://payment-processor-#{processor_name}:8080/payments/#{payload[:correlationId]}"
    response = Async::HTTP::Internet.get(url)
    # puts "Response status: #{response.status} for correlationId: #{payload[:correlationId]}" if response.status != 404
    response.status >= 200 && response.status < 300
  rescue => e
    puts "Error checking result for #{processor_name}: #{e.message} - #{e.backtrace[0..10].join("\n")}"
    false
  ensure
    response&.close
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
