require 'json'
require 'async'
require 'async/http/internet/instance'
require 'async/http/client'
require 'async/http/endpoint'

class PaymentJob
  HEADERS_JSON = [['content-type', 'application/json']].freeze

  def initialize(store:, redis_client:)
    @store = store
    @redis = redis_client
    @last_failure_default_time = nil
    @fallback_mode_start_time = nil
    @clients = {}
    @request_timeout = (ENV['ASYNC_REQUEST_TIMEOUT'] || 2.5).to_f
    @check_timeout = (ENV['ASYNC_CHECK_TIMEOUT'] || 2).to_f
    @concurrency = (ENV['ASYNC_CONCURRENCY'] || 50).to_i
  end

  def perform_now(job)
    processor = target_processor
    return false if processor.nil?

    payload = job[2]
    if payload.nil?
      data = JSON.parse(job[1])
      raise 'Missing correlationId' if data['correlationId'].nil?
      raise 'Missing amount' if data['amount'].nil?
      job[2] = { correlationId: data['correlationId'], amount: data['amount'] }
    end

    retries = job[0] || 0
    job[3] = processor

    if retries > 0 && check_result(job[2], job[3])
      @store.save(correlation_id: job[2][:correlationId], processor: processor, amount: job[2][:amount], timestamp: job[2][:requestedAt])
      record_success(processor)
      return true
    end

    if try_processor(processor, job)
      record_success(processor)
      true
    else
      record_failure(processor)
      false
    end
  rescue => e
    Console.warn "Error processing payment general: #{e.message} - #{e.backtrace[0..10].join("\n")}" if defined?(Console)
    record_failure(processor)
    false
  end

  def try_processor(processor_name, job)
    client = client_for(processor_name)
    requested_at = Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%LZ")
    job[2][:requestedAt] = requested_at
    body = build_body(job[2])
    Async::Task.current.with_timeout(@request_timeout) do
      response = client.post('/payments', HEADERS_JSON, body)
      success = response.status >= 200 && response.status < 300
      @store.save(correlation_id: job[2][:correlationId], processor: processor_name, amount: job[2][:amount], timestamp: requested_at) if success
      success
    ensure
      response&.finish
    end
  rescue => e
    Console.warn "Error processing payment #{processor_name}: #{e.class}: #{e.message}" if defined?(Console)
    check_result(job[2], processor_name)
  end

  protected

  def check_result(payload, processor_name)
    client = client_for(processor_name)
    Async::Task.current.with_timeout(@check_timeout) do
      response = client.head("/payments/#{payload[:correlationId]}")
      ok = response.status >= 200 && response.status < 300
      ok
    ensure
      response&.finish
    end
  rescue => e
    Console.debug("check_result error #{processor_name}: #{e.class}: #{e.message}") if defined?(Console)
    false
  end

  private

  def client_for(processor)
    @clients[processor] ||= begin
      endpoint = Async::HTTP::Endpoint.parse("http://payment-processor-#{processor}:8080", protocol: Async::HTTP::Protocol::HTTP11)
      Async::HTTP::Client.new(endpoint, retries: 0, concurrency: @concurrency)
    end
  end

  def record_failure(processor)
    case processor
    when 'default' then @last_failure_default_time = Time.now
    when 'fallback' then @last_failure_fallback_time = Time.now
    end
  end

  def record_success(processor)
    case processor
    when 'default'
      @last_failure_default_time = nil
      @fallback_mode_start_time = nil
    when 'fallback'
      @last_failure_fallback_time = nil
    end
  end

  def target_processor
    current_time = Time.now
    default_is_outage = !@last_failure_default_time.nil? && current_time - @last_failure_default_time > 10
    fallback_less_than_3s = @fallback_mode_start_time.nil? || (current_time - @fallback_mode_start_time <= 3)
    if default_is_outage && fallback_less_than_3s
      @fallback_mode_start_time = current_time
      return 'fallback'
    end
    'default'
  end

  def build_body(payload)
    cid = payload[:correlationId]
    amt = payload[:amount]
    ts = payload[:requestedAt]
    safe_cid = cid.gsub(/["\\]/) { |m| "\\#{m}" }
    '{"correlationId":"' + safe_cid + '","amount":' + amt.to_s + ',"requestedAt":"' + ts + '"}'
  end
end
