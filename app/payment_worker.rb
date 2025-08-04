require 'redis'
require 'json'
require 'uri'
require 'net/http'
require_relative 'store'

class PaymentWorker
  def initialize
    @store = Store.new
  end

  def run
    puts "🐦 PaymentWorker starting..."
    
    queue = Queue.new
    
    5.times do
      Thread.new do
        redis = Redis.new(host: 'redis') 
        
        loop do
          job = queue.pop
          process_payment_job(job, redis, Thread.current.object_id)
        end
      end
    end
    
    redis = Redis.new(host: 'redis')

    loop do
      puts "🐦 Waiting for payment jobs..."
      _, result = redis.brpop("payments_queue")
      
      queue.push(result)
    end
  end

  private

  def process_payment_job(result, redis_conn, thread_id)
    payload = JSON.parse(result)
    correlation_id = payload['correlationId']
    puts "🐦 Processing payment: #{correlation_id} [Thread: #{thread_id}]"

    if already_processed_anywhere?(correlation_id, redis_conn)
      puts "🐦 Payment #{correlation_id} already processed, skipping"
      return
    end

    retry_count = increment_retry_count(correlation_id, redis_conn)
    
    if retry_count > 3
      puts "🐦 Payment #{correlation_id} exceeded retry limit, sending to DLQ"
      redis_conn.lpush("payments_dlq", result)
      return
    end

    process_payment(payload, redis_conn)
  rescue Socket::ResolutionError => e
    puts "🐦 Network error: #{e.message} [Thread: #{thread_id}]"

    # Only requeue if not already processed and under retry limit
    unless already_processed_anywhere?(payload['correlationId'], redis_conn)
      redis_conn.lpush("payments_queue", result) if result
    end
  rescue StandardError => e
    puts "🐦 Unexpected error: #{e.message} [Thread: #{thread_id}]"

    # Only requeue if not already processed and under retry limit
    unless already_processed_anywhere?(payload['correlationId'], redis_conn)
      redis_conn.lpush("payments_queue", result) if result
    end
  end

  def process_payment(payload, redis_conn)
    success = try_processor('default', payload, redis_conn)
    return if success
    puts "🐦 Default processor failed, trying fallback"

    success = try_processor('fallback', payload, redis_conn)
    return if success
    puts "🐦 Fallback processor failed, sending to DLQ"

    send_to_dlq(payload, redis_conn)
  end

  def try_processor(processor_name, payload, redis_conn)
    correlation_id = payload['correlationId']
    
    # Check if already processed to avoid duplicates
    if already_processed?(correlation_id, processor_name, redis_conn)
      puts "🐦 Payment #{correlation_id} already processed by #{processor_name}, skipping"
      return true
    end

    endpoint = "http://payment-processor-#{processor_name}:8080/payments"
    
    begin
      response = make_request(endpoint, payload)

      case response
      when Net::HTTPSuccess
        mark_as_processed(correlation_id, processor_name, redis_conn)
        requested_at = Time.parse(payload['requestedAt']) rescue Time.now
        @store.save(processor: processor_name, amount: payload['amount'], timestamp: requested_at)
        puts "🐦 Payment successful via #{processor_name}: #{correlation_id}"
        true
      when Net::HTTPClientError
        puts "🐦 Client error #{processor_name}: [#{response.code}] #{response.body}"
        false
      when Net::HTTPServerError
        puts "🐦 Server error #{processor_name}: [#{response.code}] #{response.body}"
        false
      else
        puts "🐦 Unknown error #{processor_name}: [#{response.code}] #{response.body}"
        false
      end
    rescue => e
      puts "🐦 Request failed to #{processor_name}: #{e.message}"
      false
    end
  end

  def send_to_dlq(payload, redis_conn)
    correlation_id = payload['correlationId']
    redis_conn.lpush("payments_dlq", payload.to_json)
    puts "🐦 Payment #{correlation_id} failed both processors, sent to DLQ"
  end

  def already_processed?(correlation_id, processor_name, redis_conn)
    key = "processed:#{correlation_id}:#{processor_name}"
    puts "🐦 Checking if payment #{correlation_id} already processed by #{processor_name}"
    redis_conn.exists?(key)
  end

  def already_processed_anywhere?(correlation_id, redis_conn)
    already_processed?(correlation_id, 'default', redis_conn) || already_processed?(correlation_id, 'fallback', redis_conn)
  end

  def mark_as_processed(correlation_id, processor_name, redis_conn)
    key = "processed:#{correlation_id}:#{processor_name}"
    redis_conn.setex(key, 3600, "1")  # Expire after 1 hour
  end

  def increment_retry_count(correlation_id, redis_conn)
    key = "retry:#{correlation_id}"
    redis_conn.incr(key).tap do |count|
      redis_conn.expire(key, 3600)  # Expire after 1 hour
    end
  end

  def make_request(endpoint, payload)
    uri = URI(endpoint)
    http = Net::HTTP.new(uri.host, uri.port)
    http.open_timeout = 5
    http.read_timeout = 10
    
    request = Net::HTTP::Post.new(uri.path, 'Content-Type' => 'application/json')
    request.body = payload.to_json
    
    http.request(request)
  end
end
