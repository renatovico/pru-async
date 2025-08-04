require 'redis'
require 'json'
require 'uri'
require 'net/http'
require_relative 'store'

class PaymentWorker
  def initialize
    @redis = Redis.new(host: 'redis')
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
    
    loop do
      puts "🐦 Waiting for payment jobs..."
      _, result = @redis.brpop("payments_queue")
      
      queue.push(result)
    end
  end

  private

  def process_payment_job(result, redis_conn, thread_id)
    payload = JSON.parse(result)
    puts "🐦 Processing payment: #{payload['correlationId']} [Thread: #{thread_id}]"

    process_payment(payload, redis_conn)
  rescue Socket::ResolutionError => e
    puts "🐦 Network error: #{e.message} [Thread: #{thread_id}]"
    redis_conn.lpush("payments_queue", result) if result
  rescue StandardError => e
    puts "🐦 Unexpected error: #{e.message} [Thread: #{thread_id}]"
    redis_conn.lpush("payments_queue", result) if result
  end

  def process_payment(payload, redis_conn)
    success = try_processor('default', payload)
    return if success

    success = try_processor('fallback', payload)
    return if success

    send_to_dlq(payload, redis_conn)
  end

  def try_processor(processor_name, payload)
    endpoint = "http://payment-processor-#{processor_name}:8080/payments"
    response = make_request(endpoint, payload)

    case response
    when Net::HTTPSuccess
      requested_at = Time.parse(payload['requestedAt']) rescue Time.now
      @store.save(processor: processor_name, amount: payload['amount'], timestamp: requested_at)
      puts "🐦 Payment successful via #{processor_name}: #{payload['correlationId']}"
      true
    when Net::HTTPClientError
      puts "🐦 Client error #{processor_name}: [#{res.code}] #{res.body}"
      false
    when Net::HTTPServerError
      puts "🐦 Server error #{processor_name}: [#{res.code}] #{res.body}"
      false
    else
      puts "🐦 Unknown error #{processor_name}: [#{res.code}] #{res.body}"
      false
    end
  end

  def send_to_dlq(payload, redis_conn)
    correlation_id = payload['correlationId']
    redis_conn.lpush("payments_dlq", payload.to_json)
    puts "🐦 Payment #{correlation_id} failed both processors, sent to DLQ"
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
