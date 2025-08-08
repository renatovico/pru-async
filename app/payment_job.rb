require 'sidekiq'
require 'redis'
require 'json'
require 'net/http'
require 'timeout'

require_relative 'store'
require_relative 'redis_pool'

class PaymentJob
  include Sidekiq::Worker
  sidekiq_options retry: 30  # ou retry: true para infinito

  def perform(correlation_id, amount, requested_at)
    return if RedisPool.with { |redis| redis.get("processed:#{correlation_id}") }

    payload = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    store = Store.new

    3.times do |attempt|
      if try_processor('default', payload, timeout: 0.3)
        store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
        puts "🐦 Payment #{correlation_id} processed by default (attempt #{attempt + 1})"
        return
      end
      
      # Small delay between retries
      sleep(0.002 * (attempt + 1)) if attempt < 2
    end

    # Try fallback processor if default failed (with aggressive timeout)
    if try_processor('fallback', payload, timeout: 0.1)
      store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
      puts "🐦 Payment #{correlation_id} processed by fallback"
      return
    end

    raise "Processors both failed for #{correlation_id}"
  end

  def try_processor(processor_name, payload, timeout: nil)
    endpoint = "http://payment-processor-#{processor_name}:8080/payments"
    uri = URI(endpoint)
    http = Net::HTTP.new(uri.host, uri.port)
    
    if timeout
      http.open_timeout = timeout
      http.read_timeout = timeout
    end
    
    request = Net::HTTP::Post.new(uri.path, 'Content-Type' => 'application/json')
    request.body = payload.to_json
    response = http.request(request)
    response.is_a?(Net::HTTPSuccess)
  rescue Net::OpenTimeout, Net::ReadTimeout
    false
  end
end
