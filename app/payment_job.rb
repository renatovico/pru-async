require 'sidekiq'
require 'redis'
require 'json'
require 'net/http'

require_relative 'store'

class PaymentJob
  include Sidekiq::Worker
  sidekiq_options retry: 30  # ou retry: true para infinito

  def perform(correlation_id, amount, requested_at)
    redis = Redis.new(host: 'redis')
    return if redis.get("processed:#{correlation_id}")

    payload = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: requested_at
    }

    store = Store.new

    # Try default processor up to 3 times
    3.times do |attempt|
      if try_processor('default', payload)
        store.save(correlation_id: correlation_id, processor: 'default', amount: amount, timestamp: requested_at)
        puts "🐦 Payment #{correlation_id} processed by default (attempt #{attempt + 1})"
        return
      end
      
      # Small delay between retries (except after last attempt)
      sleep(0.5) if attempt < 2
    end

    # Try fallback processor if default failed
    if try_processor('fallback', payload)
      store.save(correlation_id: correlation_id, processor: 'fallback', amount: amount, timestamp: requested_at)
      puts "🐦 Payment #{correlation_id} processed by fallback after 3 default attempts"
      return
    end

    raise "Processors both failed for #{correlation_id}"
  end

  def try_processor(processor_name, payload)
    endpoint = "http://payment-processor-#{processor_name}:8080/payments"
    uri = URI(endpoint)
    http = Net::HTTP.new(uri.host, uri.port)
    request = Net::HTTP::Post.new(uri.path, 'Content-Type' => 'application/json')
    request.body = payload.to_json
    response = http.request(request)
    response.is_a?(Net::HTTPSuccess)
  end
end
