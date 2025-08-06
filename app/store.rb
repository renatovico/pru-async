require 'redis'
require 'time'
require 'json'

require_relative 'redis_pool'

class Store
  def initialize
  end
  
  def save(correlation_id:, processor:, amount:, timestamp: Time.now)
    payment_data = {
      processor: processor,
      correlationId: correlation_id,
      amount: amount,
      timestamp: timestamp
    }

    RedisPool.with do |redis|
      timestamp_score = Time.parse(timestamp.to_s).to_f
      redis.multi do
        redis.set("processed:#{correlation_id}", 1, ex: 3600)
        redis.zadd('payments_log', timestamp_score, payment_data.to_json)
        redis.incr("totalRequests:#{processor}")
        redis.incrbyfloat("totalAmount:#{processor}", amount)
      end
    end
  end
  
  def summary(from: nil, to: nil)
    if from || to
      calculate_filtered_summary(from, to)
    else
      RedisPool.with do |redis|
        %w[default fallback].each_with_object({}) do |type, hash|
          hash[type] = {
            totalRequests: redis.get("totalRequests:#{type}").to_i,
            totalAmount: redis.get("totalAmount:#{type}").to_f.round(2)
          }
        end
      end
    end
  end
  
  def purge_all
    RedisPool.with { |redis| redis.flushdb }
  end
  
  private
  
  def calculate_filtered_summary(from, to)
    from_score = from ? Time.parse(from).to_f : '-inf'
    to_score = to ? Time.parse(to).to_f : '+inf'
    
    summary = {
      'default' => { totalRequests: 0, totalAmount: 0.0 },
      'fallback' => { totalRequests: 0, totalAmount: 0.0 }
    }
    
    RedisPool.with do |redis|
      payments = redis.zrangebyscore('payments_log', from_score, to_score)
      
      payments.each do |payment_json|
        payment = JSON.parse(payment_json)
        processor = payment['processor']
        amount = payment['amount'].to_f
        
        if summary[processor]
          summary[processor][:totalRequests] += 1
          summary[processor][:totalAmount] += amount
        end
      end
    end
    
    # Round to 2 decimal places to avoid floating-point precision issues
    summary.each do |processor, data|
      data[:totalAmount] = data[:totalAmount].round(2)
    end
    
    summary
  end
end
