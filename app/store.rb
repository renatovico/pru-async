require 'redis'
require 'time'
require 'json'

class Store
  def initialize
    @redis = Redis.new(host: 'redis')
  end
  
  def save(correlation_id:, processor:, amount:, timestamp: Time.now)
    payment_data = {
      processor: processor,
      correlationId: correlation_id,
      amount: amount,
      timestamp: timestamp
    }

    @redis.multi do
      @redis.set("processed:#{correlation_id}", 1, ex: 3600)
      @redis.lpush('payments_log', payment_data.to_json)
      @redis.incr("totalRequests:#{processor}")
      @redis.incrbyfloat("totalAmount:#{processor}", amount)
    end
  end
  
  def summary(from: nil, to: nil)
    if from || to
      calculate_filtered_summary(from, to)
    else
      %w[default fallback].each_with_object({}) do |type, hash|
        hash[type] = {
          totalRequests: @redis.get("totalRequests:#{type}").to_i,
          totalAmount: @redis.get("totalAmount:#{type}").to_f.round(2)
        }
      end
    end
  end
  
  def purge_all
    @redis.flushdb
  end
  
  private
  
  def calculate_filtered_summary(from, to)
    from_time = from ? Time.parse(from) : nil
    to_time = to ? Time.parse(to) : nil
    
    summary = {
      'default' => { totalRequests: 0, totalAmount: 0.0 },
      'fallback' => { totalRequests: 0, totalAmount: 0.0 }
    }
    
    payments = @redis.lrange('payments_log', 0, -1)
    
    payments.each do |payment_json|
      payment = JSON.parse(payment_json)
      payment_time = Time.parse(payment['timestamp'])
      
      next if from_time && payment_time < from_time
      next if to_time && payment_time > to_time
      
      processor = payment['processor']
      amount = payment['amount'].to_f
      
      if summary[processor]
        summary[processor][:totalRequests] += 1
        summary[processor][:totalAmount] += amount
      end
    end
    
    # Round to 2 decimal places to avoid floating-point precision issues
    summary.each do |processor, data|
      data[:totalAmount] = data[:totalAmount].round(2)
    end
    
    summary
  end
end
