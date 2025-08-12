require 'time'
require 'json'
require 'bigdecimal'

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
      end
    end
  end

  # Summarize payments, optionally filtered by timestamp window
  def summary(from: nil, to: nil)
    calculate_filtered_summary(from, to)
  end

  def purge_all
    RedisPool.with { |redis| redis.flushdb }
  end

  private

  def calculate_filtered_summary(from, to)
    from_score = from ? Time.parse(from).to_f : '-inf'
    to_score = to ? Time.parse(to).to_f : '+inf'

    summary = {
      'default' => { totalRequests: 0, totalAmount: BigDecimal("0.00") },
      'fallback' => { totalRequests: 0, totalAmount: BigDecimal("0.00") }
    }

    RedisPool.with do |redis|
      payments = redis.zrangebyscore('payments_log', from_score, to_score)

      payments.each do |payment_json|
        payment = JSON.parse(payment_json)
        processor = payment['processor']
        amount = payment['amount'].to_f

        if summary[processor]
          summary[processor][:totalRequests] += 1
          summary[processor][:totalAmount] += BigDecimal(amount, 12)
        end
      end
    end

    # Round to 2 decimal places to avoid floating-point precision issues
    summary.each do |processor, data|
      data[:totalAmount] = ("%.2f" % data[:totalAmount]).to_f
    end

    summary
  end
end
