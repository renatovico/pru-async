require 'redis'
require 'json'
require 'time'

class Enqueuer
  def initialize
    @redis = Redis.new(host: 'redis')
  end

  def enqueue(correlation_id:, amount:)
    payment_data = {
      correlationId: correlation_id,
      amount: amount,
      requestedAt: Time.now.iso8601(3)
    }
    
    @redis.lpush('payments_queue', payment_data.to_json)
  end

  def purge_all
    @redis.flushdb
  end
end
