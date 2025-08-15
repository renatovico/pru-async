require 'time'
require 'json'

require 'async'

class Store
  def initialize(redis_client:)
    @redis = redis_client
  end

  def save(correlation_id:, processor:, amount:, timestamp: )
    ts = Time.parse(timestamp).to_f
    @redis.zadd("payments_log_#{processor}", ts, {a: amount.to_f.to_s, c: correlation_id}.to_json)
  end

  # Summarize payments, optionally filtered by timestamp window
  def summary(from: nil, to: nil)
    calculate_filtered_summary(from, to)
  end

  def purge_all
    @redis.call('FLUSHDB')
  end

  # Create a payment status if not already started.
  # Returns true if accepted, false if duplicate.
  def begin_payment(correlation_id:, ttl: 3600)
    # Use a simple NX lock to deduplicate starts.
    started_key = "payment_started:#{correlation_id}"
    set_res = @redis.call('SET', started_key, '1', 'NX', 'EX', ttl)
    return false unless set_res # nil when already exists

    true
  end

  def remove_payment(correlation_id:)
    key = "payment_status:#{correlation_id}"
    @redis.call('DEL', key)
  end

  private

  def calculate_filtered_summary(from, to)
    from_score = from ? Time.parse(from).to_f : '-inf'
    to_score = to ? Time.parse(to).to_f : '+inf'

    summary = {
      'default' => { totalRequests: 0, totalAmount: 0.00 },
      'fallback' => { totalRequests: 0, totalAmount: 0.00 }
    }

    # Use pipelining to fetch both sets of data in a single roundtrip
    default_payments = @redis.call('ZRANGEBYSCORE', 'payments_log_default', from_score, to_score)
    fallback_payments =  @redis.call('ZRANGEBYSCORE', 'payments_log_fallback', from_score, to_score)

    summary['default'][:totalRequests] += default_payments&.size || 0
    default_payments&.each do |data|
      amount_str = JSON.parse(data)['a']
      summary['default'][:totalAmount] += amount_str.to_f
    end

    summary['fallback'][:totalRequests] += fallback_payments&.size || 0
    fallback_payments&.each do |data|
      amount_str = JSON.parse(data)['a']
      summary['fallback'][:totalAmount] += amount_str.to_f
    end

    # Round to 2 decimal places to avoid floating-point precision issues
    summary.each do |processor, data|
      data[:totalAmount] = data[:totalAmount].round(2)
    end

    summary
  end
end
