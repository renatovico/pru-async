require 'time'
require 'json'
require 'bigdecimal'

require 'async'

class Store
  def initialize(redis_client:)
    @redis = redis_client
  end

  def save(correlation_id:, processor:, amount:, timestamp: )
    # return if @redis.call('GET', "processed:#{correlation_id}")

    # Use a simple transaction to ensure both updates happen together.
    # Store only the amount as the member in a sorted set keyed by timestamp score for fast range queries.
    ts = Time.parse(timestamp).to_f
    # Log.info('payment_saved', correlation_id: correlation_id, processor: processor, amount: amount, timestamp: timestamp)

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
      'default' => { totalRequests: 0, totalAmount: BigDecimal("0.00") },
      'fallback' => { totalRequests: 0, totalAmount: BigDecimal("0.00") }
    }

    payments_default = @redis.call('ZRANGEBYSCORE', 'payments_log_default', from_score, to_score)
    payments_fallback = @redis.call('ZRANGEBYSCORE', 'payments_log_fallback', from_score, to_score)

    summary['default'][:totalRequests] += payments_default&.size || 0
    payments_default&.each do |data|
      amount_str = JSON.parse(data)['a']
      summary['default'][:totalAmount] += BigDecimal(amount_str, 12)
    end

    summary['fallback'][:totalRequests] += payments_fallback&.size || 0
    payments_fallback&.each do |data|
      amount_str = JSON.parse(data)['a']
      summary['fallback'][:totalAmount] += BigDecimal(amount_str, 12)
    end

    # Round to 2 decimal places to avoid floating-point precision issues
    summary.each do |processor, data|
      data[:totalAmount] = ("%.2f" % data[:totalAmount]).to_f
    end

    summary
  end
end
