require 'time'
require 'json'
require 'bigdecimal'

require 'async'

class Store
  def initialize(redis_client:)
    @redis = redis_client
  end

  def save(correlation_id:, processor:, amount:, timestamp: Time.now)
    return if @redis.call('GET', "processed:#{correlation_id}")

    payment_data = {
      processor: processor,
      correlationId: correlation_id,
      amount: amount,
      timestamp: timestamp
    }

    timestamp_score = Time.parse(timestamp.to_s).to_f
    # Use a simple transaction to ensure both updates happen together.
    @redis.transaction do |context|
      context.call('SET', "processed:#{correlation_id}", '1', 'EX', 3600)
      context.call('ZADD', 'payments_log', timestamp_score, payment_data.to_json)
      set_status(correlation_id: correlation_id, status: 'done', fields: { processor: processor }, context: context)
    end
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
  def begin_payment(correlation_id:, amount:, requested_at:, ttl: 3600)
    # Use a simple NX lock to deduplicate starts.
    started_key = "payment_started:#{correlation_id}"
    set_res = @redis.call('SET', started_key, '1', 'NX', 'EX', ttl)
    return false unless set_res # nil when already exists

    key = status_key(correlation_id)
    @redis.transaction do |context|
      context.call('HMSET', key, 'status', 'enqueued', 'requestedAt', requested_at.to_s, 'amount', amount.to_s)
      context.call('EXPIRE', key, ttl)
    end
    true
  end

  # Update status hash for a correlation id.
  def set_status(correlation_id:, status:, fields: {}, context: nil)
    key = status_key(correlation_id)

    if status == 'failed'
      if context
        context.call('HDEL', key, 'status', 'updatedAt')
      else
        @redis.call('HDEL', key, 'status', 'updatedAt')
      end
      return
    end

    now = Time.now.iso8601(3)
    args = ['status', status, 'updatedAt', now]
    fields.each { |k, v| args << k.to_s << v.to_s }
    if context
      context.call('HMSET', key, *args)
    else
      @redis.call('HMSET', key, *args)
    end
  end

  # Fetch status as a string-keyed hash.
  def get_status(correlation_id)
    key = status_key(correlation_id)
    flat = @redis.call('HGETALL', key) || []
    return {} if flat.empty?
    Hash[*flat]
  end

  def status_key(correlation_id)
    "payment_status:#{correlation_id}"
  end

  private

  def calculate_filtered_summary(from, to)
    from_score = from ? Time.parse(from).to_f : '-inf'
    to_score = to ? Time.parse(to).to_f : '+inf'

    summary = {
      'default' => { totalRequests: 0, totalAmount: BigDecimal("0.00") },
      'fallback' => { totalRequests: 0, totalAmount: BigDecimal("0.00") }
    }

    payments = @redis.call('ZRANGEBYSCORE', 'payments_log', from_score, to_score)

    payments.each do |payment_json|
      payment = JSON.parse(payment_json)
      processor = payment['processor']
      amount = payment['amount'].to_f

      if summary[processor]
        summary[processor][:totalRequests] += 1
        summary[processor][:totalAmount] += BigDecimal(amount, 12)
      end
    end

    # Round to 2 decimal places to avoid floating-point precision issues
    summary.each do |processor, data|
      data[:totalAmount] = ("%.2f" % data[:totalAmount]).to_f
    end

    summary
  end
end
