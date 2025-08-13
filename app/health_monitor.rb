require 'json'
require 'time'
require 'async'
require 'async/http/internet/instance'
require_relative 'logger'

# Periodically polls payment processors' health endpoints and caches the results in Redis.
# - Cluster-safe: uses a Redis NX lock to ensure at most 1 request per processor per 5s across all instances.
# - Cached state in Redis so all app instances share the latest health info.
class HealthMonitor
  DEFAULT_LOCK_SECONDS = 5
  DEFAULT_CACHE_TTL = 8 # seconds

  def initialize(redis_client:, processors: %w[default fallback])
    @redis = redis_client
    @processors = processors
    @lock_seconds = (ENV['HEALTH_LOCK_SECONDS'] || DEFAULT_LOCK_SECONDS).to_i
    @cache_ttl = (ENV['HEALTH_CACHE_TTL'] || DEFAULT_CACHE_TTL).to_i
  end

  # Start background polling loops for each processor.
  def start
    Log.info('health_monitor_start', processors: @processors)

    Async do |root|
      @processors.each do |name|
        root.async do |task|
          loop do
            poll_once(name)
            task.sleep(@lock_seconds)
          end
        end
      end
    end
  end

  # Read the last known state from Redis. Returns nil if unknown.
  # Returns string-keyed hash: { 'failing' => bool, 'minResponseTime' => Integer(ms), 'statusCode' => Integer, 'updatedAt' => String }
  def get(processor)
    flat = @redis.call('HGETALL', state_key(processor)) || []
    return nil if flat.empty?
    h = Hash[*flat]
    {
      'failing' => (h['failing'] == '1'),
      'minResponseTime' => (h['minResponseTime'] || '0').to_i,
      'statusCode' => (h['statusCode'] || '0').to_i,
      'updatedAt' => h['updatedAt']
    }
  end

  # Compute a suggested timeout based on health, clamped to sensible bounds.
  def suggested_timeout(processor, default: 1.0)
    state = get(processor)
    return default unless state

    factor = (ENV['HEALTH_TIMEOUT_FACTOR'] || '3.0').to_f
    min_s = (ENV['HEALTH_TIMEOUT_MIN'] || '0.10').to_f
    max_s = (ENV['HEALTH_TIMEOUT_MAX'] || '1.00').to_f

    ms = state['minResponseTime'].to_i
    t = (ms.to_f / 1000.0) * factor
    t = [[t, min_s].max, max_s].min
    t
  end

  private

  def poll_once(processor)
    # Ensure cluster-wide rate limit: acquire lock (NX, EX)
    got = @redis.call('SET', lock_key(processor), '1', 'NX', 'EX', @lock_seconds)
    return unless got

    url = "http://payment-processor-#{processor}:8080/payments/service-health"
    response = nil
    begin
      response = Async::HTTP::Internet.get(url)
      status = response.status

      body = nil
      begin
        body = response.read
      rescue
        # ignore read errors, we'll record status only
      end

      failing = nil
      min_rt = nil
      if status == 200 && body && !body.empty?
        begin
          parsed = JSON.parse(body)
          failing = !!parsed['failing']
          min_rt = parsed['minResponseTime'].to_i
        rescue => e
          Log.debug(e, kind: 'health_parse_error', processor: processor)
        end
      end

      fields = ['statusCode', status.to_s, 'updatedAt', Time.now.utc.iso8601(3)]
      fields += ['failing', failing ? '1' : '0'] unless failing.nil?
      fields += ['minResponseTime', min_rt.to_s] unless min_rt.nil?

      @redis.call('HMSET', state_key(processor), *fields)
      @redis.call('EXPIRE', state_key(processor), @cache_ttl)
    rescue => e
      Log.debug(e, kind: 'health_poll_error', processor: processor)
    ensure
      begin
        response&.close
      rescue
        # ignore
      end
    end
  end

  def state_key(processor)
    "health:state:#{processor}"
  end

  def lock_key(processor)
    "health:lock:#{processor}"
  end
end
