require 'async'
require 'async/redis'

module RedisPool
  class Adapter
    def initialize(client)
      @client = client
    end

    # Simple command helpers matching existing usage:
    def get(key)
      @client.call('GET', key)
    end

    def set(key, value, ex: nil)
      if ex
        @client.call('SET', key, value.to_s, 'EX', ex.to_i)
      else
        @client.call('SET', key, value.to_s)
      end
    end

    def incr(key)
      @client.call('INCR', key)
    end

    def incrby(key, increment)
      @client.call('INCRBY', key, increment.to_i)
    end

    def incrbyfloat(key, increment)
      @client.call('INCRBYFLOAT', key, increment.to_s)
    end

    def zadd(key, score, member)
      @client.call('ZADD', key, score.to_f, member)
    end

    def zrangebyscore(key, min, max)
      @client.call('ZRANGEBYSCORE', key, min, max)
    end

    def flushdb
      @client.call('FLUSHDB')
    end

    # Naive transactional wrapper similar to redis.multi { ... }
    def multi
      @client.call('MULTI')
      yield self
    ensure
      @client.call('EXEC')
    end
  end

  # Yield an adapter bound to an Async::Task-local client when possible.
  # If not within an Async task, a temporary client is created and closed after yielding.
  def self.with
    task = begin
      Async::Task.current?
    rescue
      nil
    end

    if task
      client = task.instance_variable_get(:@redis_client)
      unless client
        client = Async::Redis::Client.new(build_endpoint)
        task.instance_variable_set(:@redis_client, client)
      end
      yield Adapter.new(client)
    else
      client = Async::Redis::Client.new(build_endpoint)
      begin
        yield Adapter.new(client)
      ensure
        client.close
      end
    end
  end

  def self.close
    task = begin
      Async::Task.current?
    rescue
      nil
    end
    if task
      if (client = task.instance_variable_get(:@redis_client))
        client.close
        task.remove_instance_variable(:@redis_client)
      end
    end
  end

  def self.build_endpoint
    url = ENV['REDIS_URL'] || 'redis://redis:6379/0'
    if defined?(Async::Redis::Endpoint) && Async::Redis::Endpoint.respond_to?(:parse)
      Async::Redis::Endpoint.parse(url)
    else
      Async::Redis.local_endpoint
    end
  end
end
