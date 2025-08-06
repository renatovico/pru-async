require 'redis'
require 'connection_pool'
require 'singleton'

class RedisPool
  include Singleton

  def initialize
    @pool = ConnectionPool.new(size: 5) do
      Redis.new(host: 'redis')
    end
  end

  def self.with(&block)
    instance.with(&block)
  end

  def with(&block)
    @pool.with(&block)
  end
end
