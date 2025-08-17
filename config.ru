# frozen_string_literal: true

require_relative './server'
require 'async/redis'
require 'console'

redis_endpoint = Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
redis_client = Async::Redis::Client.new(redis_endpoint, concurrency: (ENV['REDIS_CONCURRENCY'] || 512).to_i)
store = Store.new(redis_client: redis_client)

run PruApp.new(store: store, redis_client: redis_client)
