# frozen_string_literal: true

require_relative './server'

notify = Async::Container::Notify::Console.open!



redis_endpoint = Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
redis_client = Async::Redis::Client.new(redis_endpoint)
store = Store.new(redis_client: redis_client)

run PruApp.new(notify: notify, store: store, redis_client: redis_client)
