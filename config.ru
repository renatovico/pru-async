# frozen_string_literal: true

require_relative './server'

notify = Async::Container::Notify::Console.open!

# Boot dependencies (Redis, Store, HealthMonitor, JobQueue) and start background services.
app = PruApp.new(notify: notify)
run app
