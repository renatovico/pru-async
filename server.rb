# $stdout.sync = true

require 'json'
require 'cgi'
require 'async'
require 'async/http/server'
require 'async/http/endpoint'
require "async/http/protocol/response"
require 'async/redis'
require_relative 'app/store'
require_relative 'app/payment_job'
require_relative 'app/job_queue'
require 'async/container/notify/console'

class PruApp
  def initialize(notify:)
    @notify = notify

    redis_endpoint = Async::Redis::Endpoint.parse(ENV['REDIS_URL'] || 'redis://redis:6379/0')
    redis_client = Async::Redis::Client.new(redis_endpoint)
    @store = Store.new(redis_client: redis_client)
    @job_queue = JobQueue.new(
      redis_client: redis_client,
      store: @store
    )

    # Start background tasks using Async in a non-blocking background thread and wait on them.
    Async do
      1.upto(3) do |i|
        notify&.send(status: "job_queue_start", size: i)
        Async do
          @job_queue.start
        end
      end
    end
  end

  def call(request_envt)
    method = request_envt['REQUEST_METHOD']
    path = request_envt['PATH_INFO']
    # Route and obtain response
     case [method, path]
    when ['POST', '/payments']
      handle_payments(request_envt)
    when ['GET', '/payments-summary']
      handle_payments_summary(request_envt)
    when ['GET', '/health']
      handle_health
    when ['POST', '/purge-payments']
      handle_purge_payments(request_envt)
    else
      json(404, { error: 'Not Found' })
    end
  end

  private

  def json(status, object)
    headers = {
      'content-type' => 'application/json',
    }

    [status, headers, [object.to_json]]
  end

  def handle_payments(env)
    request = env["rack.input"]
    body = request.read
    return json(400, { error: 'Bad Request' }) if body.nil?

    Async do
      @job_queue.enqueue body.dup
    end
      json(201, { message: 'Payment created' })
  rescue => e
    @notify&.send(status: "payment_request_failed",  error: e.message)
    json(500, { error: 'Internal Server Error', details: e.message })
  end

  def handle_payments_summary(request)
    params = CGI.parse request["QUERY_STRING"]
    json(200, @store.summary(from: params.fetch('from', []), to: params.fetch('to', [])) )
  rescue => e
    @notify&.send(status: "payments_summary_failed", error: e.message, backtrace: e.backtrace.join("\n"))
    json(500, { error: 'Internal Server Error' })
  end

  def handle_purge_payments(_request)
    @store.purge_all
    json(200, { message: 'purged' })
  end

  def handle_health
    # Deep copy and normalize status code keys to strings for JSON stability
    queue_state = {
      'inflight' =>  @job_queue.inflight,
      'done' => @job_queue.done,
      'errors' => @job_queue.errors,
      'total' => @job_queue.queue.size
    }

    json(200, {
      ok: true,
      queue: queue_state
    })
  end
end
