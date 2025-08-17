# $stdout.sync = true

require 'json'
require 'cgi'
require 'async'
require 'async/http/server'
require 'async/http/endpoint'
require "async/http/protocol/response"
require 'async/redis'
require 'console'
require_relative 'app/store'
require_relative 'app/payment_job'
require_relative 'app/job_queue'

class PruApp
  def initialize(redis_client:, store:)
    @store = store
    @job_queue = JobQueue.new(
      redis_client: redis_client,
      store: store
    )

    # Pre-allocate commonly used responses for better performance
    @payment_created_response = json(201, { message: 'Payment created' })
    @bad_request_response = json(400, { error: 'Bad Request' })
    @not_found_response = json(404, { error: 'Not Found' })
    @internal_error_response = json(500, { error: 'Internal Server Error' })

     Async do |task|
       task.defer_stop do
        @job_queue.start(task)
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
    when ['POST', '/purge-payments']
      handle_purge_payments(request_envt)
    else
      @not_found_response
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
    Async do |task|
      task.defer_stop do
        request = env["rack.input"]
        body = request.read
        @job_queue.enqueue(body)
      end
    end

    @payment_created_response
  rescue => e
    Console.error("payment_request_failed", error: e.message)
    @internal_error_response
  end

  def handle_payments_summary(request)
    params = CGI.parse request["QUERY_STRING"]
    Async do
      sleep 1 # wait for the job queue to process some payments
    end
    json(200, @store.summary(from: params.fetch('from', []), to: params.fetch('to', [])))
  rescue => e
    Console.error("payments_summary_failed", error: e.message)
    @internal_error_response
  end

  def handle_purge_payments(_request)
    @store.purge_all
    json(200, { message: 'purged' })
  end
end
