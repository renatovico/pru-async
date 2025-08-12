$stdout.sync = true

require 'async'
require 'async/http/server'
require 'async/http/endpoint'
require 'async/http/client'
require 'protocol/http/headers'
require 'json'

# A tiny reverse proxy using async-http.
# - Round-robin load balancing across upstreams.
# - Persistent connections (keep-alive) to upstreams.
# - Per-request timeout and simple failover.
class ProxyApp
  def initialize(upstreams:, read_timeout: 3)
    raise ArgumentError, 'at least one upstream is required' if upstreams.nil? || upstreams.empty?

    @endpoints = upstreams.map { |url| Async::HTTP::Endpoint.parse(url) }
    @clients = @endpoints.map { |ep| Async::HTTP::Client.new(ep, protocol: ep.protocol) }
    @read_timeout = read_timeout
    @index = 0
    @index_lock = Async::Semaphore.new(1)
  end

  # Select the next upstream starting offset for this request.
  def next_start_index
    @index_lock.acquire do
      start = @index
      @index = (start + 1) % @clients.length
      start
    end
  end

  def call(request)
  start = next_start_index
  method = request.method
  idempotent = %w[GET HEAD OPTIONS TRACE PUT DELETE].include?(method)
    last_error = nil

  attempts = idempotent ? @clients.length : 1

  # Buffer body so we can retry safely (and avoid streaming original request twice).
  body_data = request.body ? request.read : nil

  attempts.times do |offset|
      client = @clients[(start + offset) % @clients.length]

      begin
        # Clone headers and adjust Host header for the selected upstream authority.
        headers = Protocol::HTTP::Headers.new

        hop_by_hop = %w[
          connection keep-alive proxy-connection upgrade
          proxy-authenticate proxy-authorization te trailers transfer-encoding
        ]

        request.headers&.each do |key, value|
          next if hop_by_hop.include?(key.downcase)

          if value.is_a?(Array)
            value.each { |v| headers.add(key, v) }
          else
            headers.add(key, value)
          end
        end

        #headers['host'] = client.endpoint.authority
        headers.add('x-forwarded-proto', request.scheme) if request.scheme
        headers.add('x-forwarded-host', request.authority) if request.authority
        headers.add('via', '1.1 pru-proxy')

        # Proxy the request with a timeout.
        response = Async::Task.current.with_timeout(@read_timeout) do
          outgoing = ::Protocol::HTTP::Request[method, request.path, headers, body_data]
          client.call(outgoing)
        end

        # Return upstream response as-is. The body will stream from the persistent client connection.
        return response
      rescue Async::TimeoutError => e
        last_error = e
        next
      rescue => e
        last_error = e
        next
      end
    end

    # All upstreams failed:
    body = { error: 'Bad Gateway', message: last_error&.message, backtrace: last_error&.backtrace }.to_json
    ::Protocol::HTTP::Response[502, { 'content-type' => 'application/json' }, [body]]
  end
end

if __FILE__ == $0
  port = Integer(ENV.fetch('PORT', '9999'))
  upstreams = ENV.fetch('UPSTREAMS', 'http://api01:3000,http://api02:3000').split(',').map(&:strip)
  endpoint = Async::HTTP::Endpoint.parse("http://0.0.0.0:#{port}")

  app = ProxyApp.new(upstreams: upstreams, read_timeout: Integer(ENV.fetch('READ_TIMEOUT', '3')))

  server = Async::HTTP::Server.for(endpoint) do |request|
    begin
      puts "➡️  #{request.method} #{request.path}"
      app.call(request)
    rescue => e
      puts "❌ Proxy error: #{e.class}: #{e.message}"
      ::Protocol::HTTP::Response[500, { 'content-type' => 'application/json' }, [{ error: 'Proxy Error' }.to_json]]
    end
  end

  puts "🪄 Proxy listening on :#{port}, upstreams=#{upstreams.join(', ')}"
  Async do
    server.run
  end
end
