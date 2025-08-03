require 'socket'
require 'json'
require_relative 'enqueuer'

class PruServer
  def initialize(port = 3000)
    @port = port
    @server = TCPServer.new('0.0.0.0', @port)
    @enqueuer = Enqueuer.new

    puts "🐦 Pru server starting on port #{@port}..."
  end

  def start
    loop do
      client = @server.accept
      handle_request(client)
    end
  end

  private

  def handle_request(client)
    request_line = client.gets
    return unless request_line

    method, path, _ = request_line.split
    headers = parse_headers(client)
    
    case "#{method} #{path}"
    when "POST /payments"
      handle_payments(client, headers)
    when "GET /payments-summary"
      handle_payments_summary(client, headers)
    when "POST /purge-payments"
      handle_purge_payments(client, headers)
    else
      send_response(client, 404, { error: "Not Found" })
    end
  ensure
    client.close
  end

  def parse_headers(client)
    headers = {}
    while (line = client.gets) && line.strip != ""
      key, value = line.strip.split(": ", 2)
      headers[key.downcase] = value
    end
    headers
  end

  def handle_payments(client, headers)
    content_length = headers["content-length"]&.to_i || 0
    body = client.read(content_length) if content_length > 0
    
    if body && !body.empty?
      data = JSON.parse(body)
      correlation_id = data["correlationId"]
      amount = data["amount"]
      
      @enqueuer.enqueue(correlation_id: correlation_id, amount: amount)
      send_response(client, 200, { message: "enqueued" })
    end
  end

  def handle_payments_summary(client, headers)
    summary = {
      "default" => {
        "totalRequests" => 42,
        "totalAmount" => 1250.75
      },
      "fallback" => {
        "totalRequests" => 18,
        "totalAmount" => 675.25
      }
    }
    
    send_response(client, 200, summary)
  end

  def handle_purge_payments(client, headers)
    begin
      @enqueuer.purge_all
      send_response(client, 200, { message: "purged" })
    rescue => e
      puts "🐦 Error purging payments: #{e.message}"
      send_response(client, 500, { error: "Internal server error" })
    end
  end

  def send_response(client, status, data)
    status_text = case status
                  when 200 then "OK"
                  when 404 then "Not Found"
                  when 500 then "Internal Server Error"
                  else "Unknown"
                  end

    json_body = data.to_json
    
    response = [
      "HTTP/1.1 #{status} #{status_text}",
      "Content-Type: application/json",
      "Content-Length: #{json_body.bytesize}",
      "",
      json_body
    ].join("\r\n")

    client.write(response)
  end
end

if __FILE__ == $0
  server = PruServer.new
  server.start
end
