require 'socket'
require 'json'

class PruServer
  def initialize(port = 3000)
    @port = port
    @server = TCPServer.new('0.0.0.0', @port)
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
      begin
        data = JSON.parse(body)
        correlation_id = data["correlationId"]
        amount = data["amount"]
        
        if correlation_id && amount
          puts "🐦 Payment received: #{correlation_id} - $#{amount}"
          send_response(client, 200, { message: "payment received", correlationId: correlation_id })
        else
          send_response(client, 400, { error: "Missing correlationId or amount" })
        end
      rescue JSON::ParserError
        send_response(client, 400, { error: "Invalid JSON" })
      end
    else
      send_response(client, 400, { error: "Empty body" })
    end
  end

  def handle_payments_summary(client, headers)
    puts "🐦 Summary requested"
    
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

  def send_response(client, status, data)
    status_text = case status
                  when 200 then "OK"
                  when 400 then "Bad Request"
                  when 404 then "Not Found"
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
