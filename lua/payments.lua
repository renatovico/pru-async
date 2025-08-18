local cjson = require "cjson.safe"
-- Removed resty.http dependency; implementing a minimal HTTP client with cosockets.

local _M = {}

local function processor_host(name)
  return string.format("http://payment-processor-%s:8080", name)
end

-- Simple in-memory summary (will reset on container restart). Could be swapped for redis later.
local summary = {
  default = { totalRequests = 0, totalAmount = 0.0 },
  fallback = { totalRequests = 0, totalAmount = 0.0 }
}

local function pick_processor(state)
  -- Very simplified: always use default for now.
  return "default"
end

local function simple_http_post(host, port, path, body, headers)
  local sock = ngx.socket.tcp()
  sock:settimeout(4000)
  local ok, err = sock:connect(host, port)
  if not ok then return nil, err end
  local req_headers = {
    string.format("POST %s HTTP/1.1", path),
    "Host: " .. host,
    "Content-Type: application/json",
    "Content-Length: " .. #body,
  }
  if headers then
    for k,v in pairs(headers) do
      table.insert(req_headers, k .. ": " .. v)
    end
  end
  table.insert(req_headers, "Connection: close")
  local payload = table.concat(req_headers, "\r\n") .. "\r\n\r\n" .. body
  local bytes, send_err = sock:send(payload)
  if not bytes then sock:close(); return nil, send_err end
  local line, read_err = sock:receive("*l")
  if not line then sock:close(); return nil, read_err end
  local status = tonumber(line:match("HTTP/%d%.%d%s+(%d+)%s")) or 0
  -- Drain headers
  while true do
    local h = sock:receive("*l")
    if not h or h == "" then break end
  end
  sock:close()
  return { status = status }, nil
end

function _M.handle_payment()
  ngx.req.read_body()
  local body = ngx.req.get_body_data() or ""
  local decoded = cjson.decode(body)
  if not decoded or not decoded.correlationId or not decoded.amount then
    ngx.status = 400
    ngx.header['Content-Type'] = 'application/json'
    ngx.say('{"error":"Bad Request"}')
    return ngx.exit(ngx.HTTP_OK)
  end

  local processor = pick_processor()
  local payload = cjson.encode({
    correlationId = decoded.correlationId,
    amount = decoded.amount,
    requestedAt = os.date('!%Y-%m-%dT%H:%M:%S') .. '.000Z'
  })
  local upstream_host = string.format("payment-processor-%s", processor)
  local res, err = simple_http_post(upstream_host, 8080, "/payments", payload)

  if not res or res.status >= 400 then
    ngx.status = 502
    ngx.header['Content-Type'] = 'application/json'
    ngx.say('{"error":"Upstream Failure"}')
    return ngx.exit(ngx.HTTP_OK)
  end

  -- Update summary
  summary[processor].totalRequests = summary[processor].totalRequests + 1
  summary[processor].totalAmount = summary[processor].totalAmount + tonumber(decoded.amount)

  ngx.status = 201
  ngx.header['Content-Type'] = 'application/json'
  ngx.say('{"message":"Payment created"}')
end

function _M.handle_summary()
  ngx.status = 200
  ngx.header['Content-Type'] = 'application/json'
  ngx.say(cjson.encode(summary))
end

function _M.handle_purge()
  for k,_ in pairs(summary) do
    summary[k].totalRequests = 0
    summary[k].totalAmount = 0.0
  end
  ngx.status = 200
  ngx.header['Content-Type'] = 'application/json'
  ngx.say('{"message":"purged"}')
end

return _M
