$stdout.sync = true
require_relative 'app/payment_worker'

trap("INT") do
  puts "\n🐦 PaymentWorker shutting down gracefully..."
  exit
end

trap("TERM") do
  puts "\n🐦 PaymentWorker shutting down gracefully..."
  exit
end

if __FILE__ == $0
  worker = PaymentWorker.new
  worker.run
end
