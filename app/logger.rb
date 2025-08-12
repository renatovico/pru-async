require 'logger'
require 'json'
require 'time'

module Log
  LEVELS = {
    'DEBUG' => Logger::DEBUG,
    'INFO' => Logger::INFO,
    'WARN' => Logger::WARN,
    'ERROR' => Logger::ERROR,
    'FATAL' => Logger::FATAL,
  }

  class JsonFormatter
    def call(severity, time, progname, msg)
      base = {
        time: time.utc.iso8601(3),
        level: severity,
        prog: progname,
      }.compact

      record = if msg.is_a?(Hash)
        base.merge(msg)
      else
        base.merge(message: msg.is_a?(String) ? msg : msg.inspect)
      end
      record.to_json + "\n"
    end
  end

  def self.logger
    return @logger if defined?(@logger) && @logger

    @logger = Logger.new($stdout)
    @logger.level = LEVELS.fetch(ENV['LOG_LEVEL'].to_s.upcase, Logger::INFO)
    @logger.progname = 'pru'
    @logger.formatter = JsonFormatter.new
    @logger
  end

  def self.debug(msg, **ctx); logger.debug(attach_ctx(msg, ctx)); end
  def self.info(msg, **ctx); logger.info(attach_ctx(msg, ctx)); end
  def self.warn(msg, **ctx); logger.warn(attach_ctx(msg, ctx)); end
  def self.error(msg, **ctx); logger.error(attach_ctx(msg, ctx)); end

  def self.exception(ex, msg = 'error', **ctx)
    logger.error(attach_ctx(msg, ctx.merge(error: ex.class.name, detail: ex.message)))
  end

  def self.attach_ctx(msg, ctx)
    return msg if ctx.nil? || ctx.empty?
    { event: msg, **ctx }
  end
end
