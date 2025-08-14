require 'json'
require 'time'

module Log
  # Numeric levels for simple filtering, compatible with common severities
  LEVELS = {
    'DEBUG' => 10,
    'INFO'  => 20,
    'WARN'  => 30,
    'ERROR' => 40,
    'FATAL' => 50,
  }.freeze

  PROGNAME = 'pru'

  def self.level
    @level ||= begin
      env = ENV['LOG_LEVEL'].to_s.upcase
      LEVELS.fetch(env, LEVELS['INFO'])
    end
  end

  def self.level=(lvl)
    @level = case lvl
    when String then LEVELS.fetch(lvl.upcase, LEVELS['INFO'])
    when Integer then lvl
    else LEVELS['INFO']
    end
  end

  def self.emit(severity, payload)
    return unless LEVELS[severity] >= level

    Async do
      time = Time.now.utc.iso8601(3)
      base = { time: time, level: severity, prog: PROGNAME }
      record = if payload.is_a?(Hash)
        base.merge(payload)
      else
        base.merge(message: payload.is_a?(String) ? payload : payload.inspect)
      end
      $stdout.write(record.to_json)
      $stdout.write("\n")
    end
  end

  def self.debug(msg, **ctx); emit('DEBUG', attach_ctx(msg, ctx)); end
  def self.info(msg, **ctx);  emit('INFO',  attach_ctx(msg, ctx)); end
  def self.warn(msg, **ctx);  emit('WARN',  attach_ctx(msg, ctx)); end
  def self.error(msg, **ctx); emit('ERROR', attach_ctx(msg, ctx)); end

  def self.exception(ex, msg = 'error', **ctx)
    emit('ERROR', attach_ctx(msg, ctx.merge(error: ex.class.name, detail: ex.message)))
  end

  def self.attach_ctx(msg, ctx)
    return msg if ctx.nil? || ctx.empty?
    { event: msg, **ctx }
  end
end
