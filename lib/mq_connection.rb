require 'singleton'

class MQConnection
  # include Singleton

  attr_accessor :connection, :channel, :exchange

  def initialize
    @connection ||= Bunny.new(configatron.rabbitmq.url, {automatic_recovery: true, recover_from_connection_close: true})
    @connection.start
    @channel ||= @connection.create_channel
    @exchange ||= @channel.topic("amq.topic")
  end

  def subscribe(queue, opts)
    @channel.queue(queue).bind(@exchange, opts).subscribe(opts.merge(:block => true)) do |delivery_info, metadata, payload|
      yield delivery_info, payload
    end
  end

  def publish(msg, opts)
    @exchange.publish(msg.to_json, opts)
  end

end
