#!/usr/bin/env ruby
# encoding: utf-8

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../../lib/")
require "configuration"
require "mq_connection"
require "processors/box_remote_sync_processor"

puts "Starting Box Remote Sync Consumer"
begin
  processor = BoxRemoteSyncProcessor.new
  mq_connection = MQConnection.new
  channel = mq_connection.channel
  loop do
    begin
      mq_connection.subscribe("box_remote_sync", {:manual_ack => true, :routing_key => "box_connector.sync.*.requested"}) do |delivery_info, payload|
        payload = HashWithIndifferentAccess.new(JSON.parse(payload))
        processor.metadata = delivery_info
        processor.on_message(payload)
        channel.acknowledge(delivery_info.delivery_tag)
      end
    rescue => ex
      puts "Error:\n #{ex}"
      sleep(3)
    end
  end
rescue Interrupt
  puts "Consumer stopped."
  exit(0)
ensure
  mq_connection.connection.close
end
