#!/usr/bin/env ruby
# encoding: utf-8

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../../lib/")
require "configuration"
require "mq_connection"
require "processors/box_remote_sync_processor"

puts "Starting Box Remote Sync Consumer"
begin
  loop do
    begin
      MQConnection.instance.subscribe("box_remote_sync", {:routing_key => "box_connector.sync.*.requested"}) do |delivery_info, metadata, payload|
        payload = HashWithIndifferentAccess.new(JSON.parse(payload))
        payload[:routing_key] = delivery_info.routing_key
        BoxRemoteSyncProcessor.new.on_message(payload)
      end
    rescue => ex
      puts "\n\nError:\n #{ex}"
      sleep(1)
    end
  end
rescue Interrupt
  puts "Consumer stopped."
  exit(0)
ensure
  MQConnection.instance.connection.close
end
