#!/usr/bin/env ruby
# encoding: utf-8

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../../lib/")
require "configuration"
require "mq_connection"
require "processors/box_s3_upload_processor"

puts "Started Box S3 Upload Consumer"
begin
  loop do
    begin
      MQConnection.instance.subscribe("box_s3_sync", {:routing_key => "box_connector.sync.s3.requested"}) do |delivery_info, metadata, payload|
        payload = HashWithIndifferentAccess.new(JSON.parse(payload))
        payload[:routing_key] = delivery_info.routing_key
        BoxS3UploadProcessor.new.on_message(payload)
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
