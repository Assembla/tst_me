#!/usr/bin/env ruby
# encoding: utf-8

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../../lib/")
require 'consumer'

puts "Starting Box Remote Sync Consumer"

Consumer.run({queue_name: "box_remote_sync", routing_key: "box_connector.sync.*.requested", processor: "box_remote_sync_processor"})
