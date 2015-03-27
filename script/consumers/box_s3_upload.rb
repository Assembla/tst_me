#!/usr/bin/env ruby
# encoding: utf-8

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../../lib/")
require 'consumer'

puts "Started Box S3 Upload Consumer"

Consumer.run({queue_name: "box_s3_sync", routing_key: "box_connector.sync.s3.requested", processor: "box_s3_upload_processor"})
