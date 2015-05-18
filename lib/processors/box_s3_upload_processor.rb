# -*- encoding: utf-8 -*-
require "s3_upload"

class BoxS3UploadProcessor < BaseProcessor

  def initialize
    @upload_s3 = S3Upload.new(configatron.aws_s3.access_key_id, configatron.aws_s3.secret_access_key, {logger: logger})
  end

  def on_message(event)
    logger.debug("#{self.class.name} received: #{event.inspect}")

    begin
      upload_list = upload_list(event)
      unless upload_list.blank?
        @upload_s3.box_token = event[:oauth_token]
        success, failures = @upload_s3.send_files(upload_list)
        publish_results(event, success, failures)
      end
    rescue RubyBox::AuthError => ex
      # enqueue token refresh event
      logger.info("Exception: #{ex}")
      logger.info(ex.backtrace.join("\n"))
      invalidate_box_credentials
      resubmit_event(event, metadata.routing_key)
    end
  end

  private

  def publish_results(event, success_files, failed_files)
    unless success_files.blank?
      event[:entries] = success_files
      mq_connection.publish(event, {routing_key: "box_connector.sync.s3.succeed"})
    end
    unless failed_files.blank?
      event[:errors] = failed_files
      mq_connection.publish(event, {routing_key: "box_connector.sync.s3.failed"})
    end
  end

  def upload_list(event)
    file_list = []
    event_files = event[:files]
    if event_files
      event_files.each do |key, file_meta|
        file_list << {key: key, bucket: event[:bucket], id: file_meta[:id], box_id: file_meta[:external_id], index: file_meta[:index]}
      end
    end
    file_list
  end

  def invalidate_box_credentials
    @client = nil
    @box_session = nil
  end

  def resubmit_event(event, routing_key)
    event[:errors] = "refresh_token"
    mq_connection.publish(event, {routing_key: routing_key.gsub("requested", "oauth_failed")})
  end

  def mq_connection
    @connection ||= MQConnection.new
  end

end
