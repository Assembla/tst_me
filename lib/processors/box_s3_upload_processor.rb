# -*- encoding: utf-8 -*-
require "s3_upload"

class BoxS3UploadProcessor < BaseProcessor

  def on_message(event)
    logger.debug("#{self.class.name} received: #{event.inspect}")

    begin
      upload_list = upload_list(event)
      unless upload_list.blank?
        upload_s3 = S3Upload.new(configatron.aws_s3.access_key_id, configatron.aws_s3.secret_access_key, event[:oauth_token])
        upload_s3.send_files(upload_list)
      end
    rescue RubyBox::AuthError => ex
      # enqueue token refresh event
      resubmit_event(event, metadata.routing_key)
    end
  end

  private

  def upload_list(event)
    file_list = []
    event_files = event[:files]
    if event_files
      event_files.each do |key, file_meta|
        file_list << {key: key, bucket: event[:bucket], id: file_meta[:external_id]}
      end
    end
    file_list
  end

  def resubmit_event(event, routing_key)
    event[:errors] = "refresh_token"
    MQConnection.instance.publish(event, {routing_key: routing_key.gsub("requested", "oauth_failed")})
  end

end
