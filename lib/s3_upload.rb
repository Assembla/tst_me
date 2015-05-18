# -*- encoding : utf-8 -*-

class S3Upload
  attr_reader :s3, :size_for_multipart, :part_size, :logger
  attr_accessor :box_token
  # access_key, secret_key = "amazon_access_key", "amazon_secret_key"
  def initialize(access_key, secret_key, settings = {})
    @logger             = settings[:logger] || Logger.new(STDOUT)
    @s3 = RightAws::S3Interface.new(access_key, secret_key, logger: @logger)
    @size_for_multipart = (settings[:size_for_multipart] || 100) * 1024 * 1024 # megabytes
    @part_size          = (settings[:part_size]          || 50)  * 1024 * 1024 # megabytes
  end

  # send files to S3
  # params = [{:key => key, :bucket => bucket, :id => box_file_id}, .., {...}]
  # returns list of successfully uploaded files and list of failed to upload files with related errors
  def send_files(params)
    failed_list = []
    success_list = []
    params.each do |param|
      process_file(failed_list, success_list, param)
    end
    logger.info('Done.')
    return success_list, failed_list
  end

  private

  def process_file(failed_list, success_list, param)
    begin
      box_file = client.file_by_id(param[:box_id])
      file_node_id = param[:id]
      index_file = nil
      path = nil
      if param[:index]
        path = tmp_path
        index_file = File.new(path, 'wb+') if path
      end
      store_object(box_file, index_file, param[:key], param[:bucket])
      publish_index_event({id: file_node_id, path: path}) if param[:index]
      success_list << file_node_id
    rescue RubyBox::AuthError => ex
      invalidate_box_credentials
      logger.info("Box Auth failed, raising exception to submit event for refresh token")
      logger.info(ex.message)
      logger.error(ex.backtrace.join("\n"))
      # raising the auth error exception in order to send the refresh token event from BoxS3UploadProcessor
      raise ex
    rescue => ex
      logger.error("failed to upload box file: #{param.inspect}")
      logger.error(ex.message)
      logger.error(ex.backtrace.join("\n"))
      failed_list << {id: file_node_id, error: ex.message}
    end
  end

  # store object to S3
  # will use multipart API if file size is > than size_for_multipart
  def store_object(box_file, index_file, object_key, bucket)
    logger.info("uploading: #{bucket}/#{object_key}")
    is_multipart = box_file.size > @size_for_multipart
    io = box_file.stream
    io = save_copy(index_file, io) if index_file

    options = {
      key: object_key,
      bucket: bucket,
      data: io
    }.tap do |memo|
      memo.update(part_size: @part_size) if is_multipart
    end

    method = is_multipart ? :store_object_multipart : :store_object
    @s3.send(method, options)
    index_file.close if index_file
  end

  def save_copy(index_file, io)
    while (chunk = io.read(@part_size)) do
      index_file.write chunk
    end
    index_file.rewind
    index_file
  end

  def tmp_path
    tmp_path = nil
    Dir::Tmpname.create('RackMultipart') { |p| tmp_path = p }
    tmp_path
  end

  def publish_index_event(event)
    logger.info("send indexing event: #{event}")
    mq_connection.publish(event, {routing_key: "breakout.file_connector_node.upload.#{configatron.hostname}"})
  end

  def invalidate_box_credentials
    @client = nil
    @box_session = nil
  end

  def mq_connection
    @connection ||= MQConnection.new
  end

  def box_session
    @box_session ||= RubyBox::Session.new({client_id: configatron.box_connector.client_id,
                                           client_secret: configatron.box_connector.client_secret, access_token: box_token})
  end

  def client
    @client ||= RubyBox::Client.new(box_session)
  end

end

