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
  # returns true on success
  def send_files(params)
    params.each { |param| store_object(param) }
    logger.info("Done.")
    true
  end

  private

  # store object to S3
  # will use multipart API if file size is > than size_for_multipart
  def store_object(params)
    #todo: make a map of erros by file_id
    box_file = client.file_by_id(params[:id])
    is_multipart = box_file.size > @size_for_multipart
    open(box_file.download_url) do |io|
      options = {
        key: params[:key],
        bucket: params[:bucket],
        data: io
      }.tap do |memo|
        memo.update(part_size: @part_size) if is_multipart
      end

      logger.info("uploading: #{params[:bucket]}/#{params[:key]}")
      method = is_multipart ? :store_object_multipart : :store_object
      @s3.send(method, options)
    end
  end

  def box_session
    @box_session ||= RubyBox::Session.new({client_id: configatron.box_connector.client_id,
                                           client_secret: configatron.box_connector.client_secret, access_token: box_token})
  end

  def client
    @client ||= RubyBox::Client.new(box_session)
  end

end

