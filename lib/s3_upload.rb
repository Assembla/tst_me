# -*- encoding : utf-8 -*-
# require "right_aws"

class S3Upload
  attr_reader :s3, :bucket, :box_token, :link_expires_in, :size_for_multipart, :part_size, :logger

  MAX_BUCKET_NUMBER = 100

  # access_key, secret_key = "amazon_access_key", "amazon_secret_key"
  def initialize(access_key, secret_key, box_token, settings = {})
    @logger             = settings[:logger] || Logger.new(STDOUT)
    @s3 = RightAws::S3Interface.new(access_key, secret_key, logger: @logger)
    @link_expires_in    = (settings[:link_expires_in]    || 24)  *   60 *   60 # hours
    @size_for_multipart = (settings[:size_for_multipart] || 100) * 1024 * 1024 # megabytes
    @part_size          = (settings[:part_size]          || 50)  * 1024 * 1024 # megabytes
    @bucket_name        = settings[:bucket_name]
    @box_token        = box_token
  end

  # send files to S3
  # params = [{:key => key, :bucket => bucket, :id => box_file_id}, .., {...}]
  # returns true on success
  def send_files(params)
    puts "zzzz"
    params.each { |param| store_object(param) }
    true
  end

  private

  # Find or create a bucket for S3 backup
  # accepts parameter bucket as S3 bucket name
  # if such bucket already exists, appends index (0,1,2 etc) to bucket name until its unique
  # returns string bucket_name
  def find_or_create_bucket(bucket_name)
    if !bucket_object_exists? && buckets_limit_reached?
      raise RightAws::AwsError.new("TooManyBuckets: You have attempted to create more buckets than allowed")
    end

    downcased_bucket_name = bucket_name.downcase

    if @bucket == bucket_name
      reset_counter_and_bucket_name_to(bucket_name)
    elsif @bucket == downcased_bucket_name
      reset_counter_and_bucket_name_to(downcased_bucket_name)
    elsif bucket_exists?(bucket_name)
      reset_counter_and_bucket_name_to(bucket_name)
    elsif bucket_exists?(downcased_bucket_name) || create_bucket(downcased_bucket_name)
      reset_counter_and_bucket_name_to(downcased_bucket_name)
    else
      raise RightAws::AwsError.new("BucketCreationError: Can not create bucket for unknown reason")
    end
  rescue RightAws::AwsError
    if $!.message.start_with?("InvalidBucketName") || $!.message.start_with?("BucketAlreadyExists")
      # add some uniqueness to bucket name
      idx = bucket_name[/-\d+$/].to_s.tr('-','').to_i + 1
      find_or_create_bucket(bucket_name.sub(/-\d+$/,'') + '-' + idx.to_s)
    else
      @buckets_limit_reached = true if $!.message.start_with?("TooManyBuckets")
      raise
    end
  end

  def create_bucket(bucket_name)
    s3.create_bucket(bucket_name)
    @bucket_object_exists = true
  end

  # store object to S3
  # will use multipart API if file size is > than size_for_multipart
  def store_object(params)
    if params[:bucket] && params[:bucket] != @bucket_name
      bucket = find_or_create_bucket(params[:bucket])
    else
      bucket = bucket_object_exists? ? @bucket_name : find_or_create_bucket(@bucket_name)
    end

    if params[:data]
      @s3.store_object(key: params[:key],
                       bucket: bucket,
                       data: params[:data])
    else
      box_file = client.file_by_id(params[:id])
      is_multipart = box_file.size > @size_for_multipart
      open(box_file.download_url) do |io|
        options = {
            key: params[:key],
            bucket: bucket,
            data: io
        }.tap do |memo|
          memo.update(part_size: @part_size) if is_multipart
        end

        method = is_multipart ? :store_object_multipart : :store_object
        @s3.send(method, options)
      end
    end
  end

  def reset_counter_and_bucket_name_to(bucket_name)
    @counter = 0
    @bucket = bucket_name
  end

  def bucket_exists?(bucket_name)
    s3.list_bucket(bucket_name)
    @bucket_object_exists = true
  rescue RightAws::AwsError => e
    if e.message.start_with?("NoSuchBucket")
      false
    elsif e.message.start_with?("AccessDenied")
      # Imitating s3 answer
      raise RightAws::AwsError.new("BucketAlreadyExists: Bucket already exists")
    else
      raise
    end
  end

  def bucket_object_exists?
    @bucket_object_exists
  end

  def buckets_limit_reached?
    @buckets_limit_reached
  end

  def init_bucket
    @bucket_name = find_or_create_bucket(@bucket_name)
  end

  def box_session
    @box_session ||= RubyBox::Session.new({client_id: configatron.box_connector.client_id,
                                           client_secret: configatron.box_connector.client_secret, access_token: box_token})
  end

  def client
    @client ||= RubyBox::Client.new(box_session)
  end

end

