require 'aws-sdk-s3'

class S3Object
  EXCLUSION_PATH = ['/', ''].freeze

  def initialize(client, bucket_name, base_path = '')
    @client = client
    @bucket_name = bucket_name
    @base_path = base_path

    if !@base_path.empty? && @base_path[-1] != '/'
      @base_path += '/'
    elsif @base_path == '/'
      @base_path = ''
    end
  end

  def filepath_and_timestamps
    return @filepath_and_timestamps if @filepath_and_timestamps

    create

    @filepath_and_timestamps
  end

  private

  def create
    @filepath_and_timestamps = {}

    s3_objects.each do |object|
      filepath = key_path(object.key)

      next if EXCLUSION_PATH.include?(filepath)

      dirpaths(filepath).each do |dirpath|
        store(dirpath, object) unless @filepath_and_timestamps.key?(dirpath)
      end

      store(filepath, object)
    end
  end

  # 戻り値: Array[Class: Aws::S3::Types::Object]
  # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Types/Object.html
  # ファイルパス(Object#key)順にソートされた状態で返却される
  def s3_objects
    # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Client.html#list_objects_v2-instance_method
    params = {
      bucket: @bucket_name,
      prefix: @base_path,
    }
    @client.list_objects_v2(params)[:contents]
  end

  def key_path(path)
    path[@base_path.length..]
  end

  def dirpaths(filepath)
    dirpath = ''

    filepath.split('/')[0...-1].map do |dir|
      dirpath += "#{dir}/"
    end
  end

  def store(key, object)
    @filepath_and_timestamps[key] = {
      actual_path: object.key,
      timestamp: object.last_modified,
    }
  end
end

if __FILE__ == $PROGRAM_NAME
  ENV['AWS_ACCESS_KEY_ID'] = ''
  ENV['AWS_SECRET_ACCESS_KEY'] = ''
  ENV['AWS_DEFAULT_REGION'] = 'ap-northeast-1'

  pp S3Object.new(
    Aws::S3::Client.new,
    's3-sync-target',
    '',
  ).filepath_and_timestamps
end
