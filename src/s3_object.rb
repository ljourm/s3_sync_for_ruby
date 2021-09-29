require 'aws-sdk-s3'

class S3Object
  def initialize(client, bucket_name, base_path = '')
    @client = client
    @bucket_name = bucket_name
    @base_path = base_path

    @base_path += '/' if !@base_path.empty? && @base_path[-1] != '/'
  end

  def filepath_and_timestamps
    return @filepath_and_timestamps if @filepath_and_timestamps

    @filepath_and_timestamps = s3_objects.to_h do |object|
      [
        object.key[@base_path.length..],
        object.last_modified,
      ]
    end.compact

    @filepath_and_timestamps.delete('/')
    @filepath_and_timestamps.delete('')

    @filepath_and_timestamps
  end

  private

  # 戻り値: Array[Class: Aws::S3::Types::Object]
  # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Types/Object.html
  # ファイルパス(Object#key)順にソートされた状態で返却される
  def s3_objects
    # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Client.html#list_objects_v2-instance_method
    params = {
      bucket: @bucket_name,
      encoding_type: 'url',
      prefix: @base_path,
    }
    @client.list_objects_v2(params)[:contents]
  end
end

if __FILE__ == $PROGRAM_NAME
  ENV['AWS_ACCESS_KEY_ID'] = ''
  ENV['AWS_SECRET_ACCESS_KEY'] = ''
  ENV['AWS_DEFAULT_REGION'] = 'ap-northeast-1'

  pp S3Object.new(
    Aws::S3::Client.new,
    'bucket_name',
    '',
  ).filepath_and_timestamps
  # => {"hoge"=>2021-08-12 22:45:43 UTC,
  #     "fuga/=>2021-08-24 06:32:10 UTC,
  #     "fuga/piyo.txt"=>2021-08-14 13:42:50 UTC}
end
