require 'aws-sdk-s3'
require './s3_object'
require './local_file'

class S3ToLocalSync
  def initialize(bucket_name, dry_run: false, file_mode: 0666)
    @bucket_name = bucket_name
    @dry_run = dry_run
    @file_mode = file_mode

    logger.info('dry run is enabled') if @dry_run
  end

  def sync(s3_path, local_path)
    s3_files = s3_filepath_and_timestamps(s3_path)
    local_files = local_filepath_and_timestamps(local_path)

    s3_files.each do |filepath, timestamp|
      if local_files.key?(filepath)
        # ローカルに存在する場合
        unless dir?(filepath)
          # ファイルの場合
          if timestamp > local_files[filepath]
            # S3がローカルより新しい場合
            s3_to_local(filepath, local_path, filepath)
          end
        end
      else
        # ローカルに存在しない場合
        if dir?(filepath)
          # ディレクトリの場合
          make_dir(local_path, filepath)
        else
          # ファイルの場合
          s3_to_local(filepath, local_path, filepath)
        end
      end
    end

    (local_files.keys - s3_files.keys).sort.reverse.each do |filepath|
      if dir?(filepath)
        # ディレクトリの場合
        delete_dir(local_path, filepath)
      else
        # ファイルの場合
        delete_file(local_path, filepath)
      end
    end

    logger.info("succeeded s3_path: #{s3_path}, local_path: #{local_path}")
  end

private

  def s3_client
    # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Client.html
    @client ||= Aws::S3::Client.new
  end

  def s3_filepath_and_timestamps(base_path)
    S3Object.new(s3_client, @bucket_name, base_path)
             .filepath_and_timestamps
  end

  def local_filepath_and_timestamps(base_path)
    LocalFile.new(base_path)
             .filepath_and_timestamps
  end

  def dir?(filepath)
    filepath[-1] == '/'
  end

  def s3_to_local(s3_filepath, local_base_path, local_filepath)
    local_path = "#{local_base_path}/#{local_filepath}"
    logger.info("download: #{local_path}")

    return if @dry_run

    # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Client.html#get_object-instance_method
    object = @client.get_object(bucket: @bucket_name, key: s3_filepath)

    File.open(local_path, 'wb') do |file|
      file.write object.body.read
    end
    FileUtils.chmod(@file_mode, local_path)
  end

  def make_dir(base_path, filepath)
    path = "#{base_path}/#{filepath}"
    logger.info("make dir: #{path}")

    return if @dry_run

    Dir.mkdir(path, @file_mode)
  end

  def delete_dir(base_path, filepath)
    path = "#{base_path}/#{filepath}"
    logger.info("delete dir: #{path}")

    return if @dry_run

    Dir.delete(path)
  end

  def delete_file(base_path, filepath)
    path = "#{base_path}/#{filepath}"
    logger.info("delete file: #{path}")

    return if @dry_run

    File.delete(path)
  end

  def logger
    @logger ||= Logger.new($stdout, 'INFO')
  end
end

if __FILE__ == $PROGRAM_NAME
  ENV['AWS_ACCESS_KEY_ID'] = ''
  ENV['AWS_SECRET_ACCESS_KEY'] = ''
  ENV['AWS_DEFAULT_REGION'] = 'ap-northeast-1'

  bucket_name = 'bucket_name'

  runner = S3ToLocalSync.new(bucket_name, dry_run: false)
  pp runner.sync('s3_path', 'local_dir')
end
