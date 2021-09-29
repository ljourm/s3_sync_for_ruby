require 'aws-sdk-s3'
require './s3_object'
require './local_file'

class S3ToLocalSync
  def initialize(bucket_name, dry_run: false, force: false, file_mode: 0o666)
    @bucket_name = bucket_name
    @dry_run = dry_run
    @force = force
    @file_mode = file_mode

    logger.info('dry run is enabled') if @dry_run
    logger.info('force is enabled')   if @force
  end

  def sync(s3_path, local_path) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    logger.info('start')
    logger.info("s3_path: #{s3_path}, local_path: #{local_path}")

    s3_files = s3_filepath_and_timestamps(s3_path)
    local_files = local_filepath_and_timestamps(local_path)

    s3_files.each do |filepath, info|
      if local_files.key?(filepath)
        # ローカルに存在する場合
        local_file = local_files[filepath]
        sync_exists_file(filepath, info[:actual_path],
                         info[:timestamp],
                         local_file[:actual_path],
                         local_file[:timestamp])
      else
        # ローカルに存在しない場合
        sync_unexists_file(filepath, info[:actual_path], "#{local_path}/#{filepath}")
      end
    end

    (local_files.keys - s3_files.keys).sort.reverse_each do |key|
      delete_file_or_dir(local_files[key][:actual_path])
    end

    logger.info('succeeded')
  end

  private

  def s3_client
    # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Client.html
    @s3_client ||= Aws::S3::Client.new
  end

  def s3_filepath_and_timestamps(base_path)
    S3Object.new(s3_client, @bucket_name, base_path)
            .filepath_and_timestamps
  end

  def local_filepath_and_timestamps(base_path)
    LocalFile.new(base_path)
             .filepath_and_timestamps
  end

  def sync_exists_file(filepath, s3_actual_path, s3_timestamp, local_actual_path, local_timestamp)
    return if dir?(filepath)
    return if !@force && s3_timestamp <= local_timestamp

    s3_to_local(s3_actual_path, local_actual_path)
  end

  def sync_unexists_file(filepath, s3_actual_path, local_actual_path)
    if dir?(filepath)
      make_dir(local_actual_path)
    else
      s3_to_local(s3_actual_path, local_actual_path)
    end
  end

  def dir?(filepath)
    filepath[-1] == '/'
  end

  def s3_to_local(s3_path, local_path)
    unless @dry_run
      # see: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/S3/Client.html#get_object-instance_method
      s3_client.get_object(bucket: @bucket_name, key: s3_path, response_target: local_path)
      FileUtils.chmod(@file_mode, local_path)
    end

    logger.info("download: #{s3_path} to #{local_path}")
  end

  def make_dir(path)
    Dir.mkdir(path, @file_mode) unless @dry_run

    logger.info("make dir: #{path}")
  end

  def delete_file_or_dir(path)
    if dir?(path)
      delete_dir(path)
    else
      delete_file(path)
    end
  end

  def delete_dir(path)
    Dir.delete(path) unless @dry_run

    logger.info("delete dir: #{path}")
  end

  def delete_file(path)
    File.delete(path) unless @dry_run

    logger.info("delete file: #{path}")
  end

  def logger
    @logger ||= Logger.new($stdout, 'INFO')
  end
end

if __FILE__ == $PROGRAM_NAME
  ENV['AWS_ACCESS_KEY_ID'] = ''
  ENV['AWS_SECRET_ACCESS_KEY'] = ''
  ENV['AWS_DEFAULT_REGION'] = 'ap-northeast-1'

  bucket_name = 's3-sync-target'

  runner = S3ToLocalSync.new(bucket_name, dry_run: false, force: false)
  runner.sync('', '../local_base/test') # NOTE: 第2引数のパスは既にローカルに存在している必要がある。
end
