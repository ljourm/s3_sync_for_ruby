class LocalFile
  def initialize(base_path)
    raise 'invalid params: base_path' if base_path.empty?

    @base_path = base_path
  end

  def filepath_and_timestamps
    @filepath_and_timestamp ||= local_filepaths.to_h do |name|
      [
        name,
        File::Stat.new("#{@base_path}/#{name}").mtime,
      ]
    end
  end

private

  def local_filepaths
    # delete_ifの処理: 現在のディレクトリを表す'.' (e.g. 'hoge/.') が含まれてしまうため削除している
    Dir.glob('**/*', File::FNM_DOTMATCH, base: @base_path)
       .delete_if { |name| name[-1] == '.' }
       .map { |name| local_dir_names.include?(name) ? "#{name}/" : name }
  end

  def local_dir_names
    # mapの処理: 返却値の末尾の'/' (e.g. 'hoge/') が不要なため削除している
    @local_dir_names ||= Dir.glob('**/', File::FNM_DOTMATCH, base: @base_path)
                            .map { |name| name.slice(0...-1) }
  end
end

if __FILE__ == $PROGRAM_NAME
  ENV['AWS_ACCESS_KEY_ID'] = ''
  ENV['AWS_SECRET_ACCESS_KEY'] = ''
  ENV['AWS_DEFAULT_REGION'] = 'ap-northeast-1'

  pp LocalFile.new(
    'local_dir',
  ).filepath_and_timestamps
  # => {"hoge"=>2021-09-28 15:57:32.3471809 +0000,
  #     "fuga/"=>2021-09-28 15:43:43.0868873 +0000,
  #     "fuga/piyo.txt"=>2021-09-28 16:20:14.0830284 +0000,
end
