class LocalFile
  def initialize(base_path)
    raise 'invalid params: base_path' if base_path.empty?

    @base_path = base_path
  end

  def filepath_and_timestamps
    @filepath_and_timestamps ||= local_filepaths.to_h do |name|
      [
        name,
        {
          actual_path: "#{@base_path}/#{name}",
          timestamp: File::Stat.new("#{@base_path}/#{name}").mtime,
        },
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
  pp LocalFile.new(
    '../local_base/test',
  ).filepath_and_timestamps
end
