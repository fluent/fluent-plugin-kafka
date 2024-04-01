require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/testtask'

Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'

  # TODO: include the waterdrop tests after fixing up CI
  test.test_files = FileList['test/**/test_*.rb']
    .exclude('test/**/test_out_waterdrop.rb')
  test.verbose = true
end

task :default => [:build]
