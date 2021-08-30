require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'test/unit'
require 'test/unit/rr'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'fluent/test'
unless ENV.has_key?('VERBOSE')
  nulllogger = Object.new
  nulllogger.instance_eval {|obj|
    def method_missing(method, *args)
    end
  }
  $log = nulllogger
end

require 'fluent/plugin/out_kafka'
require 'fluent/plugin/out_kafka_buffered'
require 'fluent/plugin/out_kafka2'
require 'fluent/plugin/in_kafka'
require 'fluent/plugin/in_kafka_group'

require "fluent/test/driver/output"

class Test::Unit::TestCase
end
