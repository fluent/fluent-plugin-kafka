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
require 'openssl'

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

module RubyKafkaSSLParamsTestHelper
  # SASL parameters for each Kafka.new call of the ruby-kafka based plugins.
  # Without SASL, the plugins use the same call as for GSSAPI.
  SASL_PARAMS = {
    "no SASL" => {},
    "SASL/PLAIN" => {"username" => "user", "password" => "pass"},
    "SASL/SCRAM" => {"username" => "user", "password" => "pass", "scram_mechanism" => "sha256"},
    "SASL/GSSAPI" => {"principal" => "fluentd@EXAMPLE.COM", "keytab" => "/path/to/keytab"},
  }.freeze
  AWS_MSK_IAM_PARAMS = {
    "AWS MSK IAM" => {"sasl_aws_msk_iam_access_key_id" => "access-key-id",
                      "sasl_aws_msk_iam_secret_key_id" => "secret-key-id",
                      "sasl_aws_msk_iam_aws_region" => "us-east-1"},
  }.freeze

  def create_certificate(common_name, key)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = 1
    cert.subject = cert.issuer = OpenSSL::X509::Name.parse("/CN=#{common_name}")
    cert.public_key = key
    cert.not_before = Time.now
    cert.not_after = Time.now + 3600
    cert.sign(key, OpenSSL::Digest.new('SHA256'))
    cert
  end

  # Writes the SSL files to dir, and returns the SSL parameters for them and the
  # options that the plugins should pass to Kafka.new
  def ssl_params_with_files(dir)
    paths = {}
    %w[ca_cert client_cert client_cert_key].each do |name|
      paths[name] = File.join(dir, "#{name}.pem")
      File.write(paths[name], "#{name} PEM")
    end
    # The plugins read the certificates of the chain with OpenSSL
    chain = create_certificate('intermediate', OpenSSL::PKey::EC.generate('prime256v1')).to_pem
    paths["client_cert_chain"] = File.join(dir, "client_cert_chain.pem")
    File.write(paths["client_cert_chain"], chain)
    params = {
      "ssl_ca_cert" => paths["ca_cert"],
      "ssl_client_cert" => paths["client_cert"],
      "ssl_client_cert_key" => paths["client_cert_key"],
      "ssl_client_cert_key_password" => "key-password",
      "ssl_client_cert_chain" => paths["client_cert_chain"],
      "ssl_ca_certs_from_system" => "true",
      "ssl_verify_hostname" => "false",
    }
    expected = {
      ssl_ca_cert_file_path: [paths["ca_cert"]],
      ssl_client_cert: "client_cert PEM",
      ssl_client_cert_key: "client_cert_key PEM",
      ssl_client_cert_key_password: "key-password",
      ssl_client_cert_chain: chain,
      ssl_ca_certs_from_system: true,
      ssl_verify_hostname: false,
    }
    [params, expected]
  end

  def params_to_conf(params)
    params.map { |key, value| "#{key} #{value}\n" }.join
  end

  # Returns the options given to Kafka.new while the block runs. Kafka.new
  # raises KafkaNewCalled, so that the plugin doesn't connect to the brokers.
  def capture_kafka_new_options
    options = nil
    stub(Kafka).new { |**kwargs|
      options = kwargs
      raise KafkaNewCalled
    }
    yield
    options
  end

  class KafkaNewCalled < StandardError
  end
end
