source 'https://rubygems.org'

# Specify your gem's dependencies in fluent-plugin-kafka.gemspec
gemspec

if ENV['USE_RDKAFKA']
  gem 'rdkafka', ENV['RDKAFKA_VERSION_MIN_RANGE'], ENV['RDKAFKA_VERSION_MAX_RANGE']
  min_version = Gem::Version.new('0.16.0')
  min_range_version = Gem::Version.new(ENV['RDKAFKA_VERSION_MIN_RANGE'].split(' ').last)
  if min_range_version >= min_version
    gem 'aws-msk-iam-sasl-signer'
    gem 'json', '2.7.3' # override of 2.7.4 version
  end
end
