# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.authors       = ["Hidemasa Togashi", "Masahiro Nakagawa"]
  gem.email         = ["togachiro@gmail.com", "repeatedly@gmail.com"]
  gem.description   = %q{Fluentd plugin for Apache Kafka > 0.8}
  gem.summary       = %q{Fluentd plugin for Apache Kafka > 0.8}
  gem.homepage      = "https://github.com/fluent/fluent-plugin-kafka"
  gem.license       = "Apache-2.0"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "fluent-plugin-kafka"
  gem.require_paths = ["lib"]
  gem.version       = '0.19.3'
  gem.required_ruby_version = ">= 2.1.0"

  gem.add_dependency "fluentd", [">= 0.10.58", "< 2"]
  gem.add_dependency 'ltsv'
  gem.add_dependency 'ruby-kafka', '>= 1.5.0', '< 2'

  if ENV['USE_RDKAFKA']
    gem.add_dependency 'rdkafka', [ENV['RDKAFKA_VERSION_MIN_RANGE'], ENV['RDKAFKA_VERSION_MAX_RANGE']]
    if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.0')
      gem.add_dependency 'aws-msk-iam-sasl-signer', '~> 0.1.1'
    end
  end

  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "test-unit", ">= 3.0.8"
  gem.add_development_dependency "test-unit-rr", "~> 1.0"
  gem.add_development_dependency "webrick"
  gem.add_development_dependency "digest-murmurhash"
end
