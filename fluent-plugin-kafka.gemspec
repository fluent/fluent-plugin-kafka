# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.authors       = ["Hidemasa Togashi"]
  gem.email         = ["togachiro@gmail.com"]
  gem.description   = %q{Fluentd plugin for Apache Kafka > 0.8}
  gem.summary       = %q{Fluentd plugin for Apache Kafka > 0.8}
  gem.homepage      = "https://github.com/htgc/fluent-plugin-kafka"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "fluent-plugin-kafka"
  gem.require_paths = ["lib"]
  gem.version = '0.2.2'
  gem.add_dependency "fluentd", [">= 0.10.58", "< 2"]
  gem.add_dependency 'poseidon_cluster'
  gem.add_dependency 'ltsv'
  gem.add_dependency 'zookeeper'
  gem.add_dependency 'ruby-kafka', '~> 0.3.9'
  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "test-unit", ">= 3.0.8"
end
