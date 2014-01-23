# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.authors       = ["htgc"]
  gem.email         = ["togachiro@gmail.com"]
  gem.description   = %q{Fluentd plugin for Apache Kafka > 0.8}
  gem.summary       = %q{Fluentd plugin for Apache Kafka > 0.8}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "fluent-plugin-kafka-poseidon"
  gem.require_paths = ["lib"]
  gem.version = '0.0.1'
  gem.add_dependency 'fluentd'
  gem.add_dependency 'poseidon'
end
