Gem::Specification.new do |s|
  s.name          = 'logstash-output-mycassandra'
  s.version       = '0.1.0'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'cassandra for logstash plugin'
  s.description   = 'Write a longer description or delete this line.'
  s.homepage      = 'https://rubygems.org/profiles/brinjaul'
  s.authors       = ['brinajul']
  s.email         = '463232683@qq.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_development_dependency "logstash-devutils"
  s.add_runtime_dependency   "cassandra-driver", "~> 3.2", ">= 3.2.2"
end
