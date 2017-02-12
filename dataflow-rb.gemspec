# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'dataflow/version'

Gem::Specification.new do |spec|
  spec.name          = 'dataflow-rb'
  spec.version       = Dataflow::VERSION
  spec.authors       = ['okoriko']
  spec.email         = ['eurico@phybbit.com']

  spec.summary       = %q{Helps building data and automation pipelines. It handles recomputing dependencies and parallel execution.}
  spec.description   = %q{Helps building data pipelines. It handles recomputing dependencies and parallel execution.}
  spec.homepage      = 'https://phybbit.com'

  # Prevent pushing this gem to RubyGems.org by setting 'allowed_push_host', or
  # delete this section to allow pushing this gem to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = 'git@github.com:Phybbit/dataflow-rb.git'
  else
    raise 'RubyGems 2.0 or newer is required to protect against public gem pushes.'
  end

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'byebug'
  spec.add_development_dependency 'pry-byebug'
  spec.add_development_dependency 'timecop'
  spec.add_development_dependency 'ruby-prof'
  spec.add_development_dependency 'dotenv'

  spec.add_dependency 'activesupport',    '>= 4.0.0'
  spec.add_dependency 'schema-inference', '~>1.2.1'
  spec.add_dependency 'parallel',         '~>1.10'
  spec.add_dependency 'mongoid',          '~>6.0'
  spec.add_dependency 'sequel',           '~>4.0'
  spec.add_dependency 'mysql2',           '~>0.4'
  spec.add_dependency 'pg',               '~>0.19'
  spec.add_dependency 'sequel_pg',        '~>1.6'
  spec.add_dependency 'msgpack',          '~>1.0'
  spec.add_dependency 'smarter_csv',      '1.1.0'
  spec.add_dependency 'timeliness',       '~>0.3'
  spec.add_dependency 'chronic',          '~>0.10'
end
