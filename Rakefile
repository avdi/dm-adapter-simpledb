require 'rubygems'
require 'spec'
require 'spec/rake/spectask'
require 'pathname'
require "rake/gempackagetask"
load 'tasks/devver.rake'

ROOT = Pathname(__FILE__).dirname.expand_path
require ROOT + 'lib/simpledb_adapter'

task :default => [ :spec ]

desc 'Run specifications'
Spec::Rake::SpecTask.new(:spec) do |t|
  if File.exists?('spec/spec.opts')
    t.spec_opts << '--options' << 'spec/spec.opts'
  end
  t.spec_files = Pathname.glob((ROOT + 'spec/**/*_spec.rb').to_s)
 
  begin
    t.rcov = ENV.has_key?('NO_RCOV') ? ENV['NO_RCOV'] != 'true' : true
    t.rcov_opts << '--exclude' << 'spec'
    t.rcov_opts << '--text-summary'
    t.rcov_opts << '--sort' << 'coverage' << '--sort-reverse'
  rescue Exception
    # rcov not installed
  end
end

desc 'Run specifications without Rcov'
Spec::Rake::SpecTask.new(:spec_no_rcov) do |t|
  if File.exists?('spec/spec.opts')
    t.spec_opts << '--options' << 'spec/spec.opts'
  end
  t.spec_files = Pathname.glob((ROOT + 'spec/**/*_spec.rb').to_s)
end

def get_keys
  access_key = ENV['AMAZON_ACCESS_KEY_ID']
  secret_key = ENV['AMAZON_SECRET_ACCESS_KEY']
  
  #For those that don't like to mess up their ENV
  if access_key==nil && secret_key==nil
    lines = File.readlines(File.join(File.dirname(__FILE__),'aws_config'))
    access_key = lines[0].strip
    secret_key = lines[1].strip
  end

  [access_key, secret_key]
end

def setup_dm(access_key, secret_key)
  DataMapper.setup(:default, {
                     :adapter => 'simpledb',
                     :access_key => access_key,
                     :secret_key => secret_key,
                     :domain => 'dm_simpledb_adapter_test'
                   })
end

desc 'Create test storage model'
task :create_default_storage do

  access_key, secret_key = get_keys
  setup_dm(access_key, secret_key)

  class Person
    include DataMapper::Resource    
    property :id,         String, :key => true
  end
  
  Person.auto_migrate!
end

desc 'Destroy test storage model'
task :destroy_default_storage do

  access_key, secret_key = get_keys
  setup_dm(access_key, secret_key)

  class Person
    include DataMapper::Resource    
    property :id,         String, :key => true
  end
  
  @adapter = repository(:default).adapter
  ENV['destroy']='true'
  @adapter.destroy_model_storage(repository(:default), Person)
  ENV['destroy']='false'
end

spec = Gem::Specification.new do |s|
  s.name     = "dm-adapter-simpledb"
  s.version  = "0.9.3"
  s.date     = "2008-10-01"
  s.summary  = "A DatMapper adapter for SimpleDB"
  s.email    = "jeremy@jeremyboles.com"
  s.homepage = "http://github.com/jeremyboles/dm-adapter-simpledb"
  s.description = "A DataMapper adapter for Amazon's SimpleDB"
  s.has_rdoc = true
  s.authors  = ["Jeremy Boles"]
  s.files    = ["lib/simpledb_adapter.rb", 
    "lib/simpledb_adapter/sdb_array.rb",
		"README", 
		"Rakefile", 
		"dm-adapter-simpledb.gemspec"]
  s.add_dependency("rspec", ["> 0.0.0"])
  s.add_dependency("dm-core", ["> 0.0.0"])
  s.add_dependency("aws-sdb", ["> 0.0.0"])
end

Rake::GemPackageTask.new(spec) do |package|
  package.gem_spec = spec
end

desc "Create a gemspec file"
task :gemspec do
  File.open("dm-adapter-simpledb.gemspec", "w") do |file|
    file.puts spec.to_ruby
  end
end
