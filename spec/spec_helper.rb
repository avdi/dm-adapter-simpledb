require 'pathname'
require Pathname(__FILE__).dirname.parent.expand_path + 'lib/simpledb_adapter'
require 'ruby-debug'
require 'logger'
require 'fileutils'

DOMAIN_FILE_MESSAGE = <<END
!!! ATTENTION !!!
In order to operate, these specs need a throwaway SimpleDB domain to operate
in. This domain WILL BE DELETED BEFORE EVERY SUITE IS RUN. In order to 
avoid unexpected data loss, you are required to manually configure the 
throwaway domain. In order to configure the domain, create a file in the
project root directory named THROW_AWAY_SDB_DOMAIN. It's contents should be 
the name of the SimpleDB domain to use for tests. E.g.

    $ echo dm_simpledb_adapter_test > THROW_AWAY_SDB_DOMAIN

END

Spec::Runner.configure do |config|
  access_key  = ENV['AMAZON_ACCESS_KEY_ID']
  secret_key  = ENV['AMAZON_SECRET_ACCESS_KEY']
  domain_file = File.expand_path('../THROW_AWAY_SDB_DOMAIN', File.dirname(__FILE__))
  test_domain = if File.exist?(domain_file)
                  File.read(domain_file).strip
                else
                  warn DOMAIN_FILE_MESSAGE
                  exit 1
                end

  #For those that don't like to mess up their ENV
  if access_key==nil && secret_key==nil
    lines = File.readlines(File.join(File.dirname(__FILE__),'..','aws_config'))
    access_key = lines[0].strip
    secret_key = lines[1].strip
  end

  config.before :all do
    FileUtils.mkdir_p('log') unless File.exists?('log')
    log_file = "log/dm-sdb.log"
    FileUtils.touch(log_file)
    log = Logger.new(log_file)

    @domain = test_domain

    DataMapper.logger.set_log(log_file, :debug)
    @adapter = DataMapper.setup(:default, {
        :adapter => 'simpledb',
        :access_key => access_key,
        :secret_key => secret_key,
        :domain => test_domain,
        :logger => log,
        :wait_for_consistency => :manual
      })
  end

  config.before :suite do
    @sdb ||= RightAws::SdbInterface.new(
      access_key, secret_key, :domain => test_domain)
    @sdb.delete_domain(test_domain)
    @sdb.create_domain(test_domain)
  end
end
