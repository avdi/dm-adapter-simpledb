require 'pathname'
require Pathname(__FILE__).dirname.expand_path + 'spec_helper'
require 'dm-migrations'

describe 'support migrations' do
  
  #TODO do this on different storage
  class Person
    include DataMapper::Resource
    
    property :id,         String, :key => true
    property :name,       String, :key => true
    property :age,        Integer
    property :wealth,     Float
    property :birthday,   Date
    property :created_at, DateTime
    
  end

#  test can't be run simultanious make it delete a throwawaable storage model
#  instead of the one used by all the tests 
#  it "should destroy model storage" do
#    ENV['destroy']='true'
#    @adapter.destroy_model_storage(repository(:default), Person)
#    @adapter.storage_exists?("missionaries").should == false
#    ENV['destroy']='false'
#    @adapter.create_model_storage(repository(:default), Person)
#    @adapter.storage_exists?("missionaries").should == true
#  end
  
  it "should create model storage" do
    pending "figure out where auto migrations went in 0.10"
    DataMapper.repository(:migration) do
      DataMapper.auto_migrate!
      @migration_adapter.storage_exists?(@domain).should == true
    end
  end
  
end

