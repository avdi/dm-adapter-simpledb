require File.expand_path('unit_spec_helper', File.dirname(__FILE__))
require 'simpledb_adapter'

describe DataMapper::Adapters::SimpleDBAdapter do
  class Product
    include DataMapper::Resource

    property :id,    Serial
    property :name,  String
    property :stock, Integer
  end

  before :each do
    @sdb = stub("RightAWS::SdbInterface").as_null_object
    @log = stub("Log").as_null_object

    # Using Abstract adapter as a null DB
    DataMapper.setup(:default, 
      :adapter       => 'simpledb',
      :access_key    => "ACCESS_KEY",
      :secret_key    => "SECRET_KEY",
      :domain        => "DOMAIN",
      :logger        => @log,
      :sdb_interface => @sdb
      )
  end

  after :each do
    DataMapper::Repository.adapters.delete(:default)
  end
  
  describe "given a record" do
    before :each do
      @record = Product.new(:name => "War and Peace", :stock => 3)
    end
    
    it "should be able to save the record" do
      @sdb.should_receive(:put_attributes).with(
        anything,
        anything,
        hash_including(
          'simpledb_type' => ["products"], 
          'stock'         => 3, 
          'name'          => ["War and Peace"]))
      @record.save
    end
  end

  describe "given an existing record" do
    before :each do
      @sdb.stub(:select).
        and_return(:items => [
          {"HANDLE" => {
              'id'    => ['12345'], 
              'name'  => ['War and Peace'], 
              'stock' => ['3']}}
        ])
      @record = Product.first
    end
    
    it "should be able to update the record" do
      @record.stock = 5
      @sdb.should_receive(:put_attributes).with(
        anything,
        anything,
        hash_including('stock' => 5),
        :replace)
      @record.save
    end
  end

end
