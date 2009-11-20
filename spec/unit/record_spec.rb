require File.expand_path('unit_spec_helper', File.dirname(__FILE__))
require 'simpledb/record'
describe SimpleDB::Record do
  context "given a record with no version info" do
    before :each do
      @it = SimpleDB::Record.from_simpledb_hash(
        {"KEY" => {
            "foo"           => ["123"],
            "simpledb_type" => ["thingies"]
          }
        })
    end

    it "should identify the record as version 0" do
      @it.version.should == "00.00.00"
    end

    it "should be able to convert the record to a DM-friendly hash" do
      @it.to_resource_hash.should == {
        "foo"           => "123",
      }
    end

    it "should be able to extract the storage name" do
      @it.storage_name.should == "thingies"
    end
  end

  context "given a version 1 record" do
    before :each do
      @it = SimpleDB::Record.from_simpledb_hash(
        {"KEY" => {
            "__dm_metadata" => ["v01.00.00"],
            "bar"           => ["456"],
            "simpledb_type" => ["mystuff"]
          }
        })
    end

    it "should identify the record as version 0" do
      @it.version.should == "01.00.00"
    end

    it "should be able to convert the record to a DM-friendly hash" do
      @it.to_resource_hash.should == {
        "bar" => "456"
      }
    end

    it "should be able to extract the storage name" do
      @it.storage_name.should == "mystuff"
    end
  end
end
