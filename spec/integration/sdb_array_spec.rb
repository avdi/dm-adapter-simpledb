require 'pathname'
require Pathname(__FILE__).dirname.expand_path + 'spec_helper'
require Pathname(__FILE__).dirname.expand_path + '../lib/simpledb_adapter/sdb_array'
require 'spec/autorun'

describe 'with multiple records saved' do
  
  class Hobbyist
    include DataMapper::Resource
    property :name,       String, :key => true
    property :hobbies,     SdbArray
  end
  
  before(:each) do
    @jeremy   = Hobbyist.create(:name => "Jeremy Boles",  :hobbies => ["biking", "diving", "chess"])
    @danielle = Hobbyist.create(:name => "Danille Boles", :hobbies => ["swimming", "diving"])
    @keegan   = Hobbyist.create(:name => "Keegan Jones",  :hobbies => ["painting"])
    @adapter.wait_for_consistency
  end
  
  after(:each) do
    @jeremy.destroy
    @danielle.destroy
    @keegan.destroy
  end
  
  it 'should store hobbies as array' do
    person = Hobbyist.first(:name => 'Jeremy Boles')
    person.hobbies.sort.should == ["biking", "diving", "chess"].sort
  end
  
  it 'should allow updates to array' do
    person = Hobbyist.first(:name => 'Jeremy Boles')
    person.hobbies = ["lego"]
    person.save
    @adapter.wait_for_consistency
    lego_person = Hobbyist.first(:name => 'Jeremy Boles')
    lego_person.hobbies.should == "lego"
  end
  
  it 'should allow deletion of array' do
    person = Hobbyist.first(:name => 'Jeremy Boles')
    person.hobbies = []
    person.save
    @adapter.wait_for_consistency
    lego_person = Hobbyist.first(:name => 'Jeremy Boles')
    lego_person.hobbies.should == nil
  end
  
  it 'should find all records with diving hobby' do
    people = Hobbyist.all(:hobbies => 'diving')
    people.should     include(@jeremy)
    people.should     include(@danielle)
    people.should_not include(@keegan)
  end
  
  it 'should find all records with painting hobby' do
    people = Hobbyist.all(:hobbies => ['painting'])
    people.should_not include(@jeremy)
    people.should_not include(@danielle)
    people.should     include(@keegan)
  end
  
  it "should find all records with like operator" do
    people = Hobbyist.all(:hobbies.like => 'pa%')
    people.should_not include(@jeremy)
    people.should_not include(@danielle)
    people.should     include(@keegan)
  end

end
