require 'pathname'
require Pathname(__FILE__).dirname.expand_path + 'spec_helper'

class Person
  include DataMapper::Resource
  
  property :id,         String, :key => true
  property :name,       String, :key => true
  property :age,        Integer
  property :wealth,     Float
  property :birthday,   Date
  property :created_at, DateTime
  
  belongs_to :company
end

#TODO write some tests with company or drop this
class Company
  include DataMapper::Resource
  
  property :id,   String, :key => true
  property :name, String, :key => true
  
  has n, :people
end

describe 'with multiple records saved' do
  before(:all) do
    @person_attrs = { :id => "person-#{Time.now.to_f.to_s}", :name => 'Jeremy Boles', :age  => 25, :wealth => 25.00, :birthday => Date.today }
    @jeremy   = Person.create(@person_attrs.merge(:id => Time.now.to_f.to_s, :name => "Jeremy Boles", :age => 25))
    @danielle = Person.create(@person_attrs.merge(:id => Time.now.to_f.to_s, :name => "Danille Boles", :age => 26))
    @keegan   = Person.create(@person_attrs.merge(:id => Time.now.to_f.to_s, :name => "Keegan Jones", :age => 20))
    @adapter.wait_for_consistency
  end
  
  after(:all) do
    @jeremy.destroy
    @danielle.destroy
    @keegan.destroy
  end
  
  it 'should get all records' do
    Person.all.length.should == 3
  end
  
  it 'should get records by eql matcher' do
    people = Person.all(:age => 25)
    people.length.should == 1
  end

  it 'should get record by eql matcher' do
    person = Person.first(:conditions => {:age => 25})
    person.should_not be_nil
  end
  
  it 'should get records by not matcher' do
    people = Person.all(:age.not => 25)
    people.should have(2).entries
  end

  it 'should get record by not matcher' do
    person = Person.first(:age.not => 25)
    person.should_not be_nil
  end
  
  it 'should get records by gt matcher' do
    people = Person.all(:age.gt => 25)
    people.length.should == 1
  end
  
  it 'should get records by gte matcher' do
    people = Person.all(:age.gte => 25)
    people.length.should == 2
  end
  
  it 'should get records by lt matcher' do
    people = Person.all(:age.lt => 25)
    people.length.should == 1
  end
  
  it 'should get records by lte matcher' do
    people = Person.all(:age.lte => 25)
    people.length.should == 2
  end

  it 'should get record by lte matcher' do
    person = Person.first(:age.lte => 25)
    person.should_not be_nil
  end
  
  it 'should get records with multiple matchers' do
    people = Person.all(:birthday => Date.today, :age.lte => 25)
    people.length.should == 2
  end

  it 'should get records by the like matcher' do
    people = Person.all(:name.like => 'Jeremy%')
    people.should == [@jeremy]
  end
  
  it 'should get records by the IN matcher' do
    people = Person.all(:id => [@jeremy.id, @danielle.id])
    people.should include(@jeremy)
    people.should include(@danielle)
    people.should_not include(@keegan)
  end
  it "should get no records if IN array is empty" do
    people = Person.all(:id => [])
    people.should be_empty
  end
end
