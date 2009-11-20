require 'pathname'
require Pathname(__FILE__).dirname.expand_path + 'spec_helper'

describe 'associations' do
  it 'should work with belongs_to associations'
  it 'should work with has n associations'
end

describe 'STI' do
  it 'should override default type'
  it 'should load descendents on parent.all' 
  it 'should raise an error if you have a column named couchdb_type'
end


