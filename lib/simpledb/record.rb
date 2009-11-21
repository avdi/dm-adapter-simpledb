require 'dm-core'
require 'simpledb/utils'
require 'simpledb/chunked_string'

# TODO
# * V1.1: Store type in __dm_metadata
# * V1.1: Store type as non-munged class name

module SimpleDB
  class Record
    include Utils

    METADATA_KEY     = "__dm_metadata"
    STORAGE_NAME_KEY = "simpledb_type"
    META_KEYS        = [METADATA_KEY, STORAGE_NAME_KEY]
    CURRENT_VERSION  = "01.01.00"

    def self.from_simpledb_hash(hash)
      data_version = data_version(simpledb_attributes(hash))
      versions.fetch(data_version) do
        raise "Unknown data version for: #{hash.inspect}"
      end.new(hash)
    end

    def self.from_resource(resource)
      versions.fetch(CURRENT_VERSION).new(resource)
    end

    def self.register(klass, version)
      versions[version] = klass
    end

    def self.versions
      @versions ||= {}
    end
    
    def self.version(version=nil)
      if version
        Record.register(self, version)
        @version = version
      else
        @version
      end
    end

    def self.data_version(simpledb_attributes)
      simpledb_attributes.fetch(METADATA_KEY){[]}.grep(/v\d\d\.\d\d\.\d\d/) do 
        |version_stamp|
        return version_stamp[1..-1]
      end
      return "00.00.00"
    end

    def self.simpledb_attributes(hash)
      hash.values.first
    end

    attr_reader :simpledb_attributes
    attr_reader :deletable_attributes
    attr_reader :item_name
    alias_method :writable_attributes, :simpledb_attributes

    def initialize(hash_or_resource)
      case hash_or_resource
      when DataMapper::Resource then
        attrs_to_update, attrs_to_delete = extract_attributes(hash_or_resource)
        @simpledb_attributes  = attrs_to_update
        @deletable_attributes = attrs_to_delete
        @item_name = item_name_for_resource(hash_or_resource)
      when Hash
        hash = hash_or_resource
        @item_name = hash.keys.first
        @simpledb_attributes  = hash.values.first
        @deletable_attributes = []
      else
        raise "Don't know how to initialize from #{hash_or_resource.inspect}"
      end
    end

    # Convert to a Hash suitable for initializing a Resource
    # 
    # @param [PropertySet] fields
    #   The fields to extract
    def to_resource_hash(fields)
      transform_hash(fields) {|hash, property|
        hash[property.name] = self[property.field, property.type]
      }
    end

    def storage_name
      simpledb_attributes[STORAGE_NAME_KEY].first
    end

    def [](attribute, type)
      values = Array(simpledb_attributes[attribute])
      coerce_to(values, type)
    end

    def coerce_to(values, type)
      case 
      when type <= String
        values.empty? ? nil : ChunkedString.new(values)
      when type <= Array, type <= DataMapper::Types::SdbArray
        values
      else
        values.first
      end
    end

    def version
      self.class.data_version(simpledb_attributes)
    end

    private

    def app_data
      transform_hash(simpledb_attributes) {|h,k,v| 
        h[k] = v unless META_KEYS.include?(k)
      }
    end

    def extract_attributes(resource)
      attributes = resource.attributes(:property)
      attributes = attributes.to_a.map {|a| [a.first.name.to_s, a.last]}.to_hash
      attributes = adjust_to_sdb_attributes(attributes)
      updates, deletes = attributes.partition{|name,value|
        !value.nil? && !(value.respond_to?(:to_ary) && value.to_ary.empty?)
      }
      attrs_to_update = Hash[updates]
      attrs_to_update.merge!('simpledb_type' => simpledb_type(resource.model))
      attrs_to_delete = Hash[deletes].keys
      [attrs_to_update, attrs_to_delete]
    end

    # hack for converting and storing strings longer than 1024 one thing to
    # note if you use string longer than 1019 chars you will loose the ability
    # to do full text matching on queries as the string can be broken at any
    # place during chunking
    def adjust_to_sdb_attributes(attrs)
      attrs = transform_hash(attrs) do |result, key, value|
        if primitive_value_of(value.class) <= String
          result[key] = ChunkedString.new(value).to_a
        elsif primitive_value_of(value.class) <= Array
          result[key] = value
        elsif value.nil?
          result[key] = nil
        else
          result[key] = [value.to_s]
        end
      end
      # Stringify keys and values
      transform_hash(attrs) {|h, k, v| h[k.to_s] = v}
    end

    def primitive_value_of(type)
      if type < DataMapper::Type
        type.primitive
      else
        type
      end
    end

    # Returns a string so we know what type of
    def simpledb_type(model)
      model.storage_name(repository_name(model))
    end

    # Creates an item name for a resource
    def item_name_for_resource(resource)
      sdb_type = simpledb_type(resource.model)
      
      item_name = "#{sdb_type}+"
      keys = keys_for_model(resource.model)
      item_name += keys.map do |property|
        property.get(resource)
      end.join('-')
      
      Digest::SHA1.hexdigest(item_name)
    end
    
    # Returns the keys for model sorted in alphabetical order
    def keys_for_model(model)
      model.key(repository_name(model)).sort {|a,b| a.name.to_s <=> b.name.to_s }
    end

    def repository_name(model)
      # TODO this should probably take into account the adapter
      model.repository.name
    end
      
  end

  class RecordV0 < Record
    version "00.00.00"

    def coerce_to(values, type)
      result = super(values, type)

      if result && type <= String
        replace_newline_placeholders(result)
      else
        result
      end
    end

    private

    def replace_newline_placeholders(value)
      value.gsub("[[[NEWLINE]]]", "\n")
    end
  end

  class RecordV1 < Record
    version "01.00.00"
  end

  class RecordV1_1 < Record
    version "01.01.00"
  end
end
