require 'dm-core'
require 'simpledb/utils'
require 'simpledb/chunked_string'
require 'simpledb/table'

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
      result = transform_hash(fields) {|hash, property|
        hash[property.name.to_s] = self[property.field, property]
      }
      result
    end

    def storage_name
      simpledb_attributes[STORAGE_NAME_KEY].first
    end

    def [](attribute, type)
      values = Array(simpledb_attributes[attribute])
      coerce_to(values, type)
    end

    def coerce_to(values, type_or_property)
      case type_or_property
      when DataMapper::Property
        coerce_to_property(values, type_or_property)
      when Class
        coerce_to_type(values, type_or_property)
      else raise "Should never get here"
      end
    end

    def coerce_to_property(value, property)
      property.typecast(coerce_to_type(value, property.type))
    end

    def coerce_to_type(values, type)
      case 
      when type <= String
        case values.size
        when 0
          nil
        when 1
          values.first
        else
          ChunkedString.new(values)
        end
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
        !Array(value).empty?
      }
      attrs_to_update = updates.inject({}){|h, (k,v)| h[k] = v; h}
      attrs_to_update.merge!(
        'simpledb_type' => [Table.new(resource.model).simpledb_type])
      attrs_to_delete = deletes.inject({}){|h, (k,v)| h[k] = v; h}.keys
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
        elsif value.class == Object # This is for SdbArray
          result[key] = value.to_ary
        elsif primitive_value_of(value.class) <= Array
          result[key] = value
        elsif value.nil?
          result[key] = nil
        else
          result[key] = [value.to_s]
        end
      end
      # Stringify keys
      transform_hash(attrs) {|h, k, v| h[k.to_s] = v}
    end

    def primitive_value_of(type)
      if type < DataMapper::Type
        type.primitive
      else
        type
      end
    end

    # Creates an item name for a resource
    def item_name_for_resource(resource)
      table = Table.new(resource.model)
      sdb_type = table.simpledb_type
      
      item_name = "#{sdb_type}+"
      keys = table.keys_for_model
      item_name += keys.map do |property|
        property.get(resource)
      end.join('-')
      
      Digest::SHA1.hexdigest(item_name)
    end
    
  end

  class RecordV0 < Record
    version "00.00.00"

    def coerce_to_type(values, type)
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
