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

    def self.from_resource_hash(hash, resource_type)
      versions.fetch(CURRENT_VERSION) do
        raise "Unknown data version for: #{hash.inspect}"
      end.new(hash, :source => :resource, :type => resource_type)
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
    alias_method :writable_attributes, :simpledb_attributes

    def initialize(hash, options={})
      if options[:source] == :resource
        attrs_to_update, attrs_to_delete = prepare_attributes(hash)
        @simpledb_attributes  = attrs_to_update
        @deletable_attributes = attrs_to_delete
      else
        @item_name = hash.keys.first
        @simpledb_attributes  = hash.values.first
        @deletable_attributes = []
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

    def prepare_attributes(attributes)
      attributes = attributes.to_a.map {|a| [a.first.name.to_s, a.last]}.to_hash
      attributes = adjust_to_sdb_attributes(attributes)
      updates, deletes = attributes.partition{|name,value|
        !value.nil? && !(value.respond_to?(:to_ary) && value.to_ary.empty?)
      }
      attrs_to_update = Hash[updates]
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
          nil
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
