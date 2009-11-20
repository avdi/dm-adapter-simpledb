require 'simpledb/utils'

module SimpleDB
  class Record
    include Utils

    METADATA_KEY     = "__dm_metadata"
    STORAGE_NAME_KEY = "simpledb_type"
    META_KEYS        = [METADATA_KEY, STORAGE_NAME_KEY]

    def self.from_simpledb_hash(hash)
      new(hash)
    end

    def initialize(simpledb_hash)
      @key  = simpledb_hash.keys.first
      @data = simpledb_hash.values.first
    end

    def version
      @data.fetch(METADATA_KEY){[]}.grep(/v\d\d\.\d\d\.\d\d/) do 
        |version_stamp|
        return version_stamp[1..-1]
      end
      return "00.00.00"
    end

    def to_resource_hash
      app_data.inject({}){|result, (key,values)|
        result[key] = values.first
        result
      }
    end

    def storage_name
      data[STORAGE_NAME_KEY].first
    end

    private

    attr_reader :data

    def app_data
      result = map_hash_to_hash(data) {|k,v| 
        throw :skip if META_KEYS.include?(k)
        [k, v]
      }
    end
  end
end
