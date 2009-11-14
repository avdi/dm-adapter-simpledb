require 'dm-types'

module DataMapper
  module Types
    class SdbArray < DataMapper::Type
      primitive String
      length    65535
      lazy      true

      def self.load(value, property)
        value
      end

      def self.dump(value, property)
        value
      end

      def self.typecast(value, property)
        value
      end

    end 
  end
end
