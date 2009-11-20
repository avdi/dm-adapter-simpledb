module SimpleDB
  module Utils
    def map_hash_to_hash(original)
      Hash[
        *original.inject([]){|a, (key,value)|
          catch(:skip) do
            new_key, new_value = yield(key,value)
            a << new_key << new_value
          end
          a
        }
      ]
    end
  end
end
