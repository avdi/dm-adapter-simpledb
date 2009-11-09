require 'rubygems'
require 'dm-core'
require 'digest/sha1'
require 'dm-aggregates'
require 'right_aws'
require 'uuidtools'

module DataMapper
  module Adapters
    class SimpleDBAdapter < AbstractAdapter

      #right_aws calls Array(your_string) on all string properties, which splits on \n and then sorts your string in a random order
      #This is a value that can be used to replace \n before storing a string and get it back coming out of SDB
      NEWLINE_REPLACE = "[[[NEWLINE]]]"

      def initialize(name, normalised_options)
        super
        @sdb_options = {}
        @sdb_options[:access_key] = options.fetch(:access_key) { 
          options[:user] 
        }
        @sdb_options[:secret_key] = options.fetch(:secret_key) { 
          options[:password] 
        }
        @sdb_options[:logger] = options.fetch(:logger) { DataMapper.logger }
        @sdb_options[:server] = options.fetch(:host) { 'sdb.amazonaws.com' }
        @sdb_options[:port]   = options[:port] || 443 # port may be set but nil
        @sdb_options[:domain] = options.fetch(:domain) { 
          options[:path].to_s.gsub(%r{(^/+)|(/+$)},"") # remove slashes
        }
        @wait_for_consistency = 
          normalised_options.fetch(:wait_for_consistency) { false }
      end

      def create(resources)
        created = 0
        time = Benchmark.realtime do
          resources.each do |resource|
            uuid = UUIDTools::UUID.timestamp_create
            initialize_serial(resource, uuid.to_i)
            item_name = item_name_for_resource(resource)
            sdb_type = simpledb_type(resource.model)
            attributes = resource.attributes.merge(:simpledb_type => sdb_type)
            attributes = adjust_to_sdb_attributes(attributes)
            #attributes.reject!{|key,value| value.nil? || value == '' || value #== []}
            sdb.put_attributes(domain, item_name, attributes)
            if @wait_for_consistency
              until consistent?(item_name)
                sleep 0.1
              end
            end
            created += 1
          end
        end
        DataMapper.logger.debug(format_log_entry("(#{created}) INSERT #{resources.inspect}", time))
        created
      end
      
      def delete(collection)
        deleted = 0
        time = Benchmark.realtime do
          collection.each do |resource|
            item_name = item_name_for_resource(resource)
            sdb.delete_attributes(domain, item_name)
            deleted += 1
          end
          raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(collection.query)
        end; DataMapper.logger.debug(format_log_entry("(#{deleted}) DELETE #{collection.query.conditions.inspect}", time))
        deleted
      end

      def read(query)
        sdb_type = simpledb_type(query.model)
        
        conditions, order, unsupported_conditions = 
          set_conditions_and_sort_order(query, sdb_type)
        results = get_results(query, conditions, order)
        #results = query.match_records(results)
        proto_resources = results.map do |result|
          name, attributes = *result.to_a.first
          proto_resource = query.fields.inject({}) do |proto_resource, property|
              value = attributes[property.field.to_s]
              if value != nil
                
                value = chunks_to_string(value) if property.type==String && value.size > 1
                #replace the newline placeholder with newlines
                if property.type==String
                  value = value.gsub(NEWLINE_REPLACE,"\n") if value.is_a?(String)
                  value[0] = value[0].gsub(NEWLINE_REPLACE,"\n") if value.is_a?(Array) && value[0]!=nil
                end
                if value.size > 1
                  value = value.map {|v| property.typecast(v) }
                else
                  value = property.typecast(value[0])
                end
              else
                value = property.typecast(nil)
              end
            proto_resource[property.name.to_s] = value
            proto_resource
          end
          proto_resource
        end
        proto_resources
      end
      
      def update(attributes, collection)
        updated = 0
        time = Benchmark.realtime do
          attributes = attributes.to_a.map {|a| [a.first.name.to_s, a.last]}.to_hash
          attributes = adjust_to_sdb_attributes(attributes)
          collection.each do |resource|
            item_name = item_name_for_resource(resource)
            sdb.put_attributes(domain, item_name, attributes, true)
            updated += 1
          end
          raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(collection.query)
        end
        DataMapper.logger.debug(format_log_entry("UPDATE #{collection.query.conditions.inspect} (#{updated} times)", time))
        updated
      end
      
      def query(query_call, query_limit = 999999999)
        select(query_call, query_limit).collect{|x| x.values[0]}
      end
      
      def aggregate(query)
        raise ArgumentError.new("Only count is supported") unless (query.fields.first.operator == :count)
        sdb_type = simpledb_type(query.model)
        conditions, order, unsupported_conditions = set_conditions_and_sort_order(query, sdb_type)

        query_call = "SELECT count(*) FROM #{domain} "
        query_call << "WHERE #{conditions.compact.join(' AND ')}" if conditions.length > 0
        results = nil
        time = Benchmark.realtime do
          results = sdb.select(query_call)
        end; DataMapper.logger.debug(format_log_entry(query_call, time))
        [results[:items][0].values.first["Count"].first.to_i]
      end
      
    private

      def consistent?(item_name)
        !sdb.get_attributes(@sdb_options[:domain], item_name)[:attributes].empty?
      end

      #hack for converting and storing strings longer than 1024
      #one thing to note if you use string longer than 1019 chars you will loose the ability to do full text matching on queries
      #as the string can be broken at any place during chunking
      def adjust_to_sdb_attributes(attrs)
        attrs.each_pair do |key, value|
          if value.is_a?(String)
            value = value.gsub("\n",NEWLINE_REPLACE)
            attrs[key] = value
          end
          if value.is_a?(String) && value.length > 1019
            chunked = string_to_chunks(value)
            attrs[key] = chunked
          end
        end
        attrs
      end
      
      def string_to_chunks(value)
        chunks = value.to_s.scan(%r/.{1,1019}/) # 1024 - '1024:'.size
        i = -1
        fmt = '%04d:'
        chunks.map!{|chunk| [(fmt % (i += 1)), chunk].join}
        raise ArgumentError, 'that is just too big yo!' if chunks.size >= 256
        chunks
      end
      
      def chunks_to_string(value)
        begin
          chunks =
            Array(value).flatten.map do |chunk|
            index, text = chunk.split(%r/:/, 2)
            [Float(index).to_i, text]
          end
          chunks.replace chunks.sort_by{|index, text| index}
          string_result = chunks.map!{|index, text| text}.join
        rescue ArgumentError, TypeError
          #return original value, they could have put strings in the system not using the adapter or previous versions
          #that are larger than chunk size, but less than 1024
          value
        end
      end

      # Returns the domain for the model
      def domain
        @sdb_options[:domain]
      end

      #sets the conditions and order for the SDB query
      def set_conditions_and_sort_order(query, sdb_type)
        unsupported_conditions = []
        conditions = ["simpledb_type = '#{sdb_type}'"]
        # look for query.order.first and insure in conditions
        # raise if order if greater than 1

        if query.order && query.order.length > 0
          query_object = query.order[0]
          #anything sorted on must be a condition for SDB
          conditions << "#{query_object.target.name} IS NOT NULL" 
          order = "ORDER BY #{query_object.target.name} #{query_object.operator}"
        else
          order = ""
        end
        query.conditions.each do |op|
          case op.slug
          when :regexp
            unsupported_conditions << op
          when :eql
            conditions << if op.value.nil?
              "#{op.subject.name} IS NULL"
            else
              "#{op.subject.name} = '#{op.value}'"
            end
          when :not then
            comp = op.operands.first
            if comp.slug == :like
              conditions << "#{comp.subject.name} not like '#{comp.value}'"
              next
            end
            case comp.value
            when Range, Set, Array, Regexp
              unsupported_conditions << op
            when nil
              conditions << "#{comp.subject.name} IS NOT NULL"
            else
              conditions << "#{comp.subject.name} != '#{comp.value}'"
            end
          when :gt then conditions << "#{op.subject.name} > '#{op.value}'"
          when :gte then conditions << "#{op.subject.name} >= '#{op.value}'"
          when :lt then conditions << "#{op.subject.name} < '#{op.value}'"
          when :lte then conditions << "#{op.subject.name} <= '#{op.value}'"
          when :like then conditions << "#{op.subject.name} like '#{op.value}'"
          when :in
            case op.value
            when Array, Set
              values = op.value.collect{|v| "'#{v}'"}.join(',')
              values = "'__NULL__'" if values.empty?                       
              conditions << "#{op.subject.name} IN (#{values})"
            when Range
              if op.value.exclude_end?
                unsupported_conditions << op
              else
                conditions << "#{op.subject.name} between '#{op.value.first}' and '#{op.value.last}'"
              end
            else
              raise ArgumentError, "Unsupported inclusion op: #{op.value.inspect}"
            end
          else raise "Invalid query op: #{op.inspect}"
          end
        end
        [conditions,order,unsupported_conditions]
      end
      
      def select(query_call, query_limit)
        items = []
        time = Benchmark.realtime do
          sdb_continuation_key = nil
          while (results = sdb.select(query_call, sdb_continuation_key)) do
            sdb_continuation_key = results[:next_token]
            items += results[:items]
            break if items.length > query_limit
            break if sdb_continuation_key.nil?
          end
        end; DataMapper.logger.debug(format_log_entry(query_call, time))
        items[0...query_limit]
      end
      
      #gets all results or proper number of results depending on the :limit
      def get_results(query, conditions, order)
        query_call = "SELECT * FROM #{domain} "
        query_call << "WHERE #{conditions.compact.join(' AND ')}" if conditions.length > 0
        query_call << " #{order}"
        if query.limit!=nil
          query_limit = query.limit
          query_call << " LIMIT #{query.limit}" 
        else
          #on large items force the max limit
          query_limit = 999999999 #TODO hack for query.limit being nil
          #query_call << " limit 2500" #this doesn't work with continuation keys as it halts at the limit passed not just a limit per query.
        end
        select(query_call, query_limit)
      end
      
      # Creates an item name for a query
      def item_name_for_query(query)
        sdb_type = simpledb_type(query.model)
        
        item_name = "#{sdb_type}+"
        keys = keys_for_model(query.model)
        conditions = query.conditions.sort {|a,b| a[1].name.to_s <=> b[1].name.to_s }
        item_name += conditions.map do |property|
          property[2].to_s
        end.join('-')
        Digest::SHA1.hexdigest(item_name)
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
        model.key(self.name).sort {|a,b| a.name.to_s <=> b.name.to_s }
      end
      
      def not_eql_query?(query)
        # Curosity check to make sure we are only dealing with a delete
        conditions = query.conditions.map {|c| c.slug }.uniq
        selectors = [ :gt, :gte, :lt, :lte, :not, :like, :in ]
        return (selectors - conditions).size != selectors.size
      end
      
      # Returns an SimpleDB instance to work with
      def sdb
        access_key = @sdb_options[:access_key]
        secret_key = @sdb_options[:secret_key]
        @sdb ||= RightAws::SdbInterface.new(access_key,secret_key,@sdb_options)
        @sdb
      end
      
      # Returns a string so we know what type of
      def simpledb_type(model)
        model.storage_name(model.repository.name)
      end

      def format_log_entry(query, ms = 0)
        'SDB (%.1fs)  %s' % [ms, query.squeeze(' ')]
      end

      #integrated from http://github.com/edward/dm-simpledb/tree/master
      module Migration
        # Returns whether the storage_name exists.
        # @param storage_name<String> a String defining the name of a domain
        # @return <Boolean> true if the storage exists
        def storage_exists?(storage_name)
          domains = sdb.list_domains[:domains]
          domains.detect {|d| d == storage_name }!=nil
        end
        
        def create_model_storage(repository, model)
          sdb.create_domain(@sdb_options[:domain])
        end
        
        #On SimpleDB you probably don't want to destroy the whole domain
        #if you are just adding fields it is automatically supported
        #default to non destructive migrate, to destroy run
        #rake db:automigrate destroy=true
        def destroy_model_storage(repository, model)
          if ENV['destroy']!=nil && ENV['destroy']=='true'
            sdb.delete_domain(@sdb_options[:domain])
          end
        end
        
        #TODO look at github panda simpleDB for serials support?
        module SQL
          def supports_serial?
            false
          end
        end
        
        include SQL
        
      end # module Migration
      
      include Migration
      
    end # class SimpleDBAdapter
    
    # Required naming scheme.
    SimpledbAdapter = SimpleDBAdapter
    
  end # module Adapters
end # module DataMapper
