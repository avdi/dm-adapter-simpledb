module SimpleDB
  class Table

    attr_reader :model

    def initialize(model)
      @model = model
    end

    # Returns a string so we know what type of
    def simpledb_type
      model.storage_name(repository_name)
    end

    def repository_name
      # TODO this should probably take into account the adapter
      model.repository.name
    end

    # Returns the keys for model sorted in alphabetical order
    def keys_for_model
      model.key(repository_name).sort {|a,b| a.name.to_s <=> b.name.to_s }
    end
  end
end
