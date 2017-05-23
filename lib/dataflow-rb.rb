# frozen_string_literal: true
require 'schema/inference'
require 'mongoid'
require 'sequel'
require 'msgpack'
require 'parallel'
require 'smarter_csv'
require 'timeliness'
require 'chronic'

require 'dataflow/version'
require 'dataflow/extensions/msgpack'
require 'dataflow/extensions/mongo_driver'

require 'dataflow/event_mixin'
require 'dataflow/logger'
require 'dataflow/properties_mixin'
require 'dataflow/schema_mixin'
require 'dataflow/node'

require 'dataflow/adapters/csv_adapter'
require 'dataflow/adapters/mongo_db_adapter'
require 'dataflow/adapters/sql_adapter'
require 'dataflow/adapters/mysql_adapter'
require 'dataflow/adapters/psql_adapter'
require 'dataflow/adapters/settings'

require 'dataflow/errors/invalid_configuration_error'
require 'dataflow/errors/not_implemented_error'

require 'dataflow/nodes/mixin/add_internal_timestamp'
require 'dataflow/nodes/mixin/rename_dotted_fields'

require 'dataflow/nodes/data_node'
require 'dataflow/nodes/compute_node'
require 'dataflow/nodes/join_node'
require 'dataflow/nodes/map_node'
require 'dataflow/nodes/merge_node'
require 'dataflow/nodes/read_only_data_node'
require 'dataflow/nodes/runtime_query_node'
require 'dataflow/nodes/select_keys_node'
require 'dataflow/nodes/snapshot_node'
require 'dataflow/nodes/sql_query_node'
require 'dataflow/nodes/upsert_node'
require 'dataflow/nodes/export/to_csv_node'
require 'dataflow/nodes/filter/drop_while_node'
require 'dataflow/nodes/filter/newest_node'
require 'dataflow/nodes/filter/where_node'
require 'dataflow/nodes/transformation/to_time_node'

unless defined?(Rails) || Mongoid.configured?
  env = ENV['MONGOID_ENV'] || 'default'
  # setup mongoid for stand-alone usage
  config_file_path = File.join(File.dirname(__FILE__), 'config', 'mongoid.yml')
  Mongoid.load!(config_file_path, env)
end

module Dataflow
  CsvPath = "#{Dir.pwd}/datanodes/csv"

  # helper that tries to find a data node by id and then by name
  def self.data_node(id)
    Dataflow::Nodes::DataNode.find(id)
  rescue Mongoid::Errors::DocumentNotFound
    Dataflow::Nodes::DataNode.find_by(name: id)
  end

  # helper that tries to find a computed node by id and then name
  def self.compute_node(id)
    Dataflow::Nodes::ComputeNode.find(id)
  rescue Mongoid::Errors::DocumentNotFound
    Dataflow::Nodes::ComputeNode.find_by(name: id)
  end

  # helper that helps clearing un-used datasets
  # NOTE: although there is a best attempt to not delete datasets that are
  # currently being written to, this is not safe to use while executing in parallel.
  def self.clear_tmp_datasets
    Dataflow::Nodes::DataNode.all.each(&:safely_clear_write_dataset)
  end
end

###############################################################################
# Override the #constantize in active_support/inflector/methods.rb
# to rescue from Dataflow::Nodes::... name errors.
# In such cases, we return a generic Dataflow::Nodes::DataNode instead.
# This is used within mongoid to instance the correct node types.
module Dataflow
  module ConstantizePatch
    def constantize(*args)
      super
    rescue NameError => e
      raise e unless e.message =~ /Dataflow::Nodes/
      p "Warning -- Node class not found. #{e}"
      Dataflow::Nodes::ComputeNode
    end
  end
end

ActiveSupport::Inflector.module_eval do
  extend Dataflow::ConstantizePatch
end
###############################################################################
