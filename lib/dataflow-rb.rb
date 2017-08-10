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
require 'dataflow/executor'
require 'dataflow/remote_worker'

require 'dataflow/adapters/csv_adapter'
require 'dataflow/adapters/mongo_db_adapter'
require 'dataflow/adapters/sql_adapter'
require 'dataflow/adapters/mysql_adapter'
require 'dataflow/adapters/psql_adapter'
require 'dataflow/adapters/settings'

require 'dataflow/errors/invalid_configuration_error'
require 'dataflow/errors/remote_execution_error'

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

  # Exports nodes and their data. Use #import to re-import them elsewhere.
  def self.export(nodes:, export_dir: './flows', include_data: false)
    raise ArgumentError, 'nodes must be an array of nodes' unless nodes.is_a?(Array)
    # make a tmp folder with the export dir
    archive_name = "flow_#{Time.now.strftime("%Y-%m-%d_%H-%M-%S")}"
    tmp_dir = "#{export_dir}/#{archive_name}"
    `mkdir -p #{tmp_dir}`

    # export all the dependencies
    all_nodes = nodes + nodes.flat_map(&:all_dependencies)
    # and all the compute node's datasets
    all_nodes += all_nodes.select { |x| x.is_a?(Dataflow::Nodes::ComputeNode) }
                          .map { |x| x.data_node }

    # sort by dependency level so that we can re-create without problems
    # in the import step
    all_nodes = all_nodes.sort_by { |n| n.dependency_level }

    # get all the nodes' metadata in the yaml format
    metadata_yaml = all_nodes.compact.uniq.map(&:metadata).to_yaml
    File.write("#{tmp_dir}/metadata.yaml", metadata_yaml)

    # add the dataset's data if necessary
    if include_data
      all_nodes.select { |x| x.is_a?(Dataflow::Nodes::DataNode) }
               .each { |x| x.dump_dataset(base_folder: tmp_dir) }
    end

    # pack all the content in a tar archive
    archive_path = "#{archive_name}.tar"
    `(cd #{export_dir} && tar -cvf #{archive_path} #{archive_name})`

    # clear the tmp folder
    `rm -rf #{tmp_dir}`

    "#{export_dir}/#{archive_path}"
  end

  def self.import(archive_path:)
    raise ArgumentError, 'expecting a tar archive file' unless archive_path.end_with?('.tar')

    # extract the tar
    folder_name = archive_path.split('/')[-1].split('.')[0]
    `tar -xvf #{archive_path}`

    # load and restore the content in the metadata.yaml
    metadata = YAML.load_file("#{folder_name}/metadata.yaml")

    # restore the nodes
    metadata.each do |m|
      klass = m[:_type].constantize

      # try to delete previously existing node
      begin
        previous_node = klass.find(m[:_id])
        previous_node.delete
      rescue Mongoid::Errors::DocumentNotFound
      end

      # create the node
      klass.create(m)
    end

    # look for dataset dumps and restore them
    filepaths = Dir["./#{folder_name}/**/*.gz"] + Dir["./#{folder_name}/**/*.dump"]

    filepaths.each do |filepath|
      # filepath: "./folder/db_name/dataset.1.gz"
      db_name = filepath.split('/')[2]
      dataset = filepath.split('/')[3].split('.')[0]
      n = Dataflow::Nodes::DataNode.find_by(db_name: db_name, name: dataset)
      n.restore_dataset(filepath: filepath)
    end


    # clean up the extracted folder
    `rm -rf #{folder_name}`
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

      @name_errors ||= Set.new
      unless @name_errors.include?(e.message)
        p "Warning -- Node class not found. #{e}"
        @name_errors << e.message
      end

      Dataflow::Nodes::ComputeNode
    end
  end
end

ActiveSupport::Inflector.module_eval do
  extend Dataflow::ConstantizePatch
end
###############################################################################
