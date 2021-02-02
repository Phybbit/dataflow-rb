[![Build Status](https://travis-ci.org/Phybbit/dataflow-rb.svg?branch=master)](https://travis-ci.org/Phybbit/dataflow-rb)

# Dataflow

The purpose of this gem is to help building complex dataflows and support automating long-running batch processes.
It handles parallelizing computation whenever it cans and re-computing dependencies that are not up-to-date.

There are two main concepts in describing a computing graph:
- data-nodes, which support storing/retrieving data from databases
- compute-nodes, which supports arbitrary processing, can depend on any number of nodes (compute/data) and can push their results to a data-node if needed

The main use case is to represent data sources with data-nodes and link those to compute-nodes. Upon computing, the node will store the result in another data-node.

The graph's metadata (e.g. nodes' dependencies, properties) is stored in MongoDB. It also uses MongoDB as the default DB for the data-node storage as it allows for quick schema-less prototyping. MySQL and PostgreSQL are also supported (through [Sequel](https://github.com/jeremyevans/sequel)).

This repository only includes the most common nodes. Other repos will include custom (application-dependent) nodes.

It has some similarities with the [Luigi](https://github.com/spotify/luigi) python module.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'dataflow-rb'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install dataflow-rb

You also need to install:
- mongodb 3.2 (required)
- postgresql (optional)
- mysql (optional)

## Usage

```ruby
require 'dataflow-rb'

# Create a data node
node1 = Dataflow::Nodes::DataNode.create(db_name: 'test', name: 'data_source1')
node1.add(records: [{id: 1, first_name: 'hello'}])
node1.all
# => [{"id"=>1, "name"=>"test"}]

node2 = Dataflow::Nodes::DataNode.create(db_name: 'test', name: 'data_source2')
node2.add(records: [{id: 1, last_name: 'world'}])
node2.all
# => [{"id"=>1, "name"=>"world"}]

# We will keep the results of the computation in this dataset
result_node = Dataflow::Nodes::DataNode.create(db_name: 'test', name: 'result')

# Join the 2 datasets by id:
compute_node = Dataflow::Nodes::JoinNode.create(
  name: 'join',
  dependency_ids: [node1, node2],
  data_node_id: result_node,
  key1: 'id',
  key2: 'id'
)
compute_node.compute
compute_node.data_node.all
# => [{"id"=>1, "first_name"=>"hello", "last_name"=>"world"}]
compute_node.all # this is just a facade for the above
# => [{"id"=>1, "first_name"=>"hello", "last_name"=>"world"}]

# Fetch the data again later:
result_node = Dataflow::Nodes::DataNode.find_by(name: 'result')
# or the short hand:
result_node = Dataflow.data_node('result')
result_node.all
# => [{"id"=>1, "first_name"=>"hello", "last_name"=>"world"}]
```

### Mongoid
TODO: Prepare an API that can set Mongoid config

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/phybbit/dataflow-rb.
