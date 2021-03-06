# Changelog

#### 0.15.0

- [4b48b74] Make sure computed data is included in the dump
- [54fd18d] Added support for #export #import a dataflow with its data
- [a63972f] Add #metadata to the nodes
- [696ea35] Add #all_dependencies interface
- [5165c71] Fix re-using the same variable in the RemoteWorker results
- [216a066] Only warn once about a missing node
- [c101144] Support gettng data back from remote batch workers and adding it to the data node.
- [9a06ee3] Support remapping the dataset from which to read from on ReadOnlyDataNodes
- [2fc623a] Setting the clean if exist option to pg restore. Set pg to 0.20
- [205317c] Support including the system id in the data
- [0b9b578] Fix restoring postgresql indexes and other constraints
- [e396265] Make sure indexes are recreated in postgres after a restore
- [426300a] Add the exported dataset idx in the dump filename. Make sure when restoring that the settings are compatible
- [ca44a9d] Set the no owner flag when restoring to psql

#### 0.14.0
- [ef8ddcd] Do not assume a minimum of 1 dependency per compute node.
- [b131bb1] Add type check on the data node #add methods. Filter nil values.
- [effc5a4] Set the rabbitmq as coming from the env
- [577ea2e] Add support for computing a node remotely.
- [4a450c3] Remove the custom not implemented error and use the default one.
- [f9c48c5] Added some new lines
- [336b9f8] Fix the backup options
- [2b2fbee] Make the runtime query node a subclass of the read only data node
- [fe237c4] Change the backup structure to isolate the db name by folder
- [654927f] Experiment with querying arrays
- [506f105] Order by system id when exporting
- [fa8fdc3] Keep the data ordered when exporting to csv
- [5e1718d] Add support for postgresql when inferring partial schemas (needed for export)

#### 0.13.1
- [aa3ed2e] Fix a bug when storing a db connection

#### 0.13.0
- [b79c96f] Fix a bug in the sql adapter: support multiple ORDER BY clauses
- [a17f071] Add runtime query node. Make the ops transformation public.
- [8c78aa2] Added support for a per-node backup/restore
- [6069ec0] Moved the db settings to the settings class
- [b5a77fc] Set the last update time using a query directly on the DB. Do not return unneeded information from the recompute/explain method
- [cc77366] Explain why a node needs an update
- [e87ba14] Add logging to the sql query node
- [5d82dfc] Fix logging during the sql table creation.
- [7390264] Add a read-only data node
- [dbb14ed] Refactor the debugging implementation
- [38925a3] Added parameters on the data node to flexibly connec to any database
- [7aac1eb] Add support for partial (where clause) parallel queries generation.

#### 0.12.1
- [110ded7] Fix compute node not processing in parallel

#### 0.12.0
- [4a510df] Add support for case insentive regex matching on mysql
- [63b0771] Add logging to understand the current computation batch progress
- [df86157] Add support for pg array types
- [ce04cb3] Add the loose count extension for Sequel Postgres
- [3618060] Fix Sequel deprecation warnings
- [1cea32e] Skip logging during tests sessions
- [fdddf23] Add support for regex matching
- [b4717c5] Move the refactor the mongo batch insert
- [e2897df] Use named indexes to reduce their name size
- [bc4f598] Revert to insert_ignore to support mysql adapter

#### 0.11.0
- [7c09e8a] Add data_node#drop_dataset! to completely drop the data
- [ba0532f] Added upsert on psql adapter
- [4d44bbd] Support setting the number of parallel processes
- [8b48a6b] Add support for double buffered schema inferrence on postgresql
- [49bfe1a] Add support for clearing unused datasets
- [aabd5e3] Added #required_by to the node interface
- [4fd2617] Handle forks having the same thread id
- [7fc3064] Add error logging and trace id
- [fbbd58b] Added heartbeats when recomputing the dependencies and before the pre-compute callback

#### 0.10.2
- [966e771] Do not crash if there is an unknown node type in the metadata.

#### 0.10.1
- [9ee24a4] Cleanly set the mongoid env Fix the bin/console script
- [7fdc6f1] Support symbols in schema keys when merging schemas in the join node
- [6c7ad5c] Fail silently if no table exists when fetching its metadata
- [6b0886e] Make the ComputeNode#schema public
- [03f37e2] Optimize the select keys node to avoid recomputing keys at each record.
- [23ae504] ComputeNode#schema returns the required schema

#### 0.10.0
- [2f6284c] Allow the pre-compute to modify the necessary schema
- [cec8a1d] Do not crash if process_parallel is called without dependencies.
- [83e1bb5] Various fixes related to the csv export feature
- [61e74d7] Force the data node to use double buffering when necessary.
- [553b1ea] Fix documentation
- [be21031] Added an heartbeat to the compute node
- [78308c0] Added tests to the join node. Add multi key support on Postgresql impl and select_keys support on software join.
- [090c81f] Experimental: fetch the schema directly from the DB.
- [46a7915] Fix: use the samples count when inferring a schema
- [dcd7750] Add support for selecting which keys to include in a join.
- [9005b6c] Set a default updated_at when creating a data node. Do not change the dataset immediately if we're using double buffering. Wait for the next buffer to be created instead.
- [d98d9c1] Do not crash if an index cannot be added. Use the logger instead of the stdout for the sql adapter.
- [cc40642] Catch DatabaseError.

#### 0.9.2
- [2f3129c] Fix bug when joining datasets directly in SQL
- Updated the readme with some information on how to use the gem
- Set up .travis.yml

#### 0.9.1
- Fixed the gem public information

#### 0.9.0
- Extracted the open-source version

