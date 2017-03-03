# Changelog

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

