**This is a alpha test version. I would not recommend you use this in it's current state. I want to refactor this script, but the command like arguments are not going to change**

# clickhouse-reshard
A simple utility to re-distributed data from a clickhouse cluster onto new nodes in the same cluster. 

There is no current way to do this without a major disruption of your cluster other than clickhouse-reshard. 

# Concept
The concept of this utility is that it detaches a partition across your cluster, moves the contents into an identical 
staging table, and writes the data back into a distributed table- which in theory contains new servers.

During an operation, the only disruption the consumers of your cluster should experence is missing data during the
period that is being re-distributed.

## Workflow
1. The clickhouse-server configuration, either locally or from a remote server, is loaded.
2. The configuration is parsed to discover the servers in the cluster.

# Assumptions
There are quite a few assumptions made in this script about your environment.
1. The executing node can SSH into each node in the cluster
2. The SSH User can modify clickhouse data directories and can read 
the configuration of the server
3. The executing node can connect to Clickhouse on each node via `:9000`
4. There is no authentication in the cluster (as it's not implemente yet)
5. There is no replication- as this script is not replication aware
6. The table schema is the same across all nodes
7. There is a parent distributed table that is aware of all nodes (both new and old)
8. Your table is using the MergeTree Engine. **This utility _has not_ been tested on other Engines such as CollapsingMergeTree**

# A Warning
In theory, this utility should not cause any major impacts to your production cluster as the data is not being modified
outside of normal- supported clickhouse operations

**BUT** this will be moving and rewriting data- which will always cary some risk. This will cause ClickHouse to need to
re-merge all the re-distributed data, which will cause a spike in disk usage, memory usage, CPU usage, and query time on
the affected partitions until Clickhouse performs the merges.

# Usage
## Requirements
* Python 3.6 (3.4 should work, but 3.6 is the development target)
* (optional but recommended) pipenv or virtualenv

## Setup

## Arguments
`--clickhouse-seed` The address of the initial server to connect and read the cluster configuration from.

`--clickhouse-config` The path of the server configuration file. If `--clickhouse-seed` is not specified, this file will
be searched for locally. 

`--clickhouse-cluster` The name of the cluster we're going to be working on. This needs to exist in the server configuration
file

`--target-database` The name of the database that will be redistributed. **NOTE** This should be table containing data, 
and not a Distributed table

`--target-distribute` The name of the distributed table that data will be written back into.

`--batch-size` The number of partitions to re-distribute per iteration. If your partition period is Per Hour a value 
of `10` would, in theory, re-distribute 10 hours at a time. Keep in mind that Yandex recommends that transactions 
should be larger than 3,000 records or you might risk losing data. The utility will throw warnings if the total records 
in to be re-distributed fall below this threshold.

## Running

Now that you have your arguments figured out, it's a good idea to do a dry run before we make any changes.

Append `--dryrun` to your arguments. The script will do everything except any changes
to the filesystem and databases.

*If you have backups, now would be the time to check them.*
