# MySQL to HBase Replicator.
Replicates data changes from MySQL binlog to HBase, preserving the previous data versions. Intended
for auditing purposes of historical data.

# STATUS
In testing, beta-level quality.

# USAGE
java -jar hbrepl-0.9.4 --dc $dc --applier $applier --schema $schema --binlog-filename $binlog-filename --config-path $config-path --shard $shard

# CONFIGURATION
One yml file. Example of config file:

    schema_tracker:
        username: 'user'
        password: 'pass'
        hosts:
            dc1: 'dc1-host'
            dc2: 'dc2-host'
    replicated_schema_name:
        username: 'user'
        password: 'pass'
        slaves:
            dc1: ['dc1-host-01', 'dc1-host-02']
            dc2: ['dc2-host-01']
    zookeepers:
        dc1: 'hbase-dc1-zk1-host, ..., hbase-dc1-zk5-host'
        dc2: 'hbase-dc2-zk1-host, ..., hbase-dc2-zk5-host'
    graphite:
        namespace: 'my.graphite.namespace'

# AUTHOR
Bosko Devetak <bosko.devetak@gmail.com>

# ACKNOWLEDGMENT
Replicator was originally developed for Booking.com. With approval from Booking.com, the code and specification were generalized and published as Open Source on github, for which the author would like to express his gratitude.

# COPYRIGHT AND LICENSE
Copyright (C) 2015 by Bosko Devetak

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
