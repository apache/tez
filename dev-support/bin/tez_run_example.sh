#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script is used to set up a local Hadoop and Tez environment for running a simple word count example.
# Prerequisites
# 1. java is installed and JAVA_HOME is set
# 2. ssh localhost works without password

# All parameters are optional:
# TEZ_VERSION: defaults to the latest version available on the Apache Tez download page
# HADOOP_VERSION: defaults to the version which belongs to the TEZ_VERSION
# TEZ_EXAMPLE_WORKING_DIR: defaults to the current working directory

# TEZ_VERSION comes from environment variable or is fetched from the Apache Tez download page
export TEZ_VERSION="${TEZ_VERSION:-$(curl -s "https://downloads.apache.org/tez/" | grep --color=never -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | sed -n '/\/$/!p' | sort -V | tail -1)}" # e.g. 0.10.4
export TEZ_EXAMPLE_WORKING_DIR="${TEZ_EXAMPLE_WORKING_DIR:-$PWD}"
cd "$TEZ_EXAMPLE_WORKING_DIR" || exit

echo "TEZ_VERSION: $TEZ_VERSION"
wget -nc "https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz"

# Need to extract the Tez tarball early to get hadoop version it depends on
if [ ! -d "apache-tez-$TEZ_VERSION-bin" ]; then
    tar -xzf "apache-tez-$TEZ_VERSION-bin.tar.gz"
fi

export HADOOP_VERSION="${HADOOP_VERSION:-$(basename "apache-tez-$TEZ_VERSION-bin/lib/hadoop-hdfs-client-"*.jar | sed -E 's/.*hadoop-hdfs-client-([0-9]+\.[0-9]+\.[0-9]+)\.jar/\1/')}" # e.g. 3.4.1

cat <<EOF
***
*** Demo setup script is running in $TEZ_EXAMPLE_WORKING_DIR ***
*** TEZ version: $TEZ_VERSION
*** HADOOP version $HADOOP_VERSION
***
EOF

wget -nc "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"

if [ ! -d "hadoop-$HADOOP_VERSION" ]; then
    tar -xzf "hadoop-$HADOOP_VERSION.tar.gz"
fi

export HADOOP_HOME="$TEZ_EXAMPLE_WORKING_DIR/hadoop-$HADOOP_VERSION"
export TEZ_HOME="$TEZ_EXAMPLE_WORKING_DIR/apache-tez-$TEZ_VERSION-bin"
export HADOOP_CLASSPATH="$TEZ_HOME/*:$TEZ_HOME/lib/*:$TEZ_HOME/conf"

export PATH="$PATH:$HADOOP_HOME/bin"

# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
cat <<EOF > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
      <name>dfs.replication</name>
      <value>1</value>
  </property>
</configuration>
EOF

cat <<EOF > "$HADOOP_HOME/etc/hadoop/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
      <name>fs.defaultFS</name>
      <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

cat <<EOF > "$HADOOP_HOME/etc/hadoop/yarn-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
EOF

# optionally stop previous clusters if any
"$HADOOP_HOME/sbin/stop-dfs.sh"
"$HADOOP_HOME/sbin/stop-yarn.sh"

rm -rf "/tmp/hadoop-$USER/dfs/data"
hdfs namenode -format -force

"$HADOOP_HOME/sbin/start-dfs.sh"
"$HADOOP_HOME/sbin/start-yarn.sh"

hadoop fs -mkdir -p "/apps/tez-$TEZ_VERSION"
hadoop fs -copyFromLocal "$TEZ_HOME/share/tez.tar.gz" "/apps/tez-$TEZ_VERSION"

# create a simple tez-site.xml
cat <<EOF > "$TEZ_HOME/conf/tez-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
      <name>tez.lib.uris</name>
      <value>/apps/tez-$TEZ_VERSION/tez.tar.gz</value>
  </property>
</configuration>
EOF

# create a simple input file
cat <<EOF > ./words.txt
Apple
Banana
Car
Apple
Banana
Car
Dog
Elephant
Friend
Game
EOF

hadoop fs -copyFromLocal "words.txt" "/words.txt"

export HADOOP_USER_CLASSPATH_FIRST=true
# finally run the example
yarn jar "$TEZ_HOME/tez-examples-$TEZ_VERSION.jar" orderedwordcount "/words.txt" "/words_out"

# check the output
hadoop fs -ls "/words_out"
hadoop fs -text "/words_out/part-v002-o000-r-00000"

cat <<EOF
*** Since the environment is already set up, you can rerun the DAG using the commands below.

export HADOOP_USER_CLASSPATH_FIRST=true
export TEZ_HOME="$TEZ_EXAMPLE_WORKING_DIR/apache-tez-$TEZ_VERSION-bin"
export HADOOP_CLASSPATH="$TEZ_HOME/*:$TEZ_HOME/lib/*:$TEZ_HOME/conf"
"$HADOOP_HOME/bin/yarn" jar "$TEZ_HOME/tez-examples-$TEZ_VERSION.jar" orderedwordcount /words.txt /words_out

*** You can also visit some of the sites that are set up during the script execution.

Yarn RM: http://localhost:8088
HDFS NN: http://localhost:9870

EOF