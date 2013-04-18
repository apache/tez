#!/bin/bash
#TEZ_SRC_DIR=`pwd`
M2_REPO=$HOME/.m2/repository
ln -s $TEZ_SRC_DIR/tez-api/target/tez-api-1.0-SNAPSHOT.jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib
ln -s $TEZ_SRC_DIR/tez-common/target/tez-common-1.0-SNAPSHOT.jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib
ln -s $TEZ_SRC_DIR/tez-engine/target/tez-engine-1.0-SNAPSHOT.jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib
ln -s $TEZ_SRC_DIR/tez-mapreduce/target/tez-mapreduce-1.0-SNAPSHOT.jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib
ln -s $M2_REPO/com/google/inject/extensions/guice-assistedinject/3.0/guice-assistedinject-3.0.jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib
