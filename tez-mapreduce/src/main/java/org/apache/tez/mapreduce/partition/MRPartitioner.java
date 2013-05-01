/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.tez.mapreduce.partition;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.engine.api.Master;
import org.apache.tez.mapreduce.processor.MRTask;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MRPartitioner implements org.apache.tez.engine.api.Partitioner {

  static final Log LOG = LogFactory.getLog(MRPartitioner.class);
  private final MRTask task;
  
  JobConf jobConf;
  boolean useNewApi;
  
  org.apache.hadoop.mapred.Partitioner oldPartitioner;
  org.apache.hadoop.mapreduce.Partitioner newPartitioner;

  public MRPartitioner(MRTask task) {
    this.task = task;
  }
  
  public void initialize(Configuration conf, Master master) 
      throws IOException, InterruptedException {
    if (conf instanceof JobConf) {
      jobConf = (JobConf)conf;
    } else {
      jobConf = new JobConf(conf);
    }
    
    useNewApi = jobConf.getUseNewMapper();
    final int partitions = this.task.getTezEngineTaskContext()
        .getOutputSpecList().get(0).getNumOutputs();
    if (useNewApi) {
      if (partitions > 1) {
        try {
          newPartitioner = (org.apache.hadoop.mapreduce.Partitioner)
            ReflectionUtils.newInstance(
                task.getJobContext().getPartitionerClass(), jobConf);
        } catch (ClassNotFoundException cnfe) {
          throw new IOException(cnfe);
        }
      } else {
        newPartitioner = new org.apache.hadoop.mapreduce.Partitioner() {
          @Override
          public int getPartition(Object key, Object value, int numPartitions) {
            return numPartitions - 1;
          }
        };
      }
    } else {
      if (partitions > 1) {
        oldPartitioner = (Partitioner)
          ReflectionUtils.newInstance(jobConf.getPartitionerClass(), jobConf);
      } else {
        oldPartitioner = new Partitioner() {
          @Override
          public void configure(JobConf job) {}
          
          @Override
          public int getPartition(Object key, Object value, int numPartitions) {
            return numPartitions - 1;
          }
        };
      }

    }

  }
  
  @Override
  public int getPartition(Object key, Object value, int numPartitions) {
    if (useNewApi) {
      return newPartitioner.getPartition(key, value, numPartitions);
    } else {
      return oldPartitioner.getPartition(key, value, numPartitions);
    }
  }

}
