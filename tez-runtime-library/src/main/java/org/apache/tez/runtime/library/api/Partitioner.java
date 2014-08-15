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
package org.apache.tez.runtime.library.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

/**
 * {@link Partitioner} is used by the TEZ framework to partition output
 * key/value pairs.
 * 
 * <b>Partitioner Initialization</b></p> The Partitioner class is picked up
 * using the TEZ_RUNTIME_PARTITIONER_CLASS attribute in {@link TezRuntimeConfiguration}
 * 
 * TODO NEWTEZ Change construction to first check for a Constructor with a bytep[] payload
 * 
 * Partitioners need to provide a single argument ({@link Configuration})
 * constructor or a 0 argument constructor. If both exist, preference is given
 * to the single argument constructor. This is primarily for MR support.
 * 
 * If using the configuration constructor, TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS
 * will be set in the configuration, to indicate the max number of expected
 * partitions.
 * 
 */
@Public
@Evolving
public interface Partitioner {
  
  /**
   * Get partition for given key/value.
   * @param key key
   * @param value value
   * @param numPartitions number of partitions
   * @return partition for the given key/value
   */
  int getPartition(Object key, Object value, int numPartitions);
  
}
