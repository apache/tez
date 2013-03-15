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

package org.apache.hadoop.mapreduce.v2.app2.lazy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerRequestor;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

public class LazyRMContainerRequestor extends RMContainerRequestor {

  private int preAllocContainerCount;
  private int preAllocContainerMemoryMB;
  private Configuration conf;

  private static final Log LOG =
      LogFactory.getLog(LazyRMContainerRequestor.class);

  public LazyRMContainerRequestor(ClientService clientService,
      AppContext context) {
    super(clientService, context);
  }

  @Override
  public void init(Configuration conf) {
    this.conf = conf;
    preAllocContainerCount = conf.getInt(
        LazyAMConfig.PREALLOC_CONTAINER_COUNT,
        LazyAMConfig.DEFAULT_PREALLOC_CONTAINER_COUNT);
    preAllocContainerMemoryMB =
        conf.getInt(MRJobConfig.MAP_MEMORY_MB,
            MRJobConfig.DEFAULT_MAP_MEMORY_MB);

    LOG.info("Prealloc container count for LazyAM is "
        + preAllocContainerCount
        + ", memoryMB=" + preAllocContainerMemoryMB);

    if (preAllocContainerCount < 0
        || preAllocContainerMemoryMB < 0) {
      throw new IllegalArgumentException("Invalid config values specified"
          + ", preAllocationContainerCount=" + preAllocContainerCount
          + ", mapMemoryMB=" + preAllocContainerMemoryMB);
    }

    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
    if (preAllocContainerCount <= 0) {
      return;
    }
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(preAllocContainerMemoryMB);
    capability.setVirtualCores(1);

    String[] emptyArray = new String[0];
    for (int i = 0; i < preAllocContainerCount; ++i) {
      ContainerRequest cReq = new ContainerRequest(capability, emptyArray,
          emptyArray, RMContainerAllocator.PRIORITY_MAP);
      super.addContainerReq(cReq);
    }
  }

  @Override
  public synchronized Configuration getConfig() {
    return this.conf;
  }

}
