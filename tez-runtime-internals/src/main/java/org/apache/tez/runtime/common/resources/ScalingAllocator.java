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

package org.apache.tez.runtime.common.resources;

import java.text.DecimalFormat;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

@Public
@Unstable
public class ScalingAllocator implements InitialMemoryAllocator {

  private static final Log LOG = LogFactory.getLog(ScalingAllocator.class);

  private Configuration conf;

  @Override
  public Iterable<Long> assignMemory(long availableForAllocation, int numTotalInputs,
      int numTotalOutputs, Iterable<InitialMemoryRequestContext> requests) {

    int numRequests = 0;
    long totalRequested = 0;
    for (InitialMemoryRequestContext context : requests) {
      totalRequested += context.getRequestedSize();
      numRequests++;
    }

    // Take a certain amount of memory away for general usage.
    double reserveFraction = conf.getDouble(TezConfiguration.TEZ_TASK_SCALE_TASK_MEMORY_RESERVE_FRACTION,
        TezConfiguration.TEZ_TASK_SCALE_TASK_MEMORY_RESERVE_FRACTION_DEFAULT);
    Preconditions.checkState(reserveFraction >= 0.0d && reserveFraction <= 1.0d);
    availableForAllocation = (long) (availableForAllocation - (reserveFraction * availableForAllocation));

    long totalJvmMem = Runtime.getRuntime().maxMemory();
    double ratio = totalRequested / (double) totalJvmMem;
    LOG.info("Scaling Requests. TotalRequested: " + totalRequested + ", TotalJVMHeap: "
        + totalJvmMem + ", TotalAvailable: " + availableForAllocation
        + ", TotalRequested/TotalJVMHeap:" + new DecimalFormat("0.00").format(ratio));

    if (totalRequested < availableForAllocation || totalRequested == 0) {
      // Not scaling up requests. Assuming things were setup correctly by
      // users in this case, keeping Processor, caching etc in mind.
      return Lists.newArrayList(Iterables.transform(requests,
          new Function<InitialMemoryRequestContext, Long>() {
        public Long apply(InitialMemoryRequestContext requestContext) {
          return requestContext.getRequestedSize();
        }
      }));
    }

    List<Long> allocations = Lists.newArrayListWithCapacity(numRequests);
    for (InitialMemoryRequestContext request : requests) {
      long requestedSize = request.getRequestedSize();
      if (requestedSize == 0) {
        allocations.add(0l);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scaling requested: 0 to allocated: 0");
        }
      } else {
        long allocated = (long) ((requestedSize / (double) totalRequested) * availableForAllocation);
        allocations.add(allocated);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scaling requested: " + requestedSize + " to allocated: " + allocated);  
        }

      }
    }
    return allocations;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}