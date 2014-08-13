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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.EntityDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.TaskContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

// Not calling this a MemoryManager explicitly. Not yet anyway.
@Private
public class MemoryDistributor {

  private static final Log LOG = LogFactory.getLog(MemoryDistributor.class);

  private final int numTotalInputs;
  private final int numTotalOutputs;
  private final Configuration conf;
  
  private AtomicInteger numInputsSeen = new AtomicInteger(0);
  private AtomicInteger numOutputsSeen = new AtomicInteger(0);

  private long totalJvmMemory;
  private final boolean isEnabled;
  private final Set<TaskContext> dupSet = Collections
      .newSetFromMap(new ConcurrentHashMap<TaskContext, Boolean>());
  private final List<RequestorInfo> requestList;

  /**
   * @param numTotalInputs
   *          total number of Inputs for the task
   * @param numTotalOutputs
   *          total number of Outputs for the task
   * @param conf
   *          Tez specific task configuration
   */
  public MemoryDistributor(int numTotalInputs, int numTotalOutputs, Configuration conf) {
    this.conf = conf;
    isEnabled = conf.getBoolean(TezConfiguration.TEZ_TASK_SCALE_TASK_MEMORY_ENABLED,
        TezConfiguration.TEZ_TASK_SCALE_TASK_MEMORY_ENABLED_DEFAULT);
    

    this.numTotalInputs = numTotalInputs;
    this.numTotalOutputs = numTotalOutputs;
    this.totalJvmMemory = Runtime.getRuntime().maxMemory();
    this.requestList = Collections.synchronizedList(new LinkedList<RequestorInfo>());
    LOG.info("InitialMemoryDistributor (isEnabled=" + isEnabled + ") invoked with: numInputs="
        + numTotalInputs + ", numOutputs=" + numTotalOutputs
        + ", JVM.maxFree=" + totalJvmMemory);
  }


  
  /**
   * Used by the Tez framework to request memory on behalf of user requests.
   */
  public void requestMemory(long requestSize, MemoryUpdateCallback callback,
      TaskContext taskContext, EntityDescriptor<?> descriptor) {
    registerRequest(requestSize, callback, taskContext, descriptor);
  }
  
  /**
   * Used by the Tez framework to distribute initial memory after components
   * have made their initial requests.
   */
  public void makeInitialAllocations() {
    Preconditions.checkState(numInputsSeen.get() == numTotalInputs, "All inputs are expected to ask for memory");
    Preconditions.checkState(numOutputsSeen.get() == numTotalOutputs, "All outputs are expected to ask for memory");
    Iterable<InitialMemoryRequestContext> requestContexts = Iterables.transform(requestList,
        new Function<RequestorInfo, InitialMemoryRequestContext>() {
          public InitialMemoryRequestContext apply(RequestorInfo requestInfo) {
            return requestInfo.getRequestContext();
          }
        });

    Iterable<Long> allocations = null;
    if (!isEnabled) {
      allocations = Iterables.transform(requestList, new Function<RequestorInfo, Long>() {
        public Long apply(RequestorInfo requestInfo) {
          return requestInfo.getRequestContext().getRequestedSize();
        }
      });
    } else {
      String allocatorClassName = conf.get(TezConfiguration.TEZ_TASK_SCALE_TASK_MEMORY_ALLOCATOR_CLASS,
          TezConfiguration.TEZ_TASK_SCALE_TASK_MEMORY_ALLOCATOR_CLASS_DEFAULT);
      LOG.info("Using Allocator class: " + allocatorClassName);
      InitialMemoryAllocator allocator = ReflectionUtils.createClazzInstance(allocatorClassName);
      allocator.setConf(conf);
      allocations = allocator.assignMemory(totalJvmMemory, numTotalInputs, numTotalOutputs,
          Iterables.unmodifiableIterable(requestContexts));
      validateAllocations(allocations, requestList.size());
    }

    // Making the callbacks directly for now, instead of spawning threads. The
    // callback implementors - all controlled by Tez at the moment are
    // lightweight.
    Iterator<Long> allocatedIter = allocations.iterator();
    for (RequestorInfo rInfo : requestList) {
      long allocated = allocatedIter.next();
      LOG.info("Informing: " + rInfo.getRequestContext().getComponentType() + ", "
          + rInfo.getRequestContext().getComponentVertexName() + ", "
          + rInfo.getRequestContext().getComponentClassName() + ": requested="
          + rInfo.getRequestContext().getRequestedSize() + ", allocated=" + allocated);
      rInfo.getCallback().memoryAssigned(allocated);
    }
  }

  /**
   * Allow tests to set memory.
   * @param size
   */
  @Private
  @VisibleForTesting
  void setJvmMemory(long size) {
    this.totalJvmMemory = size;
  }

  private long registerRequest(long requestSize, MemoryUpdateCallback callback,
      TaskContext entityContext, EntityDescriptor<?> descriptor) {
    Preconditions.checkArgument(requestSize >= 0);
    Preconditions.checkNotNull(callback);
    Preconditions.checkNotNull(entityContext);
    Preconditions.checkNotNull(descriptor);
    if (!dupSet.add(entityContext)) {
      throw new TezUncheckedException(
          "A single entity can only make one call to request resources for now");
    }

    RequestorInfo requestInfo = new RequestorInfo(entityContext,requestSize, callback, descriptor);
    switch (requestInfo.getRequestContext().getComponentType()) {
    case INPUT:
      numInputsSeen.incrementAndGet();
      Preconditions.checkState(numInputsSeen.get() <= numTotalInputs,
          "Num Requesting Inputs higher than total # of inputs: " + numInputsSeen + ", "
              + numTotalInputs);
      break;
    case OUTPUT:
      numOutputsSeen.incrementAndGet();
      Preconditions.checkState(numOutputsSeen.get() <= numTotalOutputs,
          "Num Requesting Inputs higher than total # of outputs: " + numOutputsSeen + ", "
              + numTotalOutputs);
    case PROCESSOR:
      break;
    default:
      break;
    }
    requestList.add(requestInfo);
    return -1;
  }

  private void validateAllocations(Iterable<Long> allocations, int numRequestors) {
    Preconditions.checkNotNull(allocations);
    long totalAllocated = 0l;
    int numAllocations = 0;
    for (Long l : allocations) {
      totalAllocated += l;
      numAllocations++;
    }
    Preconditions.checkState(numAllocations == numRequestors,
        "Number of allocations must match number of requestors. Allocated=" + numAllocations
            + ", Requests: " + numRequestors);
    Preconditions.checkState(totalAllocated <= totalJvmMemory,
        "Total allocation should be <= availableMem. TotalAllocated: " + totalAllocated
            + ", totalJvmMemory: " + totalJvmMemory);
  }


  private static class RequestorInfo {

    private static final Log LOG = LogFactory.getLog(RequestorInfo.class);

    private final MemoryUpdateCallback callback;
    private final InitialMemoryRequestContext requestContext;

    public RequestorInfo(TaskContext taskContext, long requestSize,
        final MemoryUpdateCallback callback, EntityDescriptor<?> descriptor) {
      InitialMemoryRequestContext.ComponentType type;
      String componentVertexName;
      if (taskContext instanceof InputContext) {
        type = InitialMemoryRequestContext.ComponentType.INPUT;
        componentVertexName = ((InputContext) taskContext).getSourceVertexName();
      } else if (taskContext instanceof OutputContext) {
        type = InitialMemoryRequestContext.ComponentType.OUTPUT;
        componentVertexName = ((OutputContext) taskContext).getDestinationVertexName();
      } else if (taskContext instanceof ProcessorContext) {
        type = InitialMemoryRequestContext.ComponentType.PROCESSOR;
        componentVertexName = ((ProcessorContext) taskContext).getTaskVertexName();
      } else {
        throw new IllegalArgumentException("Unknown type of entityContext: "
            + taskContext.getClass().getName());
      }
      this.requestContext = new InitialMemoryRequestContext(requestSize, descriptor.getClassName(),
          type, componentVertexName);
      this.callback = callback;
      LOG.info("Received request: " + requestSize + ", type: " + type + ", componentVertexName: "
          + componentVertexName);
    }

    public MemoryUpdateCallback getCallback() {
      return callback;
    }

    public InitialMemoryRequestContext getRequestContext() {
      return requestContext;
    }
  }

}
