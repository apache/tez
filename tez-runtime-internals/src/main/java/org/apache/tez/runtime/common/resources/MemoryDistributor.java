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
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezEntityDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.api.TezTaskContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

// Not calling this a MemoryManager explicitly. Not yet anyway.
@Private
public class MemoryDistributor {

  private static final Log LOG = LogFactory.getLog(MemoryDistributor.class);

  private final int numTotalInputs;
  private final int numTotalOutputs;
  
  private AtomicInteger numInputsSeen = new AtomicInteger(0);
  private AtomicInteger numOutputsSeen = new AtomicInteger(0);

  private long totalJvmMemory;
  private volatile long totalAssignableMemory;
  private final boolean isEnabled;
  private final boolean reserveFractionConfigured;
  private float reserveFraction;
  private final Set<TezTaskContext> dupSet = Collections
      .newSetFromMap(new ConcurrentHashMap<TezTaskContext, Boolean>());
  private final List<RequestorInfo> requestList;
  
  // Maybe make the reserve fraction configurable. Or scale it based on JVM heap.
  @VisibleForTesting
  static final float RESERVE_FRACTION_NO_PROCESSOR = 0.3f;
  @VisibleForTesting
  static final float RESERVE_FRACTION_WITH_PROCESSOR = 0.05f;

  /**
   * @param numInputs
   *          total number of Inputs for the task
   * @param numOutputs
   *          total number of Outputs for the task
   * @param conf
   *          Tez specific task configuration
   */
  public MemoryDistributor(int numTotalInputs, int numTotalOutputs, Configuration conf) {
    isEnabled = conf.getBoolean(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ENABLED,
        TezConfiguration.TEZ_TASK_SCALE_MEMORY_ENABLED_DEFAULT);
    if (conf.get(TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION) != null) {
      reserveFractionConfigured = true;
      reserveFraction = conf.getFloat(TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION,
          RESERVE_FRACTION_NO_PROCESSOR);
      Preconditions.checkArgument(reserveFraction >= 0.0f && reserveFraction <= 1.0f);
    } else {
      reserveFractionConfigured = false;
      reserveFraction = RESERVE_FRACTION_NO_PROCESSOR;
    }

    this.numTotalInputs = numTotalInputs;
    this.numTotalOutputs = numTotalOutputs;
    this.totalJvmMemory = Runtime.getRuntime().maxMemory();
    computeAssignableMemory();
    this.requestList = Collections.synchronizedList(new LinkedList<RequestorInfo>());
    LOG.info("InitialMemoryDistributor (isEnabled=" + isEnabled + ") invoked with: numInputs="
        + numTotalInputs + ", numOutputs=" + numTotalOutputs
        + ". Configuration: reserveFractionSpecified= " + reserveFractionConfigured
        + ", reserveFraction=" + reserveFraction + ", JVM.maxFree=" + totalJvmMemory
        + ", assignableMemory=" + totalAssignableMemory);
  }


  
  /**
   * Used by the Tez framework to request memory on behalf of user requests.
   */
  public void requestMemory(long requestSize, MemoryUpdateCallback callback,
      TezTaskContext taskContext, TezEntityDescriptor descriptor) {
    registerRequest(requestSize, callback, taskContext, descriptor);
  }
  
  /**
   * Used by the Tez framework to distribute initial memory after components
   * have made their initial requests.
   */
  public void makeInitialAllocations() {
    Preconditions.checkState(numInputsSeen.get() == numTotalInputs, "All inputs are expected to ask for memory");
    Preconditions.checkState(numOutputsSeen.get() == numTotalOutputs, "All outputs are expected to ask for memory");
    Iterable<RequestContext> requestContexts = Iterables.transform(requestList,
        new Function<RequestorInfo, RequestContext>() {
          public RequestContext apply(RequestorInfo requestInfo) {
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
      InitialMemoryAllocator allocator = new ScalingAllocator();
      allocations = allocator.assignMemory(totalAssignableMemory, numTotalInputs, numTotalOutputs,
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
    computeAssignableMemory();
  }
  
  private void computeAssignableMemory() {
    this.totalAssignableMemory = totalJvmMemory - ((long) (reserveFraction * totalJvmMemory));
  }

  private long registerRequest(long requestSize, MemoryUpdateCallback callback,
      TezTaskContext entityContext, TezEntityDescriptor descriptor) {
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
    if (!reserveFractionConfigured
        && requestInfo.getRequestContext().getComponentType() == RequestContext.ComponentType.PROCESSOR) {
      reserveFraction = RESERVE_FRACTION_WITH_PROCESSOR;
      computeAssignableMemory();
      LOG.info("Processor request for initial memory. Updating assignableMemory to : "
          + totalAssignableMemory);
    }
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
    Preconditions.checkState(totalAllocated <= totalAssignableMemory,
        "Total allocation should be <= availableMem. TotalAllocated: " + totalAllocated
            + ", totalAssignable: " + totalAssignableMemory);
  }

  /**
   * Used to balance memory requests before a task starts executing.
   */
  private static interface InitialMemoryAllocator {
    /**
     * @param availableForAllocation
     *          memory available for allocation
     * @param numTotalInputs
     *          number of inputs for the task
     * @param numTotalOutputs
     *          number of outputs for the tasks
     * @param requests
     *          Iterable view of requests received
     * @return list of allocations, one per request. This must be ordered in the
     *         same order of the requests.
     */
    Iterable<Long> assignMemory(long availableForAllocation, int numTotalInputs,
        int numTotalOutputs, Iterable<RequestContext> requests);
  }

  // Make this a public class if pulling the interface out.
  // Custom allocator based on The classes being used. Broadcast typically needs
  // a lot less than sort etc.
  private static class RequestContext {

    private static enum ComponentType {
      INPUT, OUTPUT, PROCESSOR
    }

    private long requestedSize;
    private String componentClassName;
    private ComponentType componentType;
    private String componentVertexName;

    public RequestContext(long requestedSize, String componentClassName,
        ComponentType componentType, String componentVertexName) {
      this.requestedSize = requestedSize;
      this.componentClassName = componentClassName;
      this.componentType = componentType;
      this.componentVertexName = componentVertexName;
    }

    public long getRequestedSize() {
      return requestedSize;
    }

    public String getComponentClassName() {
      return componentClassName;
    }

    public ComponentType getComponentType() {
      return componentType;
    }

    public String getComponentVertexName() {
      return componentVertexName;
    }
  }

  @Private
  private static class RequestorInfo {
    private final MemoryUpdateCallback callback;
    private final RequestContext requestContext;

    RequestorInfo(TezTaskContext taskContext, long requestSize,
        final MemoryUpdateCallback callback, TezEntityDescriptor descriptor) {
      RequestContext.ComponentType type;
      String componentVertexName;
      if (taskContext instanceof TezInputContext) {
        type = RequestContext.ComponentType.INPUT;
        componentVertexName = ((TezInputContext) taskContext).getSourceVertexName();
      } else if (taskContext instanceof TezOutputContext) {
        type = RequestContext.ComponentType.OUTPUT;
        componentVertexName = ((TezOutputContext) taskContext).getDestinationVertexName();
      } else if (taskContext instanceof TezProcessorContext) {
        type = RequestContext.ComponentType.PROCESSOR;
        componentVertexName = ((TezProcessorContext) taskContext).getTaskVertexName();
      } else {
        throw new IllegalArgumentException("Unknown type of entityContext: "
            + taskContext.getClass().getName());
      }
      this.requestContext = new RequestContext(requestSize, descriptor.getClassName(), type,
          componentVertexName);
      this.callback = callback;
      LOG.info("Received request: " + requestSize + ", type: " + type
          + ", componentVertexName: " + componentVertexName);
    }

    public MemoryUpdateCallback getCallback() {
      return callback;
    }

    public RequestContext getRequestContext() {
      return requestContext;
    }
  }

  private static class ScalingAllocator implements InitialMemoryAllocator {

    @Override
    public Iterable<Long> assignMemory(long availableForAllocation, int numTotalInputs,
        int numTotalOutputs, Iterable<RequestContext> requests) {
      int numRequests = 0;
      long totalRequested = 0;
      for (RequestContext context : requests) {
        totalRequested += context.getRequestedSize();
        numRequests++;
      }

      long totalJvmMem = Runtime.getRuntime().maxMemory();
      double ratio = totalRequested / (double) totalJvmMem;
      LOG.info("Scaling Requests. TotalRequested: " + totalRequested + ", TotalJVMMem: "
          + totalJvmMem + ", TotalAvailable: " + availableForAllocation
          + ", TotalRequested/TotalHeap:" + new DecimalFormat("0.00").format(ratio));

      if (totalRequested < availableForAllocation || totalRequested == 0) {
        // Not scaling up requests. Assuming things were setup correctly by
        // users in this case, keeping Processor, caching etc in mind.
        return Lists.newArrayList(Iterables.transform(requests,
            new Function<RequestContext, Long>() {
              public Long apply(RequestContext requestContext) {
                return requestContext.getRequestedSize();
              }
            }));
      }

      List<Long> allocations = Lists.newArrayListWithCapacity(numRequests);
      for (RequestContext request : requests) {
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
  }
}
