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

package org.apache.tez.runtime.library.resources;

import java.text.DecimalFormat;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.runtime.common.resources.InitialMemoryAllocator;
import org.apache.tez.runtime.common.resources.InitialMemoryRequestContext;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.input.ShuffledMergedInputLegacy;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Distributes memory between various requesting components by applying a
 * weighted scaling function. Overall, ensures that all requestors stay within the JVM limits.
 * 
 * Configuration involves specifying weights for the different Inputs available
 * in the tez-runtime-library. As an example, SortedShuffle : SortedOutput :
 * UnsortedShuffle could be configured to be 20:10:1. In this case, if both
 * SortedShuffle and UnsortedShuffle ask for the same amount of initial memory,
 * SortedShuffle will be given 20 times more; both may be scaled down to fit within the JVM though.
 * 
 */
public class WeightedScalingMemoryDistributor implements InitialMemoryAllocator {

  private static final Log LOG = LogFactory.getLog(WeightedScalingMemoryDistributor.class);

  @VisibleForTesting
  static final double DEFAULT_RESERVE_FRACTION = 0.25d;

  static final double MAX_ADDITIONAL_RESERVATION_FRACTION_PER_IO = 0.3d;
  static final double RESERVATION_FRACTION_PER_IO = 0.025d;

  private Configuration conf;

  public WeightedScalingMemoryDistributor() {
  }

  @Private
  @VisibleForTesting
  public enum RequestType {
    PARTITIONED_UNSORTED_OUTPUT, UNSORTED_INPUT, SORTED_OUTPUT, SORTED_MERGED_INPUT, PROCESSOR, OTHER
  };

  private EnumMap<RequestType, Integer> typeScaleMap = Maps.newEnumMap(RequestType.class);

  private int numRequests = 0;
  private int numRequestsScaled = 0;
  private long totalRequested = 0;

  private List<Request> requests = Lists.newArrayList();

  @Override
  public Iterable<Long> assignMemory(long availableForAllocation, int numTotalInputs,
      int numTotalOutputs, Iterable<InitialMemoryRequestContext> initialRequests) {

    // Read in configuration
    populateTypeScaleMap();

    for (InitialMemoryRequestContext context : initialRequests) {
      initialProcessMemoryRequestContext(context);
    }

    if (numRequestsScaled == 0) {
      // Fall back to regular scaling. e.g. BROADCAST : SHUFFLE = 0:1. 
      // i.e. if Shuffle present, Broadcast gets nothing, but otherwise it
      // should get an allocation
      numRequestsScaled = numRequests;
      for (Request request : requests) {
        request.requestWeight = 1;
      }
    }

    // Scale down while adding requests - don't want to hit Long limits.
    double totalScaledRequest = 0d;
    for (Request request : requests) {
      double requested = request.requestSize * (request.requestWeight / (double) numRequestsScaled);
      totalScaledRequest += requested;
    }

    // Take a certain amount of memory away for general usage.
    double reserveFraction = computeReservedFraction(numRequests);

    Preconditions.checkState(reserveFraction >= 0.0d && reserveFraction <= 1.0d);
    availableForAllocation = (long) (availableForAllocation - (reserveFraction * availableForAllocation));

    long totalJvmMem = Runtime.getRuntime().maxMemory();
    double ratio = totalRequested / (double) totalJvmMem;

    LOG.info("Scaling Requests. NumRequests: " + numRequests + ", numScaledRequests: "
        + numRequestsScaled + ", TotalRequested: " + totalRequested + ", TotalRequestedScaled: "
        + totalScaledRequest + ", TotalJVMHeap: " + totalJvmMem + ", TotalAvailable: "
        + availableForAllocation + ", TotalRequested/TotalJVMHeap:"
        + new DecimalFormat("0.00").format(ratio));

    // Actual scaling
    List<Long> allocations = Lists.newArrayListWithCapacity(numRequests);
    for (Request request : requests) {
      if (request.requestSize == 0) {
        allocations.add(0l);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scaling requested " + request.componentClassname + " of type "
              + request.requestType + " 0 to allocated: 0");
        }
      } else {
        double requestFactor = request.requestWeight / (double) numRequestsScaled;
        double scaledRequest = requestFactor * request.requestSize;
        long allocated = Math.min(
            (long) ((scaledRequest / totalScaledRequest) * availableForAllocation),
            request.requestSize);
        // TODO Later - If requestedSize is used, the difference (allocated -
        // requestedSize) could be allocated to others.
        allocations.add(allocated);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scaling requested " + request.componentClassname + " of type "
              + request.requestType + " " + request.requestSize + "  to allocated: " + allocated);
        }
      }
    }
    return allocations;

  }

  private void initialProcessMemoryRequestContext(InitialMemoryRequestContext context) {
    RequestType requestType;
    numRequests++;
    totalRequested += context.getRequestedSize();
    String className = context.getComponentClassName();
    requestType = getRequestTypeForClass(className);
    Integer typeScaleFactor = getScaleFactorForType(requestType);

    Request request = new Request(context.getComponentClassName(), context.getRequestedSize(),
        requestType, typeScaleFactor);
    requests.add(request);
    LOG.info("ScaleFactor: " + typeScaleFactor + ", for type: " + requestType);
    numRequestsScaled += typeScaleFactor;
  }

  private Integer getScaleFactorForType(RequestType requestType) {
    Integer typeScaleFactor = typeScaleMap.get(requestType);
    if (typeScaleFactor == null) {
      LOG.warn("Bad scale factor for requestType: " + requestType + ", Using factor 0");
      typeScaleFactor = 0;
    }
    return typeScaleFactor;
  }

  private RequestType getRequestTypeForClass(String className) {
    RequestType requestType;
    if (className.equals(OnFileSortedOutput.class.getName())) {
      requestType = RequestType.SORTED_OUTPUT;
    } else if (className.equals(ShuffledMergedInput.class.getName())
        || className.equals(ShuffledMergedInputLegacy.class.getName())) {
      requestType = RequestType.SORTED_MERGED_INPUT;
    } else if (className.equals(ShuffledUnorderedKVInput.class.getName())) {
      requestType = RequestType.UNSORTED_INPUT;
    } else {
      requestType = RequestType.OTHER;
      LOG.info("Falling back to RequestType.OTHER for class: " + className);
    }
    return requestType;
  }

  private void populateTypeScaleMap() {
    String[] ratios = conf.getStrings(TezJobConfig.TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS);
    int numExpectedValues = RequestType.values().length;
    if (ratios == null) {
      LOG.info("No ratio specified. Falling back to Linear scaling");
      ratios = new String[numExpectedValues];
      int i = 0;
      for (RequestType requestType : RequestType.values()) {
        ratios[i] = requestType.name() + ":1"; // Linear scale
        i++;
      }
    } else {
      if (ratios.length != RequestType.values().length) {
        throw new IllegalArgumentException(
            "Number of entries in the configured ratios should be equal to the number of entries in RequestType: "
                + numExpectedValues);
      }
    }

    Set<RequestType> seenTypes = new HashSet<RequestType>();

    for (String ratio : ratios) {
      String[] parts = ratio.split(":");
      Preconditions.checkState(parts.length == 2);
      RequestType requestType = RequestType.valueOf(parts[0]);
      Integer ratioVal = Integer.parseInt(parts[1]);
      if (!seenTypes.add(requestType)) {
        throw new IllegalArgumentException("Cannot configure the same RequestType: " + requestType
            + " multiple times");
      }
      Preconditions.checkState(ratioVal >= 0, "Ratio must be >= 0");
      typeScaleMap.put(requestType, ratioVal);
    }
  }

  private double computeReservedFraction(int numTotalRequests) {

    double reserveFractionPerIo = conf.getDouble(
        TezJobConfig.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO,
        RESERVATION_FRACTION_PER_IO);
    double maxAdditionalReserveFraction = conf.getDouble(
        TezJobConfig.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_MAX,
        MAX_ADDITIONAL_RESERVATION_FRACTION_PER_IO);
    Preconditions.checkArgument(maxAdditionalReserveFraction >= 0f
        && maxAdditionalReserveFraction <= 1f);
    Preconditions.checkArgument(reserveFractionPerIo <= maxAdditionalReserveFraction
        && reserveFractionPerIo >= 0f);
    if (LOG.isDebugEnabled()) {
      LOG.debug("ReservationFractionPerIO=" + reserveFractionPerIo + ", MaxPerIOReserveFraction="
          + maxAdditionalReserveFraction);
    }

    double initialReserveFraction = conf.getDouble(TezJobConfig.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION,
        DEFAULT_RESERVE_FRACTION);
    double additionalReserveFraction = Math.min(maxAdditionalReserveFraction, numTotalRequests
        * reserveFractionPerIo);

    double reserveFraction = initialReserveFraction + additionalReserveFraction;
    Preconditions.checkState(reserveFraction <= 1.0d);
    LOG.info("InitialReservationFraction=" + initialReserveFraction
        + ", AdditionalReservationFractionForIOs=" + additionalReserveFraction
        + ", finalReserveFractionUsed=" + reserveFraction);
    return reserveFraction;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  private static class Request {
    Request(String componentClassname, long requestSize, RequestType requestType, int requestWeight) {
      this.componentClassname = componentClassname;
      this.requestSize = requestSize;
      this.requestType = requestType;
      this.requestWeight = requestWeight;
    }

    String componentClassname;
    long requestSize;
    private RequestType requestType;
    private int requestWeight;
  }
}