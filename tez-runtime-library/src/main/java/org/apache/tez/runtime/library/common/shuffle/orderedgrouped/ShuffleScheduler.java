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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.Type;

import com.google.common.collect.Lists;

class ShuffleScheduler {
  static ThreadLocal<Long> shuffleStart = new ThreadLocal<Long>() {
    protected Long initialValue() {
      return 0L;
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleScheduler.class);
  static final long INITIAL_PENALTY = 2000L; // 2 seconds
  private static final float PENALTY_GROWTH_RATE = 1.3f;

  private boolean[] finishedMaps;
  private final int numInputs;
  private final String srcNameTrimmed;
  private int numFetchedSpills;
  private Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
  @VisibleForTesting
  final ConcurrentMap<String, InputAttemptIdentifier> pathToIdentifierMap
      = new ConcurrentHashMap<String, InputAttemptIdentifier>();

  //To track shuffleInfo events when finalMerge is disabled in source or pipelined shuffle is
  // enabled in source.
  @VisibleForTesting
  final Map<InputIdentifier, ShuffleEventInfo> pipelinedShuffleInfoEventsMap;

  @VisibleForTesting
  final Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private Set<InputAttemptIdentifier> obsoleteInputs = new HashSet<InputAttemptIdentifier>();
  
  private final Random random = new Random(System.currentTimeMillis());
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  private final Referee referee;
  @VisibleForTesting
  final Map<InputAttemptIdentifier, IntWritable> failureCounts = new HashMap<InputAttemptIdentifier,IntWritable>();
  final Set<String> uniqueHosts = Sets.newHashSet();
  private final Map<String,IntWritable> hostFailures = 
    new HashMap<String,IntWritable>();
  private final InputContext inputContext;
  private final Shuffle shuffle;
  private final TezCounter shuffledInputsCounter;
  private final TezCounter skippedInputCounter;
  private final TezCounter reduceShuffleBytes;
  private final TezCounter reduceBytesDecompressed;
  @VisibleForTesting
  final TezCounter failedShuffleCounter;
  private final TezCounter bytesShuffledToDisk;
  private final TezCounter bytesShuffledToDiskDirect;
  private final TezCounter bytesShuffledToMem;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;

  @VisibleForTesting
  final AtomicInteger remainingMaps;
  private final long startTime;
  @VisibleForTesting
  long lastProgressTime;
  @VisibleForTesting
  long failedShufflesSinceLastCompletion;

  private int maxTaskOutputAtOnce;
  private int maxFetchFailuresBeforeReporting;
  private boolean reportReadErrorImmediately = true; 
  private int maxFailedUniqueFetches = 5;
  private final int abortFailureLimit;

  private final int minFailurePerHost;
  private final float hostFailureFraction;
  private final float maxStallTimeFraction;
  private final float minReqProgressFraction;
  private final float maxAllowedFailedFetchFraction;
  private final boolean checkFailedFetchSinceLastCompletion;

  private long totalBytesShuffledTillNow = 0;
  private final DecimalFormat  mbpsFormat = new DecimalFormat("0.00");

  public ShuffleScheduler(InputContext inputContext,
                          Configuration conf,
                          int numberOfInputs,
                          Shuffle shuffle,
                          TezCounter shuffledInputsCounter,
                          TezCounter reduceShuffleBytes,
                          TezCounter reduceBytesDecompressed,
                          TezCounter failedShuffleCounter,
                          TezCounter bytesShuffledToDisk,
                          TezCounter bytesShuffledToDiskDirect,
                          TezCounter bytesShuffledToMem, long startTime,
                          String srcNameTrimmed) {
    this.inputContext = inputContext;
    this.numInputs = numberOfInputs;
    int abortFailureLimitConf = conf.getInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_SHUFFLE_SOURCE_ATTEMPT_ABORT_LIMIT, TezRuntimeConfiguration
        .TEZ_RUNTIME_SHUFFLE_SOURCE_ATTEMPT_ABORT_LIMIT_DEFAULT);
    if (abortFailureLimitConf <= -1) {
      abortFailureLimit = Math.max(15, numberOfInputs / 10);
    } else {
      //No upper cap, as user is setting this intentionally
      abortFailureLimit = abortFailureLimitConf;
    }
    remainingMaps = new AtomicInteger(numberOfInputs);
    finishedMaps = new boolean[remainingMaps.get()]; // default init to false

    this.minFailurePerHost = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST_DEFAULT);
    Preconditions.checkArgument(minFailurePerHost >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST
            + "=" + minFailurePerHost + " should not be negative");

    this.hostFailureFraction = conf.getFloat(TezRuntimeConfiguration
            .TEZ_RUNTIME_SHUFFLE_ACCEPTABLE_HOST_FETCH_FAILURE_FRACTION,
        TezRuntimeConfiguration
            .TEZ_RUNTIME_SHUFFLE_ACCEPTABLE_HOST_FETCH_FAILURE_FRACTION_DEFAULT);

    this.maxStallTimeFraction = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_STALL_TIME_FRACTION,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_STALL_TIME_FRACTION_DEFAULT);
    Preconditions.checkArgument(maxStallTimeFraction >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_STALL_TIME_FRACTION
            + "=" + maxStallTimeFraction + " should not be negative");

    this.minReqProgressFraction = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_REQUIRED_PROGRESS_FRACTION,
        TezRuntimeConfiguration
            .TEZ_RUNTIME_SHUFFLE_MIN_REQUIRED_PROGRESS_FRACTION_DEFAULT);
    Preconditions.checkArgument(minReqProgressFraction >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_REQUIRED_PROGRESS_FRACTION
            + "=" + minReqProgressFraction + " should not be negative");

    this.maxAllowedFailedFetchFraction = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_ALLOWED_FAILED_FETCH_ATTEMPT_FRACTION,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_ALLOWED_FAILED_FETCH_ATTEMPT_FRACTION_DEFAULT);
    Preconditions.checkArgument(maxAllowedFailedFetchFraction >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_ALLOWED_FAILED_FETCH_ATTEMPT_FRACTION
            + "=" + maxAllowedFailedFetchFraction + " should not be negative");

    this.checkFailedFetchSinceLastCompletion = conf.getBoolean
        (TezRuntimeConfiguration
                .TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION,
            TezRuntimeConfiguration
                .TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION_DEFAULT);

    this.srcNameTrimmed = srcNameTrimmed;
    this.referee = new Referee();
    this.shuffle = shuffle;
    this.shuffledInputsCounter = shuffledInputsCounter;
    this.reduceShuffleBytes = reduceShuffleBytes;
    this.reduceBytesDecompressed = reduceBytesDecompressed;
    this.failedShuffleCounter = failedShuffleCounter;
    this.bytesShuffledToDisk = bytesShuffledToDisk;
    this.bytesShuffledToDiskDirect = bytesShuffledToDiskDirect;
    this.bytesShuffledToMem = bytesShuffledToMem;
    this.startTime = startTime;
    this.lastProgressTime = startTime;

    this.maxFailedUniqueFetches = Math.min(numberOfInputs,
        this.maxFailedUniqueFetches);
    referee.start();
    this.maxFetchFailuresBeforeReporting = 
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT_DEFAULT);
    this.reportReadErrorImmediately = 
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR, 
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR_DEFAULT);
    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would
     * be approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce = Math.max(1, Math.min(75, conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));
    
    this.skippedInputCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SKIPPED_INPUTS);
    this.firstEventReceived = inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived = inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);

    pipelinedShuffleInfoEventsMap = new HashMap<InputIdentifier, ShuffleEventInfo>();
    LOG.info("ShuffleScheduler running for sourceVertex: "
        + inputContext.getSourceVertexName() + " with configuration: "
        + "maxFetchFailuresBeforeReporting=" + maxFetchFailuresBeforeReporting
        + ", reportReadErrorImmediately=" + reportReadErrorImmediately
        + ", maxFailedUniqueFetches=" + maxFailedUniqueFetches
        + ", abortFailureLimit=" + abortFailureLimit
        + ", hostFailureFraction=" + hostFailureFraction
        + ", minFailurePerHost=" + minFailurePerHost
        + ", maxAllowedFailedFetchFraction=" + maxAllowedFailedFetchFraction
        + ", maxStallTimeFraction=" + maxStallTimeFraction
        + ", minReqProgressFraction=" + minReqProgressFraction
        + ", checkFailedFetchSinceLastCompletion=" + checkFailedFetchSinceLastCompletion
        + ", maxTaskOutputAtOnce=" + maxTaskOutputAtOnce);
  }

  protected synchronized  void updateEventReceivedTime() {
    long relativeTime = System.currentTimeMillis() - startTime;
    if (firstEventReceived.getValue() == 0) {
      firstEventReceived.setValue(relativeTime);
      lastEventReceived.setValue(relativeTime);
      return;
    }
    lastEventReceived.setValue(relativeTime);
  }

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same
   * attempt.
   */
  static class ShuffleEventInfo {
    BitSet eventsProcessed;
    int finalEventId = -1; //0 indexed
    int attemptNum;
    String id;


    ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier().getInputIndex() + "_" + input.getAttemptNumber();
      this.eventsProcessed = new BitSet();
      this.attemptNum = input.getAttemptNumber();
    }

    void spillProcessed(int spillId) {
      if (finalEventId != -1) {
        Preconditions.checkState(eventsProcessed.cardinality() <= (finalEventId + 1),
            "Wrong state. eventsProcessed cardinality=" + eventsProcessed.cardinality() + " "
                + "finalEventId=" + finalEventId + ", spillId=" + spillId + ", " + toString());
      }
      eventsProcessed.set(spillId);
    }

    void setFinalEventId(int spillId) {
      finalEventId = spillId;
    }

    boolean isDone() {
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    public String toString() {
      return "[eventsProcessed=" + eventsProcessed + ", finalEventId=" + finalEventId
          +  ", id=" + id + ", attemptNum=" + attemptNum + "]";
    }
  }

  public synchronized void copySucceeded(InputAttemptIdentifier srcAttemptIdentifier,
                                         MapHost host,
                                         long bytesCompressed,
                                         long bytesDecompressed,
                                         long millis,
                                         MapOutput output,
                                         boolean isLocalFetch
                                         ) throws IOException {

    if (!isInputFinished(srcAttemptIdentifier.getInputIdentifier().getInputIndex())) {
      if (!isLocalFetch) {
        /**
         * Reset it only when it is a non-local-disk copy.
         */
        failedShufflesSinceLastCompletion = 0;
      }
      if (output != null) {

        failureCounts.remove(srcAttemptIdentifier);
        if (host != null) {
          hostFailures.remove(host.getHostIdentifier());
        }

        output.commit();
        ShuffleUtils.logIndividualFetchComplete(LOG, millis, bytesCompressed,
            bytesDecompressed, output.getType().toString(), srcAttemptIdentifier);
        if (output.getType() == Type.DISK) {
          bytesShuffledToDisk.increment(bytesCompressed);
        } else if (output.getType() == Type.DISK_DIRECT) {
          bytesShuffledToDiskDirect.increment(bytesCompressed);
        } else {
          bytesShuffledToMem.increment(bytesCompressed);
        }
        shuffledInputsCounter.increment(1);
      } else {
        // Output null implies that a physical input completion is being
        // registered without needing to fetch data
        skippedInputCounter.increment(1);
      }

      /**
       * In case of pipelined shuffle, it is quite possible that fetchers pulled the FINAL_UPDATE
       * spill in advance due to smaller output size.  In such scenarios, we need to wait until
       * we retrieve all spill details to claim success.
       */
      if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
        remainingMaps.decrementAndGet();
        setInputFinished(srcAttemptIdentifier.getInputIdentifier().getInputIndex());
        numFetchedSpills++;
      } else {
        InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
        //Allow only one task attempt to proceed.
        if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
          return;
        }

        ShuffleEventInfo eventInfo = pipelinedShuffleInfoEventsMap.get(inputIdentifier);

        //Possible that Shuffle event handler invoked this, due to empty partitions
        if (eventInfo == null && output == null) {
          eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
          pipelinedShuffleInfoEventsMap.put(inputIdentifier, eventInfo);
        }

        assert(eventInfo != null);
        eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
        numFetchedSpills++;

        if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
          eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
        }

        //check if we downloaded all spills pertaining to this InputAttemptIdentifier
        if (eventInfo.isDone()) {
          remainingMaps.decrementAndGet();
          setInputFinished(inputIdentifier.getInputIndex());
          pipelinedShuffleInfoEventsMap.remove(inputIdentifier);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Removing : " + srcAttemptIdentifier + ", pending: " +
                pipelinedShuffleInfoEventsMap);
          }
        }

        if (LOG.isTraceEnabled()) {
          LOG.trace("eventInfo " + eventInfo.toString());
        }
      }

      if (remainingMaps.get() == 0) {
        LOG.info(srcNameTrimmed + ": " + "All inputs fetched for input vertex : " + inputContext.getSourceVertexName());
        notifyAll();
      }

      // update the status
      lastProgressTime = System.currentTimeMillis();
      totalBytesShuffledTillNow += bytesCompressed;
      logProgress();
      reduceShuffleBytes.increment(bytesCompressed);
      reduceBytesDecompressed.increment(bytesDecompressed);
      if (LOG.isDebugEnabled()) {
        LOG.debug("src task: "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
                inputContext.getSourceVertexName(), srcAttemptIdentifier.getInputIdentifier().getInputIndex(),
                srcAttemptIdentifier.getAttemptNumber()) + " done");
      }
    } else {
      // input is already finished. duplicate fetch.
      LOG.warn(srcNameTrimmed + ": Duplicate fetch of input "
          + "no longer needs to be fetched: " + srcAttemptIdentifier);
      // free the resource - specially memory
      
      // If the src does not generate data, output will be null.
      if (output != null) {
        output.abort();
      }
    }
    // TODO NEWTEZ Should this be releasing the output, if not committed ? Possible memory leak in case of speculation.
  }

  private boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    //For pipelined shuffle.
    //TODO: TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      ShuffleEventInfo eventInfo = pipelinedShuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        reportExceptionForInput(new IOException("Previous event already got scheduled for " +
            input + ". Previous attempt's data could have been already merged "
            + "to memory/disk outputs.  Failing the fetch early. currentAttemptNum="
            + eventInfo.attemptNum + ", eventsProcessed=" + eventInfo.eventsProcessed
            + ", newAttemptNum=" + input.getAttemptNumber()));
        return false;
      }

      if (eventInfo == null) {
        pipelinedShuffleInfoEventsMap.put(input.getInputIdentifier(), new ShuffleEventInfo(input));
      }
    }
    return true;
  }

  @VisibleForTesting
  void reportExceptionForInput(Exception exception) {
    LOG.error(srcNameTrimmed + ": " + "Reporting exception for input", exception);
    shuffle.reportException(exception);
  }

  private final AtomicInteger nextProgressLineEventCount = new AtomicInteger(0);

  private void logProgress() {
    int inputsDone = numInputs - remainingMaps.get();
    if (inputsDone > nextProgressLineEventCount.get() || inputsDone == numInputs) {
      nextProgressLineEventCount.addAndGet(50);
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

      double transferRate = mbs / secsSinceStart;
      LOG.info("copy(" + inputsDone + " (spillsFetched=" + numFetchedSpills + ") of " + numInputs +
          ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) "
          + mbpsFormat.format(transferRate) + " MB/s)");
    }
  }

  public synchronized void copyFailed(InputAttemptIdentifier srcAttempt,
                                      MapHost host,
                                      boolean readError,
                                      boolean connectError,
                                      boolean isLocalFetch
                                      ) {
    failedShuffleCounter.increment(1);

    int failures = incrementAndGetFailureAttempt(srcAttempt);

    if (!isLocalFetch) {
      /**
       * Track the number of failures that has happened since last completion.
       * This gets reset on a successful copy.
       */
      failedShufflesSinceLastCompletion++;
    }

    /**
     * Inform AM:
     *    - In case of read/connect error
     *    - In case attempt failures exceed threshold of
     *    maxFetchFailuresBeforeReporting (5)
     * Bail-out if needed:
     *    - Check whether individual attempt crossed failure threshold limits
     *    - Check overall shuffle health. Bail out if needed.*
     */

    //TEZ-2890
    boolean shouldInformAM =
        (reportReadErrorImmediately && (readError || connectError))
            || ((failures % maxFetchFailuresBeforeReporting) == 0);

    if (shouldInformAM) {
      //Inform AM. In case producer needs to be restarted, it is handled at AM.
      informAM(srcAttempt);
    }

    //Restart consumer in case shuffle is not healthy
    if (!isShuffleHealthy(srcAttempt)) {
      return;
    }

    penalizeHost(host, failures);
  }

  private boolean isAbortLimitExceeedFor(InputAttemptIdentifier srcAttempt) {
    int attemptFailures = getFailureCount(srcAttempt);
    if (attemptFailures >= abortFailureLimit) {
      // This task has seen too many fetch failures - report it as failed. The
      // AM may retry it if max failures has not been reached.

      // Between the task and the AM - someone needs to determine who is at
      // fault. If there's enough errors seen on the task, before the AM informs
      // it about source failure, the task considers itself to have failed and
      // allows the AM to re-schedule it.
      String errorMsg = "Failed " + attemptFailures + " times trying to "
          + "download from " + TezRuntimeUtils.getTaskAttemptIdentifier(
          inputContext.getSourceVertexName(),
          srcAttempt.getInputIdentifier().getInputIndex(),
          srcAttempt.getAttemptNumber()) + ". threshold=" + abortFailureLimit;
      IOException ioe = new IOException(errorMsg);
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
      shuffle.reportException(ioe);
      return true;
    }
    return false;
  }

  private void penalizeHost(MapHost host, int failures) {
    host.penalize();

    String hostPort = host.getHostIdentifier();
    // TODO TEZ-922 hostFailures isn't really used for anything apart from
    // hasFailedAcrossNodes().Factor it into error
    // reporting / potential blacklisting of hosts.
    if (hostFailures.containsKey(hostPort)) {
      IntWritable x = hostFailures.get(hostPort);
      x.set(x.get() + 1);
    } else {
      hostFailures.put(hostPort, new IntWritable(1));
    }

    long delay = (long) (INITIAL_PENALTY *
        Math.pow(PENALTY_GROWTH_RATE, failures));
    penalties.add(new Penalty(host, delay));
  }

  private int getFailureCount(InputAttemptIdentifier srcAttempt) {
    IntWritable failureCount = failureCounts.get(srcAttempt);
    return (failureCount == null) ? 0 : failureCount.get();
  }

  private int incrementAndGetFailureAttempt(InputAttemptIdentifier srcAttempt) {
    int failures = 1;
    if (failureCounts.containsKey(srcAttempt)) {
      IntWritable x = failureCounts.get(srcAttempt);
      x.set(x.get() + 1);
      failures = x.get();
    } else {
      failureCounts.put(srcAttempt, new IntWritable(1));
    }
    return failures;
  }

  public void reportLocalError(IOException ioe) {
    LOG.error(srcNameTrimmed + ": " + "Shuffle failed : caused by local error",
        ioe);
    // Shuffle knows how to deal with failures post shutdown via the onFailure hook
    shuffle.reportException(ioe);
  }

  // Notify AM
  private void informAM(InputAttemptIdentifier srcAttempt) {
    LOG.info(
        srcNameTrimmed + ": " + "Reporting fetch failure for InputIdentifier: "
            + srcAttempt + " taskAttemptIdentifier: " + TezRuntimeUtils
            .getTaskAttemptIdentifier(inputContext.getSourceVertexName(),
                srcAttempt.getInputIdentifier().getInputIndex(),
                srcAttempt.getAttemptNumber()) + " to AM.");
    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(InputReadErrorEvent.create(
        "Fetch failure for " + TezRuntimeUtils
            .getTaskAttemptIdentifier(inputContext.getSourceVertexName(),
                srcAttempt.getInputIdentifier().getInputIndex(),
                srcAttempt.getAttemptNumber()) + " to jobtracker.",
        srcAttempt.getInputIdentifier().getInputIndex(),
        srcAttempt.getAttemptNumber()));

    inputContext.sendEvents(failedEvents);
  }

  /**
   * To determine if failures happened across nodes or not. This will help in
   * determining whether this task needs to be restarted or source needs to
   * be restarted.
   *
   * @param logContext context info for logging
   * @return boolean true indicates this task needs to be restarted
   */
  private boolean hasFailedAcrossNodes(String logContext) {
    int numUniqueHosts = uniqueHosts.size();
    Preconditions.checkArgument(numUniqueHosts > 0, "No values in unique hosts");
    int threshold = Math.max(3,
        (int) Math.ceil(numUniqueHosts * hostFailureFraction));
    int total = 0;
    boolean failedAcrossNodes = false;
    for(String host : uniqueHosts) {
      IntWritable failures = hostFailures.get(host);
      if (failures != null && failures.get() > minFailurePerHost) {
        total++;
        failedAcrossNodes = (total > (threshold * minFailurePerHost));
        if (failedAcrossNodes) {
          break;
        }
      }
    }

    LOG.info(logContext + ", numUniqueHosts=" + numUniqueHosts
        + ", hostFailureThreshold=" + threshold
        + ", hostFailuresCount=" + hostFailures.size()
        + ", hosts crossing threshold=" + total
        + ", reducerFetchIssues=" + failedAcrossNodes
    );

    return failedAcrossNodes;
  }

  private boolean allEventsReceived() {
    if (!pipelinedShuffleInfoEventsMap.isEmpty()) {
      return (pipelinedShuffleInfoEventsMap.size() == numInputs);
    } else {
      //no pipelining
      return ((pathToIdentifierMap.size() + skippedInputCounter.getValue())
          == numInputs);
    }
  }

  /**
   * Check if consumer needs to be restarted based on total failures w.r.t
   * completed outputs and based on number of errors that have happened since
   * last successful completion. Consider into account whether failures have
   * been seen across different nodes.
   *
   * @return true to indicate fetchers are healthy
   */
  private boolean isFetcherHealthy(String logContext) {

    long totalFailures = failedShuffleCounter.getValue();
    int doneMaps = numInputs - remainingMaps.get();

    boolean fetcherHealthy = true;
    if (doneMaps > 0) {
      fetcherHealthy = (((float) totalFailures / (totalFailures + doneMaps))
          < maxAllowedFailedFetchFraction);
    }

    if (fetcherHealthy) {
      //Compute this logic only when all events are received
      if (allEventsReceived()) {
        if (hostFailureFraction > 0) {
          boolean failedAcrossNodes = hasFailedAcrossNodes(logContext);
          if (failedAcrossNodes) {
            return false; //not healthy
          }
        }

        if (checkFailedFetchSinceLastCompletion) {
          /**
           * remainingMaps works better instead of pendingHosts in the
           * following condition because of the way the fetcher reports failures
           */
          if (failedShufflesSinceLastCompletion >=
              remainingMaps.get() * minFailurePerHost) {
            /**
             * Check if lots of errors are seen after last progress time.
             *
             * E.g totalFailures = 20. doneMaps = 320 - 300;
             * fetcherHealthy = (20/(20+300)) < 0.5.  So reducer would be marked as healthy.
             * Assume 20 errors happen when downloading the last 20 attempts. Host failure & individual
             * attempt failures would keep increasing; but at very slow rate 15 * 180 seconds per
             * attempt to find out the issue.
             *
             * Instead consider the new errors with the pending items to be fetched.
             * Assume 21 new errors happened after last progress; remainingMaps = (320-300) = 20;
             * (21 / (21 + 20)) > 0.5
             * So we reset the reducer to unhealthy here (special case)
             *
             * In normal conditions (i.e happy path), this wouldn't even cause any issue as
             * failedShufflesSinceLastCompletion is reset as soon as we see successful download.
             */

            fetcherHealthy =
                (((float) failedShufflesSinceLastCompletion / (
                    failedShufflesSinceLastCompletion + remainingMaps.get()))
                    < maxAllowedFailedFetchFraction);

            LOG.info(logContext + ", fetcherHealthy=" + fetcherHealthy
                + ", failedShufflesSinceLastCompletion="
                + failedShufflesSinceLastCompletion
                + ", remainingMaps=" + remainingMaps.get()
            );
          }
        }
      }
    }
    return fetcherHealthy;
  }

  boolean isShuffleHealthy(InputAttemptIdentifier srcAttempt) {

    if (isAbortLimitExceeedFor(srcAttempt)) {
      return false;
    }

    final float MIN_REQUIRED_PROGRESS_PERCENT = minReqProgressFraction;
    final float MAX_ALLOWED_STALL_TIME_PERCENT = maxStallTimeFraction;

    int doneMaps = numInputs - remainingMaps.get();

    String logContext = "srcAttempt=" + srcAttempt.toString();
    boolean fetcherHealthy = isFetcherHealthy(logContext);

    // check if the reducer has progressed enough
    boolean reducerProgressedEnough =
      (((float)doneMaps / numInputs)
          >= MIN_REQUIRED_PROGRESS_PERCENT);

    // check if the reducer is stalled for a long time
    // duration for which the reducer is stalled
    int stallDuration =
      (int)(System.currentTimeMillis() - lastProgressTime);
    
    // duration for which the reducer ran with progress
    int shuffleProgressDuration =
      (int)(lastProgressTime - startTime);

    boolean reducerStalled = (shuffleProgressDuration > 0) &&
        (((float)stallDuration / shuffleProgressDuration)
            >= MAX_ALLOWED_STALL_TIME_PERCENT);

    // kill if not healthy and has insufficient progress
    if ((failureCounts.size() >= maxFailedUniqueFetches ||
        failureCounts.size() == (numInputs - doneMaps))
        && !fetcherHealthy
        && (!reducerProgressedEnough || reducerStalled)) {
      String errorMsg = (srcNameTrimmed + ": "
          + "Shuffle failed with too many fetch failures and insufficient progress!"
          + "failureCounts=" + failureCounts.size()
          + ", pendingInputs=" + (numInputs - doneMaps)
          + ", fetcherHealthy=" + fetcherHealthy
          + ", reducerProgressedEnough=" + reducerProgressedEnough
          + ", reducerStalled=" + reducerStalled);
      LOG.error(errorMsg);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Host failures=" + hostFailures.keySet());
      }
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
      shuffle.reportException(new IOException(errorMsg));
      return false;
    }
    return true;
  }

  public synchronized void addKnownMapOutput(String inputHostName,
                                             int port,
                                             int partitionId,
                                             String hostUrl,
                                             InputAttemptIdentifier srcAttempt) {
    String hostPort = (inputHostName + ":" + String.valueOf(port));
    uniqueHosts.add(hostPort);
    String identifier = MapHost.createIdentifier(hostPort, partitionId);


    MapHost host = mapLocations.get(identifier);
    if (host == null) {
      host = new MapHost(partitionId, hostPort, hostUrl);
      assert identifier.equals(host.getIdentifier());
      mapLocations.put(identifier, host);
    }

    //Allow only one task attempt to proceed.
    if (!validateInputAttemptForPipelinedShuffle(srcAttempt)) {
      return;
    }

    host.addKnownMap(srcAttempt);
    pathToIdentifierMap.put(
        getIdentifierFromPathAndReduceId(srcAttempt.getPathComponent(),
            partitionId), srcAttempt);

    // Mark the host as pending
    if (host.getState() == MapHost.State.PENDING) {
      pendingHosts.add(host);
      notifyAll();
    }
  }
  
  public synchronized void obsoleteInput(InputAttemptIdentifier srcAttempt) {
    // The incoming srcAttempt does not contain a path component.
    LOG.info(srcNameTrimmed + ": " + "Adding obsolete input: " + srcAttempt);
    if (pipelinedShuffleInfoEventsMap.containsKey(srcAttempt.getInputIdentifier())) {
      //Pipelined shuffle case (where pipelinedShuffleInfoEventsMap gets populated).
      //Fail fast here.
      shuffle.reportException(new IOException(srcAttempt + " is marked as obsoleteInput, but it "
          + "exists in shuffleInfoEventMap. Some data could have been already merged "
          + "to memory/disk outputs.  Failing the fetch early."));
      return;
    }
    obsoleteInputs.add(srcAttempt);
  }
  
  public synchronized void putBackKnownMapOutput(MapHost host,
                                                 InputAttemptIdentifier srcAttempt) {
    host.addKnownMap(srcAttempt);
  }

  public synchronized MapHost getHost() throws InterruptedException {
      while(pendingHosts.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("PendingHosts=" + pendingHosts);
        }
        wait();
      }
      
      MapHost host = null;
      Iterator<MapHost> iter = pendingHosts.iterator();
      int numToPick = random.nextInt(pendingHosts.size());
      for (int i=0; i <= numToPick; ++i) {
        host = iter.next();
      }
      
      pendingHosts.remove(host);     
      host.markBusy();
      if (LOG.isDebugEnabled()) {
        LOG.debug(srcNameTrimmed + ": " + "Assigning " + host + " with " + host.getNumKnownMapOutputs() +
            " to " + Thread.currentThread().getName());
      }
      shuffleStart.set(System.currentTimeMillis());
      
      return host;
  }
  
  public InputAttemptIdentifier getIdentifierForFetchedOutput(
      String path, int reduceId) {
    return pathToIdentifierMap.get(getIdentifierFromPathAndReduceId(path, reduceId));
  }
  
  private boolean inputShouldBeConsumed(InputAttemptIdentifier id) {
    return (!obsoleteInputs.contains(id) && 
             !isInputFinished(id.getInputIdentifier().getInputIndex()));
  }

  public synchronized List<InputAttemptIdentifier> getMapsForHost(MapHost host) {
    List<InputAttemptIdentifier> origList = host.getAndClearKnownMaps();

    ListMultimap<Integer, InputAttemptIdentifier> dedupedList = LinkedListMultimap.create();

    Iterator<InputAttemptIdentifier> listItr = origList.iterator();
    while (listItr.hasNext()) {
      // we may want to try all versions of the input but with current retry
      // behavior older ones are likely to be lost and should be ignored.
      // This may be removed after TEZ-914
      InputAttemptIdentifier id = listItr.next();
      if (inputShouldBeConsumed(id)) {
        Integer inputNumber = Integer.valueOf(id.getInputIdentifier().getInputIndex());
        List<InputAttemptIdentifier> oldIdList = dedupedList.get(inputNumber);

        if (oldIdList == null || oldIdList.isEmpty()) {
          dedupedList.put(inputNumber, id);
          continue;
        }

        //In case of pipelined shuffle, we can have multiple spills. In such cases, we can have
        // more than one item in the oldIdList.
        boolean addIdentifierToList = false;
        Iterator<InputAttemptIdentifier> oldIdIterator = oldIdList.iterator();
        while(oldIdIterator.hasNext()) {
          InputAttemptIdentifier oldId = oldIdIterator.next();

          //no need to add if spill ids are same
          if (id.canRetrieveInputInChunks()) {
            if (oldId.getSpillEventId() == id.getSpillEventId()) {
              //TODO: need to handle deterministic spills later.
              addIdentifierToList = false;
              continue;
            } else if (oldId.getAttemptNumber() == id.getAttemptNumber()) {
              //but with different spill id.
              addIdentifierToList = true;
              break;
            }
          }

          //if its from different attempt, take the latest attempt
          if (oldId.getAttemptNumber() < id.getAttemptNumber()) {
            //remove existing identifier
            oldIdIterator.remove();
            LOG.warn("Old Src for InputIndex: " + inputNumber + " with attemptNumber: "
                + oldId.getAttemptNumber()
                + " was not determined to be invalid. Ignoring it for now in favour of "
                + id.getAttemptNumber());
            addIdentifierToList = true;
            break;
          }
        }
        if (addIdentifierToList) {
          dedupedList.put(inputNumber, id);
        }
      } else {
        LOG.info("Ignoring finished or obsolete source: " + id);
      }
    }

    // Compute the final list, limited by NUM_FETCHERS_AT_ONCE
    List<InputAttemptIdentifier> result = new ArrayList<InputAttemptIdentifier>();
    int includedMaps = 0;
    int totalSize = dedupedList.size();

    for(Integer inputIndex : dedupedList.keySet()) {
      List<InputAttemptIdentifier> attemptIdentifiers = dedupedList.get(inputIndex);
      for (InputAttemptIdentifier inputAttemptIdentifier : attemptIdentifiers) {
        if (includedMaps++ >= maxTaskOutputAtOnce) {
          host.addKnownMap(inputAttemptIdentifier);
        } else {
          result.add(inputAttemptIdentifier);
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("assigned " + includedMaps + " of " + totalSize + " to " +
          host + " to " + Thread.currentThread().getName());
    }
    return result;
  }

  public synchronized void freeHost(MapHost host) {
    if (host.getState() != MapHost.State.PENALIZED) {
      if (host.markAvailable() == MapHost.State.PENDING) {
        pendingHosts.add(host);
        notifyAll();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(host + " freed by " + Thread.currentThread().getName() + " in " +
          (System.currentTimeMillis() - shuffleStart.get()) + "ms");
    }
  }

  public synchronized void resetKnownMaps() {
    mapLocations.clear();
    obsoleteInputs.clear();
    pendingHosts.clear();
    pathToIdentifierMap.clear();
  }

  /**
   * Utility method to check if the Shuffle data fetch is complete.
   * @return
   */
  public synchronized boolean isDone() {
    return remainingMaps.get() == 0;
  }

  /**
   * Wait until the shuffle finishes or until the timeout.
   * @param millis maximum wait time
   * @return true if the shuffle is done
   * @throws InterruptedException
   */
  public synchronized boolean waitUntilDone(int millis
                                            ) throws InterruptedException {
    if (remainingMaps.get() > 0) {
      wait(millis);
      return remainingMaps.get() == 0;
    }
    return true;
  }
  
  /**
   * A structure that records the penalty for a host.
   */
  private static class Penalty implements Delayed {
    MapHost host;
    private long endTime;
    
    Penalty(MapHost host, long delay) {
      this.host = host;
      this.endTime = System.currentTimeMillis() + delay;
    }

    public long getDelay(TimeUnit unit) {
      long remainingTime = endTime - System.currentTimeMillis();
      return unit.convert(remainingTime, TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed o) {
      long other = ((Penalty) o).endTime;
      return endTime == other ? 0 : (endTime < other ? -1 : 1);
    }
    
  }
  
  private String getIdentifierFromPathAndReduceId(String path, int reduceId) {
    return path + "_" + reduceId;
  }
  
  /**
   * A thread that takes hosts off of the penalty list when the timer expires.
   */
  private class Referee extends Thread {
    public Referee() {
      setName("ShufflePenaltyReferee {"
          + TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName()) + "}");
      setDaemon(true);
    }

    public void run() {
      try {
        while (true) {
          // take the first host that has an expired penalty
          MapHost host = penalties.take().host;
          synchronized (ShuffleScheduler.this) {
            if (host.markAvailable() == MapHost.State.PENDING) {
              pendingHosts.add(host);
              ShuffleScheduler.this.notifyAll();
            }
          }
        }
      } catch (InterruptedException ie) {
        // This handles shutdown of the entire fetch / merge process.
        return;
      } catch (Throwable t) {
        // Shuffle knows how to deal with failures post shutdown via the onFailure hook
        shuffle.reportException(t);
      }
    }
  }
  
  public void close() throws InterruptedException {
    logProgress();
    referee.interrupt();
    referee.join();
  }

  void setInputFinished(int inputIndex) {
    synchronized(finishedMaps) {
      finishedMaps[inputIndex] = true;
    }
  }
  
  boolean isInputFinished(int inputIndex) {
    synchronized (finishedMaps) {
      return finishedMaps[inputIndex];      
    }
  }
}
