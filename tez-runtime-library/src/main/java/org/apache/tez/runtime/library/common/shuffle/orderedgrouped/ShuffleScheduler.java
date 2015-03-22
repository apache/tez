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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  private static final Log LOG = LogFactory.getLog(ShuffleScheduler.class);
  private static final long INITIAL_PENALTY = 2000l; // 2 seconds
  private static final float PENALTY_GROWTH_RATE = 1.3f;

  private boolean[] finishedMaps;
  private final int numInputs;
  private int remainingMaps;
  private int numFetchedSpills;
  private Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
  //TODO Clean this and other maps at some point
  private ConcurrentMap<String, InputAttemptIdentifier> pathToIdentifierMap = new ConcurrentHashMap<String, InputAttemptIdentifier>();

  //To track shuffleInfo events when finalMerge is disabled in source or pipelined shuffle is
  // enabled in source.
  @VisibleForTesting
  final Map<InputIdentifier, ShuffleEventInfo> shuffleInfoEventsMap;

  private Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private Set<InputAttemptIdentifier> obsoleteInputs = new HashSet<InputAttemptIdentifier>();
  
  private final Random random = new Random(System.currentTimeMillis());
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  private final Referee referee;
  private final Map<InputAttemptIdentifier, IntWritable> failureCounts =
    new HashMap<InputAttemptIdentifier,IntWritable>(); 
  private final Map<String,IntWritable> hostFailures = 
    new HashMap<String,IntWritable>();
  private final InputContext inputContext;
  private final Shuffle shuffle;
  private final TezCounter shuffledInputsCounter;
  private final TezCounter skippedInputCounter;
  private final TezCounter reduceShuffleBytes;
  private final TezCounter reduceBytesDecompressed;
  private final TezCounter failedShuffleCounter;
  private final TezCounter bytesShuffledToDisk;
  private final TezCounter bytesShuffledToDiskDirect;
  private final TezCounter bytesShuffledToMem;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;

  private final long startTime;
  private long lastProgressTime;

  private int maxTaskOutputAtOnce;
  private int maxFetchFailuresBeforeReporting;
  private boolean reportReadErrorImmediately = true; 
  private int maxFailedUniqueFetches = 5;
  private final int abortFailureLimit;
  private int maxMapRuntime = 0;

  private long totalBytesShuffledTillNow = 0;
  private DecimalFormat  mbpsFormat = new DecimalFormat("0.00");

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
                          TezCounter bytesShuffledToMem, long startTime) {
    this.inputContext = inputContext;
    this.numInputs = numberOfInputs;
    abortFailureLimit = Math.max(30, numberOfInputs / 10);
    remainingMaps = numberOfInputs;
    finishedMaps = new boolean[remainingMaps]; // default init to false
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
    this.maxTaskOutputAtOnce = Math.max(1, conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT));
    
    this.skippedInputCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SKIPPED_INPUTS);
    this.firstEventReceived = inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived = inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);

    shuffleInfoEventsMap = new HashMap<InputIdentifier, ShuffleEventInfo>();
    LOG.info("ShuffleScheduler running for sourceVertex: "
        + inputContext.getSourceVertexName() + " with configuration: "
        + "maxFetchFailuresBeforeReporting=" + maxFetchFailuresBeforeReporting
        + ", reportReadErrorImmediately=" + reportReadErrorImmediately
        + ", maxFailedUniqueFetches=" + maxFailedUniqueFetches
        + ", abortFailureLimit=" + abortFailureLimit
        + ", maxMapRuntime=" + maxMapRuntime);
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
                                         MapOutput output
                                         ) throws IOException {

    if (!isInputFinished(srcAttemptIdentifier.getInputIdentifier().getInputIndex())) {
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
        remainingMaps = remainingMaps - 1;
        setInputFinished(srcAttemptIdentifier.getInputIdentifier().getInputIndex());
        numFetchedSpills++;
      } else {
        InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
        //Allow only one task attempt to proceed.
        if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
          return;
        }

        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

        //Possible that Shuffle event handler invoked this, due to empty partitions
        if (eventInfo == null && output == null) {
          eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
          shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
        }

        assert(eventInfo != null);
        eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
        numFetchedSpills++;

        if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
          eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
        }

        //check if we downloaded all spills pertaining to this InputAttemptIdentifier
        if (eventInfo.isDone()) {
          remainingMaps = remainingMaps - 1;
          setInputFinished(inputIdentifier.getInputIndex());
          shuffleInfoEventsMap.remove(inputIdentifier);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Removing : " + srcAttemptIdentifier + ", pending: " +
                shuffleInfoEventsMap);
          }
        }

        if (LOG.isTraceEnabled()) {
          LOG.trace("eventInfo " + eventInfo.toString());
        }
      }

      if (remainingMaps == 0) {
        LOG.info("All inputs fetched for input vertex : " + inputContext.getSourceVertexName());
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
      LOG.warn("Duplicate fetch of input no longer needs to be fetched: " + srcAttemptIdentifier);
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
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        reportExceptionForInput(new IOException("Previous event already got scheduled for " +
            input + ". Previous attempt's data could have been already merged "
            + "to memory/disk outputs.  Failing the fetch early. currentAttemptNum="
            + eventInfo.attemptNum + ", eventsProcessed=" + eventInfo.eventsProcessed
            + ", newAttemptNum=" + input.getAttemptNumber()));
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  void reportExceptionForInput(Exception exception) {
    LOG.fatal(exception);
    shuffle.reportException(exception);
  }

  private void logProgress() {
    double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
    int inputsDone = numInputs - remainingMaps;
    long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

    double transferRate = mbs / secsSinceStart;
    LOG.info("copy(" + inputsDone + " (spillsFetched=" + numFetchedSpills +  ") of " + numInputs +
        ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) "
        + mbpsFormat.format(transferRate) + " MB/s)");
  }

  public synchronized void copyFailed(InputAttemptIdentifier srcAttempt,
                                      MapHost host,
                                      boolean readError,
                                      boolean connectError) {
    host.penalize();
    int failures = 1;
    if (failureCounts.containsKey(srcAttempt)) {
      IntWritable x = failureCounts.get(srcAttempt);
      x.set(x.get() + 1);
      failures = x.get();
    } else {
      failureCounts.put(srcAttempt, new IntWritable(1));      
    }
    String hostPort = host.getHostIdentifier();
    // TODO TEZ-922 hostFailures isn't really used for anything. Factor it into error
    // reporting / potential blacklisting of hosts.
    if (hostFailures.containsKey(hostPort)) {
      IntWritable x = hostFailures.get(hostPort);
      x.set(x.get() + 1);
    } else {
      hostFailures.put(hostPort, new IntWritable(1));
    }
    if (failures >= abortFailureLimit) {
      // This task has seen too many fetch failures - report it as failed. The
      // AM may retry it if max failures has not been reached.
      
      // Between the task and the AM - someone needs to determine who is at
      // fault. If there's enough errors seen on the task, before the AM informs
      // it about source failure, the task considers itself to have failed and
      // allows the AM to re-schedule it.
      IOException ioe = new IOException(failures
            + " failures downloading "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
                inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier().getInputIndex(),
                srcAttempt.getAttemptNumber()));
      ioe.fillInStackTrace();
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
      shuffle.reportException(ioe);
    }

    failedShuffleCounter.increment(1);
    checkAndInformAM(failures, srcAttempt, readError, connectError);

    checkReducerHealth();
    
    long delay = (long) (INITIAL_PENALTY *
        Math.pow(PENALTY_GROWTH_RATE, failures));
    
    penalties.add(new Penalty(host, delay));    
  }

  public void reportLocalError(IOException ioe) {
    LOG.error("Shuffle failed : caused by local error", ioe);
    // Shuffle knows how to deal with failures post shutdown via the onFailure hook
    shuffle.reportException(ioe);
  }

  // Notify the AM  
  // after every read error, if 'reportReadErrorImmediately' is true or
  // after every 'maxFetchFailuresBeforeReporting' failures
  private void checkAndInformAM(
      int failures, InputAttemptIdentifier srcAttempt, boolean readError,
      boolean connectError) {
    if ((reportReadErrorImmediately && (readError || connectError))
        || ((failures % maxFetchFailuresBeforeReporting) == 0)) {
      LOG.info("Reporting fetch failure for InputIdentifier: "
          + srcAttempt + " taskAttemptIdentifier: "
          + TezRuntimeUtils.getTaskAttemptIdentifier(
          inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier().getInputIndex(),
          srcAttempt.getAttemptNumber()) + " to AM.");
      List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
      failedEvents.add(InputReadErrorEvent.create("Fetch failure for "
          + TezRuntimeUtils.getTaskAttemptIdentifier(
          inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier().getInputIndex(),
          srcAttempt.getAttemptNumber()) + " to jobtracker.", srcAttempt.getInputIdentifier()
          .getInputIndex(), srcAttempt.getAttemptNumber()));

      inputContext.sendEvents(failedEvents);      
    }
  }

  private void checkReducerHealth() {
    final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;
    final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;
    final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

    long totalFailures = failedShuffleCounter.getValue();
    int doneMaps = numInputs - remainingMaps;
    
    boolean reducerHealthy =
      (((float)totalFailures / (totalFailures + doneMaps))
          < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);
    
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

    // min time the reducer should run without getting killed
    int minShuffleRunDuration =
      (shuffleProgressDuration > maxMapRuntime)
      ? shuffleProgressDuration
          : maxMapRuntime;
    
    boolean reducerStalled =
      (((float)stallDuration / minShuffleRunDuration)
          >= MAX_ALLOWED_STALL_TIME_PERCENT);

    // kill if not healthy and has insufficient progress
    if ((failureCounts.size() >= maxFailedUniqueFetches ||
        failureCounts.size() == (numInputs - doneMaps))
        && !reducerHealthy
        && (!reducerProgressedEnough || reducerStalled)) {
      LOG.fatal("Shuffle failed with too many fetch failures " + "and insufficient progress!"
          + "failureCounts=" + failureCounts.size() + ", pendingInputs=" + (numInputs - doneMaps)
          + ", reducerHealthy=" + reducerHealthy + ", reducerProgressedEnough="
          + reducerProgressedEnough + ", reducerStalled=" + reducerStalled);
      String errorMsg = "Exceeded MAX_FAILED_UNIQUE_FETCHES; bailing-out.";
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
      shuffle.reportException(new IOException(errorMsg));
    }

  }

  public synchronized void addKnownMapOutput(String inputHostName,
                                             int port,
                                             int partitionId,
                                             String hostUrl,
                                             InputAttemptIdentifier srcAttempt) {
    String hostPort = (inputHostName + ":" + String.valueOf(port));
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
    if (shuffleInfoEventsMap.get(srcAttempt.getInputIdentifier()) == null) {
      shuffleInfoEventsMap.put(srcAttempt.getInputIdentifier(), new ShuffleEventInfo(srcAttempt));
    }

    host.addKnownMap(srcAttempt);
    pathToIdentifierMap.put(
        getIdentifierFromPathAndReduceId(srcAttempt.getPathComponent(), partitionId), srcAttempt);

    // Mark the host as pending
    if (host.getState() == MapHost.State.PENDING) {
      pendingHosts.add(host);
      notifyAll();
    }
  }
  
  public synchronized void obsoleteInput(InputAttemptIdentifier srcAttempt) {
    // The incoming srcAttempt does not contain a path component.
    LOG.info("Adding obsolete input: " + srcAttempt);
    if (shuffleInfoEventsMap.containsKey(srcAttempt.getInputIdentifier())) {
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
        LOG.debug("Assigning " + host + " with " + host.getNumKnownMapOutputs() +
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
    LOG.info(host + " freed by " + Thread.currentThread().getName() + " in " + 
             (System.currentTimeMillis()-shuffleStart.get()) + "ms");
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
    return remainingMaps == 0;
  }

  /**
   * Wait until the shuffle finishes or until the timeout.
   * @param millis maximum wait time
   * @return true if the shuffle is done
   * @throws InterruptedException
   */
  public synchronized boolean waitUntilDone(int millis
                                            ) throws InterruptedException {
    if (remainingMaps > 0) {
      wait(millis);
      return remainingMaps == 0;
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
      setName("ShufflePenaltyReferee ["
          + TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName()) + "]");
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
    referee.interrupt();
    referee.join();
  }

  public synchronized void informMaxMapRunTime(int duration) {
    if (duration > maxMapRuntime) {
      maxMapRuntime = duration;
    }
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
