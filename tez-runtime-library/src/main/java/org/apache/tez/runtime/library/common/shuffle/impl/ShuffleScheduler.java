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
package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;

import com.google.common.collect.Lists;

class ShuffleScheduler {
  static ThreadLocal<Long> shuffleStart = new ThreadLocal<Long>() {
    protected Long initialValue() {
      return 0L;
    }
  };

  private static final Log LOG = LogFactory.getLog(ShuffleScheduler.class);
  private static final int MAX_MAPS_AT_ONCE = 20;
  private static final long INITIAL_PENALTY = 10000;
  private static final float PENALTY_GROWTH_RATE = 1.3f;
  
  // TODO NEWTEZ May need to be a string if attempting to fetch from multiple inputs.
  private boolean[] finishedMaps;
  private final int numInputs;
  private int remainingMaps;
  private Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
  //TODO NEWTEZ Clean this and other maps at some point
  private ConcurrentMap<String, InputAttemptIdentifier> pathToIdentifierMap = new ConcurrentHashMap<String, InputAttemptIdentifier>(); 
  private Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private Set<InputAttemptIdentifier> obsoleteInputs = new HashSet<InputAttemptIdentifier>();
  
  private final Random random = new Random(System.currentTimeMillis());
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  private final Referee referee = new Referee();
  private final Map<InputAttemptIdentifier, IntWritable> failureCounts =
    new HashMap<InputAttemptIdentifier,IntWritable>(); 
  private final Map<String,IntWritable> hostFailures = 
    new HashMap<String,IntWritable>();
  private final TezInputContext inputContext;
  private final Shuffle shuffle;
  private final int abortFailureLimit;
  private final TezCounter shuffledMapsCounter;
  private final TezCounter reduceShuffleBytes;
  private final TezCounter failedShuffleCounter;
  
  private final long startTime;
  private long lastProgressTime;
  
  private int maxMapRuntime = 0;
  private int maxFailedUniqueFetches = 5;
  private int maxFetchFailuresBeforeReporting;
  
  private long totalBytesShuffledTillNow = 0;
  private DecimalFormat  mbpsFormat = new DecimalFormat("0.00");

  private boolean reportReadErrorImmediately = true;
  
  public ShuffleScheduler(TezInputContext inputContext,
                          Configuration conf,
                          int numberOfInputs,
                          Shuffle shuffle,
                          TezCounter shuffledMapsCounter,
                          TezCounter reduceShuffleBytes,
                          TezCounter failedShuffleCounter) {
    this.inputContext = inputContext;
    this.numInputs = numberOfInputs;
    abortFailureLimit = Math.max(30, numberOfInputs / 10);
    remainingMaps = numberOfInputs;
    finishedMaps = new boolean[remainingMaps]; // default init to false
    this.shuffle = shuffle;
    this.shuffledMapsCounter = shuffledMapsCounter;
    this.reduceShuffleBytes = reduceShuffleBytes;
    this.failedShuffleCounter = failedShuffleCounter;
    this.startTime = System.currentTimeMillis();
    this.lastProgressTime = startTime;
    this.maxFailedUniqueFetches = Math.min(numberOfInputs,
        this.maxFailedUniqueFetches);
    referee.start();
    this.maxFetchFailuresBeforeReporting = 
        conf.getInt(
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT);
    this.reportReadErrorImmediately = 
        conf.getBoolean(
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR);
  }

  public synchronized void copySucceeded(InputAttemptIdentifier srcAttemptIdentifier, 
                                         MapHost host,
                                         long bytes,
                                         long milis,
                                         MapOutput output
                                         ) throws IOException {
    String taskIdentifier = TezRuntimeUtils.getTaskAttemptIdentifier(srcAttemptIdentifier.getInputIdentifier().getInputIndex(), srcAttemptIdentifier.getAttemptNumber());
    failureCounts.remove(taskIdentifier);
    hostFailures.remove(host.getHostName());
    
    if (!isInputFinished(srcAttemptIdentifier.getInputIdentifier().getInputIndex())) {
      output.commit();
      setInputFinished(srcAttemptIdentifier.getInputIdentifier().getInputIndex());
      shuffledMapsCounter.increment(1);
      if (--remainingMaps == 0) {
        notifyAll();
      }

      // update the status
      lastProgressTime = System.currentTimeMillis();
      totalBytesShuffledTillNow += bytes;
      logProgress();
      reduceShuffleBytes.increment(bytes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("src task: "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
                inputContext.getSourceVertexName(), srcAttemptIdentifier.getInputIdentifier().getInputIndex(),
                srcAttemptIdentifier.getAttemptNumber()) + " done");
      }
    }
    // TODO NEWTEZ Should this be releasing the output, if not committed ? Possible memory leak in case of speculation.
  }

  private void logProgress() {
    float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
    int mapsDone = numInputs - remainingMaps;
    long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

    float transferRate = mbs / secsSinceStart;
    LOG.info("copy(" + mapsDone + " of " + numInputs + " at "
        + mbpsFormat.format(transferRate) + " MB/s)");
  }

  public synchronized void copyFailed(InputAttemptIdentifier srcAttempt,
                                      MapHost host,
                                      boolean readError) {
    host.penalize();
    int failures = 1;
    if (failureCounts.containsKey(srcAttempt)) {
      IntWritable x = failureCounts.get(srcAttempt);
      x.set(x.get() + 1);
      failures = x.get();
    } else {
      failureCounts.put(srcAttempt, new IntWritable(1));      
    }
    String hostname = host.getHostName();
    if (hostFailures.containsKey(hostname)) {
      IntWritable x = hostFailures.get(hostname);
      x.set(x.get() + 1);
    } else {
      hostFailures.put(hostname, new IntWritable(1));
    }
    if (failures >= abortFailureLimit) {
      IOException ioe = new IOException(failures
            + " failures downloading "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
                inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier().getInputIndex(),
                srcAttempt.getAttemptNumber()));
      ioe.fillInStackTrace();
      shuffle.reportException(ioe);
    }
    
    checkAndInformAM(failures, srcAttempt, readError);

    checkReducerHealth();
    
    long delay = (long) (INITIAL_PENALTY *
        Math.pow(PENALTY_GROWTH_RATE, failures));
    
    penalties.add(new Penalty(host, delay));
    
    failedShuffleCounter.increment(1);
  }
  
  // Notify the JobTracker  
  // after every read error, if 'reportReadErrorImmediately' is true or
  // after every 'maxFetchFailuresBeforeReporting' failures
  private void checkAndInformAM(
      int failures, InputAttemptIdentifier srcAttempt, boolean readError) {
    if ((reportReadErrorImmediately && readError)
        || ((failures % maxFetchFailuresBeforeReporting) == 0)) {
      LOG.info("Reporting fetch failure for InputIdentifier: " 
          + srcAttempt + " taskAttemptIdentifier: "
          + TezRuntimeUtils.getTaskAttemptIdentifier(
              inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier().getInputIndex(),
              srcAttempt.getAttemptNumber()) + " to AM.");
      List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
      failedEvents.add(new InputReadErrorEvent("Fetch failure for "
          + TezRuntimeUtils.getTaskAttemptIdentifier(
              inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier().getInputIndex(),
              srcAttempt.getAttemptNumber()) + " to jobtracker.", srcAttempt.getInputIdentifier()
          .getInputIndex(), srcAttempt.getAttemptNumber()));

      inputContext.sendEvents(failedEvents);      
      //status.addFailedDependency(mapId);
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
      LOG.fatal("Shuffle failed with too many fetch failures " +
      "and insufficient progress!");
      String errorMsg = "Exceeded MAX_FAILED_UNIQUE_FETCHES; bailing-out.";
      shuffle.reportException(new IOException(errorMsg));
    }

  }
  
  public synchronized void addKnownMapOutput(String hostName,
                                             int partitionId,
                                             String hostUrl,
                                             InputAttemptIdentifier srcAttempt) {
    String identifier = MapHost.createIdentifier(hostName, partitionId);
    MapHost host = mapLocations.get(identifier);
    if (host == null) {
      host = new MapHost(partitionId, hostName, hostUrl);
      assert identifier.equals(host.getIdentifier());
      mapLocations.put(identifier, host);
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
      
      LOG.info("Assigning " + host + " with " + host.getNumKnownMapOutputs() + 
               " to " + Thread.currentThread().getName());
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
    Map<Integer, InputAttemptIdentifier> dedupedList = new LinkedHashMap<Integer, InputAttemptIdentifier>();
    Iterator<InputAttemptIdentifier> listItr = origList.iterator();
    while (listItr.hasNext()) {
      // we may want to try all versions of the input but with current retry 
      // behavior older ones are likely to be lost and should be ignored.
      // This may be removed after TEZ-914
      InputAttemptIdentifier id = listItr.next();
      Integer inputNumber = new Integer(id.getInputIdentifier().getInputIndex());
      InputAttemptIdentifier oldId = dedupedList.get(inputNumber);
      if (oldId == null || oldId.getAttemptNumber() < id.getAttemptNumber()) {
        dedupedList.put(inputNumber, id);
        if (oldId != null) {
          LOG.warn("Ignoring older source: " + oldId + 
              " in favor of newer source: " + id);
        }
      }
    }
    List<InputAttemptIdentifier> result = new ArrayList<InputAttemptIdentifier>();
    int includedMaps = 0;
    int totalSize = dedupedList.size();
    Iterator<Map.Entry<Integer, InputAttemptIdentifier>> dedupedItr = dedupedList.entrySet().iterator();
    // find the maps that we still need, up to the limit
    while (dedupedItr.hasNext()) {
      InputAttemptIdentifier id = dedupedItr.next().getValue();
      if (inputShouldBeConsumed(id)) {
        result.add(id);
        if (++includedMaps >= MAX_MAPS_AT_ONCE) {
          break;
        }
      } else {
        LOG.info("Ignoring finished or obsolete source: " + id);
      }
    }
    // put back the maps left after the limit
    while (dedupedItr.hasNext()) {
      InputAttemptIdentifier id = dedupedItr.next().getValue();
      if (inputShouldBeConsumed(id)) {
        host.addKnownMap(id);
      } else {
        LOG.info("Ignoring finished or obsolete source: " + id);
      }
    }
    LOG.info("assigned " + includedMaps + " of " + totalSize + " to " +
             host + " to " + Thread.currentThread().getName());
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
             (System.currentTimeMillis()-shuffleStart.get()) + "s");
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
      setName("ShufflePenaltyReferee");
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
        return;
      } catch (Throwable t) {
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
