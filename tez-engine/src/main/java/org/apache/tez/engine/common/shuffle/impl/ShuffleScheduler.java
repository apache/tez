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
package org.apache.tez.engine.common.shuffle.impl;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Progress;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTaskStatus;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskID;

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
  
  private final boolean[] finishedMaps;
  private final int tasksInDegree;
  private int remainingMaps;
  private Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
  private Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private Set<TezTaskAttemptID> obsoleteMaps = new HashSet<TezTaskAttemptID>();
  
  private final Random random = new Random(System.currentTimeMillis());
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  private final Referee referee = new Referee();
  private final Map<TezTaskAttemptID,IntWritable> failureCounts =
    new HashMap<TezTaskAttemptID,IntWritable>();
  private final Map<String,IntWritable> hostFailures = 
    new HashMap<String,IntWritable>();
  private final TezTaskStatus status;
  private final ExceptionReporter reporter;
  private final int abortFailureLimit;
  private final Progress progress;
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
  
  public ShuffleScheduler(Configuration conf,
                          int tasksInDegree,
                          TezTaskStatus status,
                          ExceptionReporter reporter,
                          Progress progress,
                          TezCounter shuffledMapsCounter,
                          TezCounter reduceShuffleBytes,
                          TezCounter failedShuffleCounter) {
    this.tasksInDegree = tasksInDegree;
    abortFailureLimit = Math.max(30, tasksInDegree / 10);
    remainingMaps = tasksInDegree;
    finishedMaps = new boolean[remainingMaps];
    this.reporter = reporter;
    this.status = status;
    this.progress = progress;
    this.shuffledMapsCounter = shuffledMapsCounter;
    this.reduceShuffleBytes = reduceShuffleBytes;
    this.failedShuffleCounter = failedShuffleCounter;
    this.startTime = System.currentTimeMillis();
    lastProgressTime = startTime;
    referee.start();
    this.maxFailedUniqueFetches = Math.min(tasksInDegree,
        this.maxFailedUniqueFetches);
    this.maxFetchFailuresBeforeReporting = 
        conf.getInt(
            TezJobConfig.TEZ_ENGINE_SHUFFLE_FETCH_FAILURES, 
            TezJobConfig.DEFAULT_TEZ_ENGINE_SHUFFLE_FETCH_FAILURES_LIMIT);
    this.reportReadErrorImmediately = 
        conf.getBoolean(
            TezJobConfig.TEZ_ENGINE_SHUFFLE_NOTIFY_READERROR, 
            TezJobConfig.DEFAULT_TEZ_ENGINE_SHUFFLE_NOTIFY_READERROR);
  }

  public synchronized void copySucceeded(TezTaskAttemptID mapId, 
                                         MapHost host,
                                         long bytes,
                                         long millis,
                                         MapOutput output
                                         ) throws IOException {
    failureCounts.remove(mapId);
    hostFailures.remove(host.getHostName());
    int mapIndex = mapId.getTaskID().getId();
    
    if (!finishedMaps[mapIndex]) {
      output.commit();
      finishedMaps[mapIndex] = true;
      shuffledMapsCounter.increment(1);
      if (--remainingMaps == 0) {
        notifyAll();
      }

      // update the status
      totalBytesShuffledTillNow += bytes;
      updateStatus();
      reduceShuffleBytes.increment(bytes);
      lastProgressTime = System.currentTimeMillis();
      LOG.debug("map " + mapId + " done " + status.getStateString());
    }
  }
  
  private void updateStatus() {
    float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
    int mapsDone = tasksInDegree - remainingMaps;
    long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

    float transferRate = mbs / secsSinceStart;
    progress.set((float) mapsDone / tasksInDegree);
    String statusString = mapsDone + " / " + tasksInDegree + " copied.";
    status.setStateString(statusString);

    progress.setStatus("copy(" + mapsDone + " of " + tasksInDegree + " at "
        + mbpsFormat.format(transferRate) + " MB/s)");
  }

  public synchronized void copyFailed(TezTaskAttemptID mapId, MapHost host,
                                      boolean readError) {
    host.penalize();
    int failures = 1;
    if (failureCounts.containsKey(mapId)) {
      IntWritable x = failureCounts.get(mapId);
      x.set(x.get() + 1);
      failures = x.get();
    } else {
      failureCounts.put(mapId, new IntWritable(1));      
    }
    String hostname = host.getHostName();
    if (hostFailures.containsKey(hostname)) {
      IntWritable x = hostFailures.get(hostname);
      x.set(x.get() + 1);
    } else {
      hostFailures.put(hostname, new IntWritable(1));
    }
    if (failures >= abortFailureLimit) {
      try {
        throw new IOException(failures + " failures downloading " + mapId);
      } catch (IOException ie) {
        reporter.reportException(ie);
      }
    }
    
    checkAndInformJobTracker(failures, mapId, readError);

    checkReducerHealth();
    
    long delay = (long) (INITIAL_PENALTY *
        Math.pow(PENALTY_GROWTH_RATE, failures));
    
    penalties.add(new Penalty(host, delay));
    
    failedShuffleCounter.increment(1);
  }
  
  // Notify the JobTracker  
  // after every read error, if 'reportReadErrorImmediately' is true or
  // after every 'maxFetchFailuresBeforeReporting' failures
  private void checkAndInformJobTracker(
      int failures, TezTaskAttemptID mapId, boolean readError) {
    if ((reportReadErrorImmediately && readError)
        || ((failures % maxFetchFailuresBeforeReporting) == 0)) {
      LOG.info("Reporting fetch failure for " + mapId + " to jobtracker.");
      status.addFailedDependency(mapId);
    }
  }
    
  private void checkReducerHealth() {
    final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;
    final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;
    final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

    long totalFailures = failedShuffleCounter.getValue();
    int doneMaps = tasksInDegree - remainingMaps;
    
    boolean reducerHealthy =
      (((float)totalFailures / (totalFailures + doneMaps))
          < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);
    
    // check if the reducer has progressed enough
    boolean reducerProgressedEnough =
      (((float)doneMaps / tasksInDegree)
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
        failureCounts.size() == (tasksInDegree - doneMaps))
        && !reducerHealthy
        && (!reducerProgressedEnough || reducerStalled)) {
      LOG.fatal("Shuffle failed with too many fetch failures " +
      "and insufficient progress!");
      String errorMsg = "Exceeded MAX_FAILED_UNIQUE_FETCHES; bailing-out.";
      reporter.reportException(new IOException(errorMsg));
    }

  }
  
  public synchronized void tipFailed(TezTaskID taskId) {
    if (!finishedMaps[taskId.getId()]) {
      finishedMaps[taskId.getId()] = true;
      if (--remainingMaps == 0) {
        notifyAll();
      }
      updateStatus();
    }
  }
  
  public synchronized void addKnownMapOutput(String hostName, 
                                             String hostUrl,
                                             TezTaskAttemptID mapId) {
    MapHost host = mapLocations.get(hostName);
    if (host == null) {
      host = new MapHost(hostName, hostUrl);
      mapLocations.put(hostName, host);
    }
    host.addKnownMap(mapId);

    // Mark the host as pending
    if (host.getState() == MapHost.State.PENDING) {
      pendingHosts.add(host);
      notifyAll();
    }
  }
  
  public synchronized void obsoleteMapOutput(TezTaskAttemptID mapId) {
    obsoleteMaps.add(mapId);
  }
  
  public synchronized void putBackKnownMapOutput(MapHost host, 
                                                 TezTaskAttemptID mapId) {
    host.addKnownMap(mapId);
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
      
      LOG.info("Assiging " + host + " with " + host.getNumKnownMapOutputs() + 
               " to " + Thread.currentThread().getName());
      shuffleStart.set(System.currentTimeMillis());
      
      return host;
  }
  
  public synchronized List<TezTaskAttemptID> getMapsForHost(MapHost host) {
    List<TezTaskAttemptID> list = host.getAndClearKnownMaps();
    Iterator<TezTaskAttemptID> itr = list.iterator();
    List<TezTaskAttemptID> result = new ArrayList<TezTaskAttemptID>();
    int includedMaps = 0;
    int totalSize = list.size();
    // find the maps that we still need, up to the limit
    while (itr.hasNext()) {
      TezTaskAttemptID id = itr.next();
      if (!obsoleteMaps.contains(id) && !finishedMaps[id.getTaskID().getId()]) {
        result.add(id);
        if (++includedMaps >= MAX_MAPS_AT_ONCE) {
          break;
        }
      }
    }
    // put back the maps left after the limit
    while (itr.hasNext()) {
      TezTaskAttemptID id = itr.next();
      if (!obsoleteMaps.contains(id) && !finishedMaps[id.getTaskID().getId()]) {
        host.addKnownMap(id);
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
    obsoleteMaps.clear();
    pendingHosts.clear();
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
        reporter.reportException(t);
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
}
