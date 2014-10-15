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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.net.NetUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.impl.MapOutput.Type;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection.HttpConnectionParams;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;

import com.google.common.annotations.VisibleForTesting;

class Fetcher extends Thread {
  
  private static final Log LOG = LogFactory.getLog(Fetcher.class);
  private final Configuration conf;
  private final boolean localDiskFetchEnabled;

  private static enum ShuffleErrors{IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP,
                                    CONNECTION, WRONG_REDUCE}
  
  private final static String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";
  private final TezCounter connectionErrs;
  private final TezCounter ioErrs;
  private final TezCounter wrongLengthErrs;
  private final TezCounter badIdErrs;
  private final TezCounter wrongMapErrs;
  private final TezCounter wrongReduceErrs;
  private final MergeManager merger;
  private final ShuffleScheduler scheduler;
  private final ShuffleClientMetrics metrics;
  private final Shuffle shuffle;
  private final int id;
  private final String logIdentifier;
  private static int nextId = 0;
  private int currentPartition = -1;

  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final SecretKey jobTokenSecret;

  private volatile boolean stopped = false;
  
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private LinkedHashSet<InputAttemptIdentifier> remaining;

  volatile HttpURLConnection connection;
  volatile DataInputStream input;

  HttpConnection httpConnection;
  HttpConnectionParams httpConnectionParams;

  final static String localhostName = NetUtils.getHostname();
  
  public Fetcher(HttpConnectionParams httpConnectionParams,
                 ShuffleScheduler scheduler, MergeManager merger,
                 ShuffleClientMetrics metrics,
                 Shuffle shuffle, SecretKey jobTokenSecret,
                 boolean ifileReadAhead, int ifileReadAheadLength, CompressionCodec codec,
                 InputContext inputContext, Configuration conf, boolean localDiskFetchEnabled) throws IOException {
    setDaemon(true);
    this.scheduler = scheduler;
    this.merger = merger;
    this.metrics = metrics;
    this.shuffle = shuffle;
    this.id = ++nextId;
    this.jobTokenSecret = jobTokenSecret;
    ioErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.IO_ERROR.toString());
    wrongLengthErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_LENGTH.toString());
    badIdErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.BAD_ID.toString());
    wrongMapErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_MAP.toString());
    connectionErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.CONNECTION.toString());
    wrongReduceErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_REDUCE.toString());

    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.httpConnectionParams = httpConnectionParams;
    if (codec != null) {
      this.codec = codec;
    } else {
      this.codec = null;
    }
    this.conf = conf;

    this.localDiskFetchEnabled = localDiskFetchEnabled;

    this.logIdentifier = "fetcher [" + TezUtilsInternal
        .cleanVertexName(inputContext.getSourceVertexName()) + "] #" + id + " " + localhostName;
    setName(logIdentifier);
    setDaemon(true);
  }  

  public void run() {
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        remaining = null; // Safety.
        MapHost host = null;
        try {
          // If merge is on, block
          merger.waitForInMemoryMerge();

          // Get a host to shuffle from
          host = scheduler.getHost();
          metrics.threadBusy();

          String hostPort = host.getHostIdentifier();
          String hostname = hostPort.substring(0, hostPort.indexOf(":"));
          if (localDiskFetchEnabled &&
              hostname.equals(System.getenv(ApplicationConstants.Environment.NM_HOST.toString()))) {
            setupLocalDiskFetch(host);
          } else {
            // Shuffle
            copyFromHost(host);
          }
        } finally {
          cleanupCurrentConnection(false);
          if (host != null) {
            scheduler.freeHost(host);
            metrics.threadFree();
          }
        }
      }
    } catch (InterruptedException ie) {
      return;
    } catch (Throwable t) {
      shuffle.reportException(t);
    }
  }

  public void shutDown() throws InterruptedException {
    this.stopped = true;
    interrupt();
    cleanupCurrentConnection(true);
    try {
      join(5000);
    } catch (InterruptedException ie) {
      LOG.warn("Got interrupt while joining " + getName(), ie);
    }
  }

  private Object cleanupLock = new Object();
  private void cleanupCurrentConnection(boolean disconnect) {
    // Synchronizing on cleanupLock to ensure we don't run into a parallel close
    // Can't synchronize on the main class itself since that would cause the
    // shutdown request to block
    synchronized (cleanupLock) {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(disconnect);
        }
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Exception while shutting down fetcher " + logIdentifier, e);
        } else {
          LOG.info("Exception while shutting down fetcher " + logIdentifier + ": " + e.getMessage());
        }
      }
    }
  }

  /**
   * The crux of the matter...
   * 
   * @param host {@link MapHost} from which we need to  
   *              shuffle available map-outputs.
   */
  @VisibleForTesting
  protected void copyFromHost(MapHost host) throws IOException {
    // Get completed maps on 'host'
    List<InputAttemptIdentifier> srcAttempts = scheduler.getMapsForHost(host);
    currentPartition = host.getPartitionId();
    
    // Sanity check to catch hosts with only 'OBSOLETE' maps, 
    // especially at the tail of large jobs
    if (srcAttempts.size() == 0) {
      return;
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + id + " going to fetch from " + host + " for: "
        + srcAttempts + ", partitionId: " + currentPartition);
    }
    
    // List of maps to be fetched yet
    remaining = new LinkedHashSet<InputAttemptIdentifier>(srcAttempts);
    
    // Construct the url and connect
    boolean connectSucceeded = false;
    
    try {
      URL url = ShuffleUtils.constructInputURL(host.getBaseUrl(), srcAttempts,
        httpConnectionParams.getKeepAlive());
      httpConnection = new HttpConnection(url, httpConnectionParams,
        logIdentifier, jobTokenSecret);
      connectSucceeded = httpConnection.connect();
      
      if (stopped) {
        LOG.info("Detected fetcher has been shutdown after connection establishment. Returning");
        cleanupCurrentConnection(true);
        putBackRemainingMapOutputs(host);
        return;
      }
      input = httpConnection.getInputStream();
      httpConnection.validate();
    } catch (IOException ie) {
      if (stopped) {
        LOG.info("Not reporting fetch failure, since an Exception was caught after shutdown");
        cleanupCurrentConnection(true);
        putBackRemainingMapOutputs(host);
        return;
      }
      ioErrs.increment(1);
      if (!connectSucceeded) {
        LOG.warn("Failed to connect to " + host + " with " + remaining.size() + " inputs", ie);
        connectionErrs.increment(1);
      } else {
        LOG.warn("Failed to verify reply after connecting to " + host + " with " + remaining.size()
          + " inputs pending", ie);
      }

      // At this point, either the connection failed, or the initial header verification failed.
      // The error does not relate to any specific Input. Report all of them as failed.
      // This ends up indirectly penalizing the host (multiple failures reported on the single host)
      for(InputAttemptIdentifier left: remaining) {
        // Need to be handling temporary glitches .. 
        // Report read error to the AM to trigger source failure heuristics
        scheduler.copyFailed(left, host, connectSucceeded, !connectSucceeded);
      }

      // Add back all remaining maps - which at this point is ALL MAPS the
      // Fetcher was started with. The Scheduler takes care of retries,
      // reporting too many failures etc.
      putBackRemainingMapOutputs(host);
      return;
    }
    
    try {
      // Loop through available map-outputs and fetch them
      // On any error, faildTasks is not null and we exit
      // after putting back the remaining maps to the 
      // yet_to_be_fetched list and marking the failed tasks.
      InputAttemptIdentifier[] failedTasks = null;
      while (!remaining.isEmpty() && failedTasks == null) {
        // fail immediately after first failure because we dont know how much to 
        // skip for this error in the input stream. So we cannot move on to the 
        // remaining outputs. YARN-1773. Will get to them in the next retry.
        failedTasks = copyMapOutput(host, input);
      }
      
      if(failedTasks != null && failedTasks.length > 0) {
        LOG.warn("copyMapOutput failed for tasks "+Arrays.toString(failedTasks));
        for(InputAttemptIdentifier left: failedTasks) {
          scheduler.copyFailed(left, host, true, false);
        }
      }

      cleanupCurrentConnection(false);

      // Sanity check
      if (failedTasks == null && !remaining.isEmpty()) {
        throw new IOException("server didn't return all expected map outputs: "
            + remaining.size() + " left.");
      }
    } finally {
      putBackRemainingMapOutputs(host);
    }
  }

  @VisibleForTesting
  protected void putBackRemainingMapOutputs(MapHost host) {
    // Cycle through remaining MapOutputs
    boolean isFirst = true;
    InputAttemptIdentifier first = null;
    for (InputAttemptIdentifier left : remaining) {
      if (isFirst) {
        first = left;
        isFirst = false;
        continue;
      }
      scheduler.putBackKnownMapOutput(host, left);
    }
    if (first != null) { // Empty remaining list.
      scheduler.putBackKnownMapOutput(host, first);
    }
  }

  private static InputAttemptIdentifier[] EMPTY_ATTEMPT_ID_ARRAY = new InputAttemptIdentifier[0];
  
  private InputAttemptIdentifier[] copyMapOutput(MapHost host,
                                DataInputStream input) {
    MapOutput mapOutput = null;
    InputAttemptIdentifier srcAttemptId = null;
    long decompressedLength = -1;
    long compressedLength = -1;
    
    try {
      long startTime = System.currentTimeMillis();
      int forReduce = -1;
      //Read the shuffle header
      try {
        ShuffleHeader header = new ShuffleHeader();
        // TODO Review: Multiple header reads in case of status WAIT ? 
        header.readFields(input);
        if (!header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX)) {
          throw new IllegalArgumentException(
              "Invalid header received: " + header.mapId + " partition: " + header.forReduce);
        }
        srcAttemptId = 
            scheduler.getIdentifierForFetchedOutput(header.mapId, header.forReduce);
        compressedLength = header.compressedLength;
        decompressedLength = header.uncompressedLength;
        forReduce = header.forReduce;
      } catch (IllegalArgumentException e) {
        badIdErrs.increment(1);
        LOG.warn("Invalid map id ", e);
        // Don't know which one was bad, so consider this one bad and dont read
        // the remaining because we dont know where to start reading from. YARN-1773
        return new InputAttemptIdentifier[] {getNextRemainingAttempt()};
      }

      // Do some basic sanity verification
      if (!verifySanity(compressedLength, decompressedLength, forReduce,
          remaining, srcAttemptId)) {
        if (srcAttemptId == null) {
          LOG.warn("Was expecting " + getNextRemainingAttempt() + " but got null");
          srcAttemptId = getNextRemainingAttempt();
        }
        assert(srcAttemptId != null);
        return new InputAttemptIdentifier[] {srcAttemptId};
      }
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("header: " + srcAttemptId + ", len: " + compressedLength + 
            ", decomp len: " + decompressedLength);
      }
      
      // Get the location for the map output - either in-memory or on-disk
      try {
        mapOutput = merger.reserve(srcAttemptId, decompressedLength, compressedLength, id);
      } catch (IOException e) {
        // Kill the reduce attempt
        ioErrs.increment(1);
        scheduler.reportLocalError(e);
        return EMPTY_ATTEMPT_ID_ARRAY;
      }
      
      // Check if we can shuffle *now* ...
      if (mapOutput.getType() == Type.WAIT) {
        // TODO Review: Does this cause a tight loop ?
        LOG.info("fetcher#" + id + " - MergerManager returned Status.WAIT ...");
        //Not an error but wait to process data.
        return EMPTY_ATTEMPT_ID_ARRAY;
      } 
      
      // Go!
      LOG.info("fetcher#" + id + " about to shuffle output of map " + 
               mapOutput.getAttemptIdentifier() + " decomp: " +
               decompressedLength + " len: " + compressedLength);
      if (mapOutput.getType() == Type.MEMORY) {
        ShuffleUtils.shuffleToMemory(mapOutput.getMemory(), input,
          (int) decompressedLength, (int) compressedLength, codec, ifileReadAhead,
          ifileReadAheadLength, LOG, mapOutput.getAttemptIdentifier().toString());
      } else if (mapOutput.getType() == Type.DISK) {
        ShuffleUtils.shuffleToDisk(mapOutput.getDisk(), host.getHostIdentifier(),
          input, compressedLength, LOG, mapOutput.getAttemptIdentifier().toString());
      } else {
        throw new IOException("Unknown mapOutput type while fetching shuffle data:" +
            mapOutput.getType());
      }
      
      // Inform the shuffle scheduler
      long endTime = System.currentTimeMillis();
      scheduler.copySucceeded(srcAttemptId, host, compressedLength, decompressedLength, 
                              endTime - startTime, mapOutput);
      // Note successful shuffle
      remaining.remove(srcAttemptId);
      metrics.successFetch();
      return null;
    } catch (IOException ioe) {
      if (stopped) {
        LOG.info("Not reporting fetch failure for exception during data copy: ["
            + ioe.getClass().getName() + ", " + ioe.getMessage() + "]");
        cleanupCurrentConnection(true);
        if (mapOutput != null) {
          mapOutput.abort(); // Release resources
        }
        // Don't need to put back - since that's handled by the invoker
        return EMPTY_ATTEMPT_ID_ARRAY;
      }
      ioErrs.increment(1);
      if (srcAttemptId == null || mapOutput == null) {
        LOG.info("fetcher#" + id + " failed to read map header" + 
                 srcAttemptId + " decomp: " + 
                 decompressedLength + ", " + compressedLength, ioe);
        if(srcAttemptId == null) {
          return remaining.toArray(new InputAttemptIdentifier[remaining.size()]);
        } else {
          return new InputAttemptIdentifier[] {srcAttemptId};
        }
      }
      
      LOG.warn("Failed to shuffle output of " + srcAttemptId + 
               " from " + host.getHostIdentifier(), ioe); 

      // Inform the shuffle-scheduler
      mapOutput.abort();
      metrics.failedFetch();
      return new InputAttemptIdentifier[] {srcAttemptId};
    }

  }
  
  /**
   * Do some basic verification on the input received -- Being defensive
   * @param compressedLength
   * @param decompressedLength
   * @param forReduce
   * @param remaining
   * @param srcAttemptId
   * @return true/false, based on if the verification succeeded or not
   */
  private boolean verifySanity(long compressedLength, long decompressedLength,
      int forReduce, Set<InputAttemptIdentifier> remaining, InputAttemptIdentifier srcAttemptId) {
    if (compressedLength < 0 || decompressedLength < 0) {
      wrongLengthErrs.increment(1);
      LOG.warn(getName() + " invalid lengths in map output header: id: " +
          srcAttemptId + " len: " + compressedLength + ", decomp len: " + 
               decompressedLength);
      return false;
    }

    // partitionId verification. Isn't availalbe here because it is encoded into
    // URI
    if (forReduce != currentPartition) {
      wrongReduceErrs.increment(1);
      LOG.warn(getName() + " data for the wrong partition map: " + srcAttemptId + " len: "
          + compressedLength + " decomp len: " + decompressedLength + " for partition " + forReduce
          + ", expected partition: " + currentPartition);
      return false;
    }

    // Sanity check
    if (!remaining.contains(srcAttemptId)) {
      wrongMapErrs.increment(1);
      LOG.warn("Invalid map-output! Received output for " + srcAttemptId);
      return false;
    }
    
    return true;
  }
  
  private InputAttemptIdentifier getNextRemainingAttempt() {
    if (remaining.size() > 0) {
      return remaining.iterator().next();
    } else {
      return null;
    }
  }

  @VisibleForTesting
  protected void setupLocalDiskFetch(MapHost host) throws InterruptedException {
    // Get completed maps on 'host'
    List<InputAttemptIdentifier> srcAttempts = scheduler.getMapsForHost(host);
    currentPartition = host.getPartitionId();

    // Sanity check to catch hosts with only 'OBSOLETE' maps,
    // especially at the tail of large jobs
    if (srcAttempts.size() == 0) {
      return;
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + id + " going to fetch (local disk) from " + host + " for: "
          + srcAttempts + ", partitionId: " + currentPartition);
    }

    // List of maps to be fetched yet
    remaining = new LinkedHashSet<InputAttemptIdentifier>(srcAttempts);

    try {
      final Iterator<InputAttemptIdentifier> iter = remaining.iterator();
      while (iter.hasNext()) {
        InputAttemptIdentifier srcAttemptId = iter.next();
        MapOutput mapOutput = null;
        try {
          long startTime = System.currentTimeMillis();
          Path filename = getShuffleInputFileName(srcAttemptId.getPathComponent(), null);

          TezIndexRecord indexRecord = getIndexRecord(srcAttemptId.getPathComponent(),
              currentPartition);

          mapOutput = getMapOutputForDirectDiskFetch(srcAttemptId, filename, indexRecord);
          long endTime = System.currentTimeMillis();
          scheduler.copySucceeded(srcAttemptId, host, indexRecord.getPartLength(),
              indexRecord.getRawLength(), (endTime - startTime), mapOutput);
          iter.remove();
          metrics.successFetch();
        } catch (IOException e) {
          if (mapOutput != null) {
            mapOutput.abort();
          }
          metrics.failedFetch();
          ioErrs.increment(1);
          scheduler.copyFailed(srcAttemptId, host, true, false);
          LOG.warn("Failed to read local disk output of " + srcAttemptId + " from " +
              host.getHostIdentifier(), e);
        }
      }
    } finally {
      putBackRemainingMapOutputs(host);
    }

  }

  @VisibleForTesting
  protected Path getShuffleInputFileName(String pathComponent, String suffix)
      throws IOException {
    LocalDirAllocator localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    suffix = suffix != null ? suffix : "";

    String pathFromLocalDir = Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR + Path.SEPARATOR +
        pathComponent + Path.SEPARATOR + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING + suffix;

    return localDirAllocator.getLocalPathToRead(pathFromLocalDir.toString(), conf);
  }

  @VisibleForTesting
  protected TezIndexRecord getIndexRecord(String pathComponent, int partitionId)
      throws IOException {
    Path indexFile = getShuffleInputFileName(pathComponent,
        Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
    return spillRecord.getIndex(partitionId);
  }

  @VisibleForTesting
  protected MapOutput getMapOutputForDirectDiskFetch(InputAttemptIdentifier srcAttemptId,
                                                     Path filename, TezIndexRecord indexRecord)
      throws IOException {
    return MapOutput.createLocalDiskMapOutput(srcAttemptId, merger, filename,
        indexRecord.getStartOffset(), indexRecord.getPartLength(), true);
  }
}

