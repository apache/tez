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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.common.CallableWithNdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.Type;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.exceptions.FetcherReadTimeoutException;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import com.google.common.annotations.VisibleForTesting;

class FetcherOrderedGrouped extends CallableWithNdc<Void> {
  
  private static final Logger LOG = LoggerFactory.getLogger(FetcherOrderedGrouped.class);

  private static final AtomicInteger nextId = new AtomicInteger(0);

  private final Configuration conf;
  private final boolean localDiskFetchEnabled;
  private final boolean verifyDiskChecksum;

  private final TezCounter connectionErrs;
  private final TezCounter ioErrs;
  private final TezCounter wrongLengthErrs;
  private final TezCounter badIdErrs;
  private final TezCounter wrongMapErrs;
  private final TezCounter wrongReduceErrs;
  private final FetchedInputAllocatorOrderedGrouped allocator;
  private final ShuffleScheduler scheduler;
  private final ExceptionReporter exceptionReporter;
  private final int id;
  private final String logIdentifier;
  private final String localShuffleHost;
  private final int localShufflePort;
  private final String applicationId;
 private final int dagId;
  private final MapHost mapHost;

  private final int minPartition;
  private final int maxPartition;

  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final JobTokenSecretManager jobTokenSecretManager;

  final HttpConnectionParams httpConnectionParams;
  private final boolean sslShuffle;

  @VisibleForTesting
  volatile boolean stopped = false;

  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  @VisibleForTesting
  Map<String, InputAttemptIdentifier> remaining;
  volatile DataInputStream input;

  volatile BaseHttpConnection httpConnection;
  private final boolean asyncHttp;
  private final boolean compositeFetch;


  // Initiative value is 0, which means it hasn't retried yet.
  private long retryStartTime = 0;

  public FetcherOrderedGrouped(HttpConnectionParams httpConnectionParams,
                               ShuffleScheduler scheduler,
                               FetchedInputAllocatorOrderedGrouped allocator,
                               ExceptionReporter exceptionReporter, JobTokenSecretManager jobTokenSecretMgr,
                               boolean ifileReadAhead, int ifileReadAheadLength,
                               CompressionCodec codec,
                               Configuration conf,
                               boolean localDiskFetchEnabled,
                               String localHostname,
                               int shufflePort,
                               String srcNameTrimmed,
                               MapHost mapHost,
                               TezCounter ioErrsCounter,
                               TezCounter wrongLengthErrsCounter,
                               TezCounter badIdErrsCounter,
                               TezCounter wrongMapErrsCounter,
                               TezCounter connectionErrsCounter,
                               TezCounter wrongReduceErrsCounter,
                               String applicationId,
                               int dagId,
                               boolean asyncHttp,
                               boolean sslShuffle,
                               boolean verifyDiskChecksum,
                               boolean compositeFetch) {
    this.scheduler = scheduler;
    this.allocator = allocator;
    this.exceptionReporter = exceptionReporter;
    this.mapHost = mapHost;
    this.minPartition = this.mapHost.getPartitionId();
    this.maxPartition = this.minPartition + this.mapHost.getPartitionCount() - 1;
    this.id = nextId.incrementAndGet();
    this.jobTokenSecretManager = jobTokenSecretMgr;

    this.ioErrs = ioErrsCounter;
    this.wrongLengthErrs = wrongLengthErrsCounter;
    this.badIdErrs = badIdErrsCounter;
    this.wrongMapErrs = wrongMapErrsCounter;
    this.connectionErrs = connectionErrsCounter;
    this.wrongReduceErrs = wrongReduceErrsCounter;
    this.applicationId = applicationId;
    this.dagId = dagId;

    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.httpConnectionParams = httpConnectionParams;
    this.asyncHttp = asyncHttp;
    if (codec != null) {
      this.codec = codec;
    } else {
      this.codec = null;
    }
    this.conf = conf;
    this.localShuffleHost = localHostname;
    this.localShufflePort = shufflePort;

    this.localDiskFetchEnabled = localDiskFetchEnabled;
    this.sslShuffle = sslShuffle;
    this.verifyDiskChecksum = verifyDiskChecksum;
    this.compositeFetch = compositeFetch;

    this.logIdentifier = "fetcher [" + srcNameTrimmed + "] #" + id;
  }

  @VisibleForTesting
  protected void fetchNext() throws InterruptedException, IOException {
    try {
      if (localDiskFetchEnabled && mapHost.getHost().equals(localShuffleHost) && mapHost.getPort() == localShufflePort) {
        setupLocalDiskFetch(mapHost);
      } else {
        // Shuffle
        copyFromHost(mapHost);
      }
    } finally {
      cleanupCurrentConnection(false);
      scheduler.freeHost(mapHost);
    }
  }

  @Override
  public Void callInternal() {
    try {
      remaining = null; // Safety.
      fetchNext();
    } catch (InterruptedException ie) {
      //TODO: might not be respected when fetcher is in progress / server is busy.  TEZ-711
      //Set the status back
      Thread.currentThread().interrupt();
      return null;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
    }
    return null;
  }

  public void shutDown() {
    if (!stopped) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetcher stopped for host " + mapHost);
      }
      stopped = true;
      // An interrupt will come in while shutting down the thread.
      cleanupCurrentConnection(false);
    }
  }

  private final Object cleanupLock = new Object();
  private void cleanupCurrentConnection(boolean disconnect) {
    // Synchronizing on cleanupLock to ensure we don't run into a parallel close
    // Can't synchronize on the main class itself since that would cause the
    // shutdown request to block
    synchronized (cleanupLock) {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(disconnect);
          httpConnection = null;
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
    // reset retryStartTime for a new host
    retryStartTime = 0;
    // Get completed maps on 'host'
    List<InputAttemptIdentifier> srcAttempts = scheduler.getMapsForHost(host);
    // Sanity check to catch hosts with only 'OBSOLETE' maps,
    // especially at the tail of large jobs
    if (srcAttempts.size() == 0) {
      return;
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + id + " going to fetch from " + host + " for: "
        + srcAttempts + ", partition range: " + minPartition + "-" + maxPartition);
    }
    populateRemainingMap(srcAttempts);
    // Construct the url and connect
    try {
      if (!setupConnection(host, remaining.values())) {
        if (stopped) {
          cleanupCurrentConnection(true);
        }
        // Maps will be added back in the finally block in case of failure.
        return;
      }

      // Loop through available map-outputs and fetch them
      // On any error, faildTasks is not null and we exit
      // after putting back the remaining maps to the 
      // yet_to_be_fetched list and marking the failed tasks.
      InputAttemptIdentifier[] failedTasks = null;
      while (!remaining.isEmpty() && failedTasks == null) {
        InputAttemptIdentifier inputAttemptIdentifier =
            remaining.entrySet().iterator().next().getValue();
        // fail immediately after first failure because we dont know how much to
        // skip for this error in the input stream. So we cannot move on to the 
        // remaining outputs. YARN-1773. Will get to them in the next retry.
        try {
          failedTasks = copyMapOutput(host, input, inputAttemptIdentifier);
        } catch (FetcherReadTimeoutException e) {
          // Setup connection again if disconnected
          cleanupCurrentConnection(true);
          if (stopped) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Not re-establishing connection since Fetcher has been stopped");
            }
            return;
          }
          // Connect with retry
          if (!setupConnection(host, remaining.values())) {
            if (stopped) {
              cleanupCurrentConnection(true);
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Not reporting connection re-establishment failure since fetcher is stopped");
              }
              return;
            }
            failedTasks = new InputAttemptIdentifier[] {getNextRemainingAttempt()};
            break;
          }
        }
      }

      if (failedTasks != null && failedTasks.length > 0) {
        if (stopped) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Ignoring copyMapOutput failures for tasks: " + Arrays.toString(failedTasks) +
                " since Fetcher has been stopped");
          }
        } else {
          LOG.warn("copyMapOutput failed for tasks " + Arrays.toString(failedTasks));
          for (InputAttemptIdentifier left : failedTasks) {
            scheduler.copyFailed(left, host, true, false, false);
          }
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
  boolean setupConnection(MapHost host, Collection<InputAttemptIdentifier> attempts)
      throws IOException {
    boolean connectSucceeded = false;
    try {
      StringBuilder baseURI = ShuffleUtils.constructBaseURIForShuffleHandler(host.getHost(),
          host.getPort(), host.getPartitionId(), host.getPartitionCount(), applicationId, dagId, sslShuffle);
      URL url = ShuffleUtils.constructInputURL(baseURI.toString(), attempts, httpConnectionParams.isKeepAlive());
      httpConnection = ShuffleUtils.getHttpConnection(asyncHttp, url, httpConnectionParams,
          logIdentifier, jobTokenSecretManager);
      connectSucceeded = httpConnection.connect();

      if (stopped) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Detected fetcher has been shutdown after connection establishment. Returning");
        }
        return false;
      }
      input = httpConnection.getInputStream();
      httpConnection.validate();
      return true;
    } catch (IOException | InterruptedException ie) {
      if (ie instanceof InterruptedException) {
        Thread.currentThread().interrupt(); //reset status
      }
      if (stopped) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not reporting fetch failure, since an Exception was caught after shutdown");
        }
        return false;
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
      for (InputAttemptIdentifier left : remaining.values()) {
        // Need to be handling temporary glitches ..
        // Report read error to the AM to trigger source failure heuristics
        scheduler.copyFailed(left, host, connectSucceeded, !connectSucceeded, false);
      }
      return false;
    }
  }

  @VisibleForTesting
  protected void putBackRemainingMapOutputs(MapHost host) {
    // Cycle through remaining MapOutputs
    boolean isFirst = true;
    InputAttemptIdentifier first = null;
    for (InputAttemptIdentifier left : remaining.values()) {
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

  private static class MapOutputStat {
    final InputAttemptIdentifier srcAttemptId;
    final long decompressedLength;
    final long compressedLength;
    final int forReduce;

    MapOutputStat(InputAttemptIdentifier srcAttemptId, long decompressedLength, long compressedLength, int forReduce) {
      this.srcAttemptId = srcAttemptId;
      this.decompressedLength = decompressedLength;
      this.compressedLength = compressedLength;
      this.forReduce = forReduce;
    }

    @Override
    public String toString() {
      return "id: " + srcAttemptId + ", decompressed length: " + decompressedLength + ", compressed length: " + compressedLength + ", reduce: " + forReduce;
    }
  }

  protected InputAttemptIdentifier[] copyMapOutput(MapHost host,
                                DataInputStream input, InputAttemptIdentifier inputAttemptIdentifier) throws FetcherReadTimeoutException {
    MapOutput mapOutput = null;
    InputAttemptIdentifier srcAttemptId = null;
    long decompressedLength = 0;
    long compressedLength = 0;
    try {
      long startTime = System.currentTimeMillis();
      int partitionCount = 1;

      if (this.compositeFetch) {
        // Multiple partitions are fetched
        partitionCount = WritableUtils.readVInt(input);
      }
      ArrayList<MapOutputStat> mapOutputStats = new ArrayList<>(partitionCount);
      for (int mapOutputIndex = 0; mapOutputIndex < partitionCount; mapOutputIndex++) {
        MapOutputStat mapOutputStat = null;
        try {
          //Read the shuffle header
          ShuffleHeader header = new ShuffleHeader();
          // TODO Review: Multiple header reads in case of status WAIT ?
          header.readFields(input);
          if (!header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX)) {
            if (!stopped) {
              badIdErrs.increment(1);
              LOG.warn("Invalid map id: " + header.mapId + ", expected to start with " +
                  InputAttemptIdentifier.PATH_PREFIX + ", partition: " + header.forReduce);
              return new InputAttemptIdentifier[]{getNextRemainingAttempt()};
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Already shutdown. Ignoring invalid map id error");
              }
              return EMPTY_ATTEMPT_ID_ARRAY;
            }
          }

          if (header.getCompressedLength() == 0) {
            // Empty partitions are already accounted for
            continue;
          }

          mapOutputStat = new MapOutputStat(scheduler.getIdentifierForFetchedOutput(header.mapId, header.forReduce),
              header.uncompressedLength,
              header.compressedLength,
              header.forReduce);
          mapOutputStats.add(mapOutputStat);
        } catch (IllegalArgumentException e) {
          if (!stopped) {
            badIdErrs.increment(1);
            LOG.warn("Invalid map id ", e);
            // Don't know which one was bad, so consider this one bad and dont read
            // the remaining because we dont know where to start reading from. YARN-1773
            return new InputAttemptIdentifier[]{getNextRemainingAttempt()};
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Already shutdown. Ignoring invalid map id error. Exception: " +
                  e.getClass().getName() + ", Message: " + e.getMessage());
            }
            return EMPTY_ATTEMPT_ID_ARRAY;
          }
        }

        // Do some basic sanity verification
        if (!verifySanity(mapOutputStat.compressedLength, mapOutputStat.decompressedLength, mapOutputStat.forReduce,
            remaining, mapOutputStat.srcAttemptId)) {
          if (!stopped) {
            srcAttemptId = mapOutputStat.srcAttemptId;
            if (srcAttemptId == null) {
              srcAttemptId = getNextRemainingAttempt();
              LOG.warn("Was expecting " + srcAttemptId + " but got null");
            }
            assert (srcAttemptId != null);
            return new InputAttemptIdentifier[]{srcAttemptId};
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Already stopped. Ignoring verification failure.");
            }
            return EMPTY_ATTEMPT_ID_ARRAY;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("header: " + mapOutputStat.srcAttemptId + ", len: " + mapOutputStat.compressedLength +
              ", decomp len: " + mapOutputStat.decompressedLength);
        }
      }

      for (MapOutputStat mapOutputStat : mapOutputStats) {
        // Get the location for the map output - either in-memory or on-disk
        srcAttemptId = mapOutputStat.srcAttemptId;
        decompressedLength = mapOutputStat.decompressedLength;
        compressedLength = mapOutputStat.compressedLength;
        try {
          mapOutput = allocator.reserve(srcAttemptId, decompressedLength, compressedLength, id);
        } catch (IOException e) {
          if (!stopped) {
            // Kill the reduce attempt
            ioErrs.increment(1);
            scheduler.reportLocalError(e);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Already stopped. Ignoring error from merger.reserve");
            }
          }
          return EMPTY_ATTEMPT_ID_ARRAY;
        }

        // Check if we can shuffle *now* ...
        if (mapOutput.getType() == Type.WAIT) {
          LOG.info("fetcher#" + id + " - MergerManager returned Status.WAIT ...");
          //Not an error but wait to process data.
          return EMPTY_ATTEMPT_ID_ARRAY;
        }

        // Go!
        if (LOG.isDebugEnabled()) {
          LOG.debug("fetcher#" + id + " about to shuffle output of map " +
              mapOutput.getAttemptIdentifier() + " decomp: " +
              decompressedLength + " len: " + compressedLength + " to " + mapOutput.getType());
        }

        if (mapOutput.getType() == Type.MEMORY) {
          ShuffleUtils.shuffleToMemory(mapOutput.getMemory(), input,
              (int) decompressedLength, (int) compressedLength, codec, ifileReadAhead,
              ifileReadAheadLength, LOG, mapOutput.getAttemptIdentifier());
        } else if (mapOutput.getType() == Type.DISK) {
          ShuffleUtils.shuffleToDisk(mapOutput.getDisk(), host.getHostIdentifier(),
              input, compressedLength, decompressedLength, LOG,
              mapOutput.getAttemptIdentifier(),
              ifileReadAhead, ifileReadAheadLength, verifyDiskChecksum);
        } else {
          throw new IOException("Unknown mapOutput type while fetching shuffle data:" +
              mapOutput.getType());
        }

        // Inform the shuffle scheduler
        long endTime = System.currentTimeMillis();
        // Reset retryStartTime as map task make progress if retried before.
        retryStartTime = 0;

        scheduler.copySucceeded(srcAttemptId, host, compressedLength, decompressedLength,
            endTime - startTime, mapOutput, false);
      }
      remaining.remove(inputAttemptIdentifier.toString());
    } catch(IOException ioe) {
      if (stopped) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not reporting fetch failure for exception during data copy: ["
              + ioe.getClass().getName() + ", " + ioe.getMessage() + "]");
        }
        cleanupCurrentConnection(true);
        if (mapOutput != null) {
          mapOutput.abort(); // Release resources
        }
        // Don't need to put back - since that's handled by the invoker
        return EMPTY_ATTEMPT_ID_ARRAY;
      }
      if (shouldRetry(host, ioe)) {
        //release mem/file handles
        if (mapOutput != null) {
          mapOutput.abort();
        }
        throw new FetcherReadTimeoutException(ioe);
      }
      ioErrs.increment(1);
      if (srcAttemptId == null || mapOutput == null) {
        LOG.info("fetcher#" + id + " failed to read map header" +
            srcAttemptId + " decomp: " +
            decompressedLength + ", " + compressedLength, ioe);
        if (srcAttemptId == null) {
          return remaining.values().toArray(new InputAttemptIdentifier[remaining.values().size()]);
        } else {
          return new InputAttemptIdentifier[]{srcAttemptId};
        }
      }
      LOG.warn("Failed to shuffle output of " + srcAttemptId +
          " from " + host.getHostIdentifier(), ioe);

      // Inform the shuffle-scheduler
      mapOutput.abort();
      return new InputAttemptIdentifier[] {srcAttemptId};
    }
    return null;
  }

  /**
   * Check connection needs to be re-established.
   *
   * @param host
   * @param ioe
   * @return true to indicate connection retry. false otherwise.
   * @throws IOException
   */
  private boolean shouldRetry(MapHost host, IOException ioe) {
    if (!(ioe instanceof SocketTimeoutException)) {
      return false;
    }
    // First time to retry.
    long currentTime = System.currentTimeMillis();
    if (retryStartTime == 0) {
      retryStartTime = currentTime;
    }

    if (currentTime - retryStartTime < httpConnectionParams.getReadTimeout()) {
      LOG.warn("Shuffle output from " + host.getHostIdentifier() +
          " failed, retry it.");
      //retry connecting to the host
      return true;
    } else {
      // timeout, prepare to be failed.
      LOG.warn("Timeout for copying MapOutput with retry on host " + host
          + "after " + httpConnectionParams.getReadTimeout() + "milliseconds.");
      return false;
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
      int forReduce, Map<String, InputAttemptIdentifier> remaining, InputAttemptIdentifier srcAttemptId) {
    if (compressedLength < 0 || decompressedLength < 0) {
      wrongLengthErrs.increment(1);
      LOG.warn(logIdentifier + " invalid lengths in map output header: id: " +
          srcAttemptId + " len: " + compressedLength + ", decomp len: " + 
               decompressedLength);
      return false;
    }

    // partitionId verification. Isn't availalbe here because it is encoded into
    // URI
    if (forReduce < minPartition || forReduce > maxPartition) {
      wrongReduceErrs.increment(1);
      LOG.warn(logIdentifier + " data for the wrong partition map: " + srcAttemptId + " len: "
          + compressedLength + " decomp len: " + decompressedLength + " for partition " + forReduce
          + ", expected partition range: " + minPartition + "-" + maxPartition);
      return false;
    }
    return true;
  }
  
  private InputAttemptIdentifier getNextRemainingAttempt() {
    if (remaining.size() > 0) {
      return remaining.values().iterator().next();
    } else {
      return null;
    }
  }

  @VisibleForTesting
  protected void setupLocalDiskFetch(MapHost host) throws InterruptedException {
    // Get completed maps on 'host'
    List<InputAttemptIdentifier> srcAttempts = scheduler.getMapsForHost(host);

    // Sanity check to catch hosts with only 'OBSOLETE' maps,
    // especially at the tail of large jobs
    if (srcAttempts.size() == 0) {
      return;
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + id + " going to fetch (local disk) from " + host + " for: "
          + srcAttempts + ", partition range: " + minPartition + "-" + maxPartition);
    }

    // List of maps to be fetched yet
    populateRemainingMap(srcAttempts);

    try {
      final Iterator<InputAttemptIdentifier> iter = remaining.values().iterator();
      while (iter.hasNext()) {
        // Avoid fetching more if already stopped
        if (stopped) {
          return;
        }
        InputAttemptIdentifier srcAttemptId = iter.next();
        MapOutput mapOutput = null;
        boolean hasFailures = false;
        // Fetch partition count number of map outputs (handles auto-reduce case)
        for (int curPartition = minPartition; curPartition <= maxPartition; curPartition++) {
          try {
            long startTime = System.currentTimeMillis();

            // Partition id is the base partition id plus the relative offset
            int reduceId = host.getPartitionId() + curPartition - minPartition;
            srcAttemptId = scheduler.getIdentifierForFetchedOutput(srcAttemptId.getPathComponent(), reduceId);
            Path filename = getShuffleInputFileName(srcAttemptId.getPathComponent(), null);
            TezIndexRecord indexRecord = getIndexRecord(srcAttemptId.getPathComponent(), reduceId);

            mapOutput = getMapOutputForDirectDiskFetch(srcAttemptId, filename, indexRecord);
            long endTime = System.currentTimeMillis();
            scheduler.copySucceeded(srcAttemptId, host, indexRecord.getPartLength(),
                indexRecord.getRawLength(), (endTime - startTime), mapOutput, true);
          } catch (IOException e) {
            if (mapOutput != null) {
              mapOutput.abort();
            }
            if (!stopped) {
              hasFailures = true;
              ioErrs.increment(1);
              scheduler.copyFailed(srcAttemptId, host, true, false, true);
              LOG.warn("Failed to read local disk output of " + srcAttemptId + " from " +
                  host.getHostIdentifier(), e);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Ignoring fetch error during local disk copy since fetcher has already been stopped");
              }
              return;
            }

          }
        }
        if (!hasFailures) {
          iter.remove();
        }
      }
    } finally {
      putBackRemainingMapOutputs(host);
    }

  }

  @VisibleForTesting
  //TODO: Refactor following to make use of methods from TezTaskOutputFiles to be consistent.
  protected Path getShuffleInputFileName(String pathComponent, String suffix)
      throws IOException {
    LocalDirAllocator localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    suffix = suffix != null ? suffix : "";
    String outputPath = Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR + Path.SEPARATOR +
        pathComponent + Path.SEPARATOR +
        Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING + suffix;
    String pathFromLocalDir = getPathForLocalDir(outputPath);

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
    return MapOutput.createLocalDiskMapOutput(srcAttemptId, allocator, filename,
        indexRecord.getStartOffset(), indexRecord.getPartLength(), true);
  }

  @VisibleForTesting
  void populateRemainingMap(List<InputAttemptIdentifier> origlist) {
    if (remaining == null) {
      remaining = new LinkedHashMap<String, InputAttemptIdentifier>(origlist.size());
    }
    for (InputAttemptIdentifier id : origlist) {
      remaining.put(id.toString(), id);
    }
  }

  private String getPathForLocalDir(String suffix) {
    if(ShuffleUtils.isTezShuffleHandler(conf)) {
      return Constants.DAG_PREFIX + dagId + Path.SEPARATOR + suffix;
    }
    return suffix;
  }
}

