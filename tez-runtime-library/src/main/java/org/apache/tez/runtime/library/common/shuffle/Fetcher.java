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

package org.apache.tez.runtime.library.common.shuffle;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.exceptions.FetcherReadTimeoutException;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;

import com.google.common.base.Preconditions;

/**
 * Responsible for fetching inputs served by the ShuffleHandler for a single
 * host. Construct using {@link FetcherBuilder}
 */
public class Fetcher extends CallableWithNdc<FetchResult> {

  private static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

  private static final AtomicInteger fetcherIdGen = new AtomicInteger(0);
  private final Configuration conf;
  private final int shufflePort;

  // Configurable fields.
  private CompressionCodec codec;

  private boolean ifileReadAhead = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT;
  private int ifileReadAheadLength = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT;
  
  private final JobTokenSecretManager jobTokenSecretMgr;

  private final FetcherCallback fetcherCallback;
  private final FetchedInputAllocator inputManager;
  private final ApplicationId appId;
  private final int dagIdentifier;
  
  private final String logIdentifier;

  private final String localHostname;
  
  private final AtomicBoolean isShutDown = new AtomicBoolean(false);

  private final int fetcherIdentifier;

  // Parameters to track work.
  private List<InputAttemptIdentifier> srcAttempts;
  @VisibleForTesting
  Map<String, InputAttemptIdentifier> srcAttemptsRemaining;
  private String host;
  private int port;
  private int partition;

  // Maps from the pathComponents (unique per srcTaskId) to the specific taskId
  private final Map<String, InputAttemptIdentifier> pathToAttemptMap;

  private URL url;
  private volatile DataInputStream input;
  
  BaseHttpConnection httpConnection;
  private HttpConnectionParams httpConnectionParams;

  private final boolean localDiskFetchEnabled;
  private final boolean sharedFetchEnabled;

  private final LocalDirAllocator localDirAllocator;
  private final Path lockPath;
  private final RawLocalFileSystem localFs;

  // Initiative value is 0, which means it hasn't retried yet.
  private long retryStartTime = 0;

  private final boolean asyncHttp;

  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  private Fetcher(FetcherCallback fetcherCallback, HttpConnectionParams params,
      FetchedInputAllocator inputManager, ApplicationId appId, int dagIdentifier,
      JobTokenSecretManager jobTokenSecretManager, String srcNameTrimmed, Configuration conf,
      RawLocalFileSystem localFs,
      LocalDirAllocator localDirAllocator,
      Path lockPath,
      boolean localDiskFetchEnabled,
      boolean sharedFetchEnabled,
      String localHostname,
      int shufflePort, boolean asyncHttp) {
    this.asyncHttp = asyncHttp;
    this.fetcherCallback = fetcherCallback;
    this.inputManager = inputManager;
    this.jobTokenSecretMgr = jobTokenSecretManager;
    this.appId = appId;
    this.dagIdentifier = dagIdentifier;
    this.pathToAttemptMap = new HashMap<String, InputAttemptIdentifier>();
    this.httpConnectionParams = params;
    this.conf = conf;

    this.localDiskFetchEnabled = localDiskFetchEnabled;
    this.sharedFetchEnabled = sharedFetchEnabled;

    this.fetcherIdentifier = fetcherIdGen.getAndIncrement();
    this.logIdentifier = " fetcher [" + srcNameTrimmed +"] " + fetcherIdentifier;

    this.localFs = localFs;
    this.localDirAllocator = localDirAllocator;
    this.lockPath = lockPath;
    this.localHostname = localHostname;
    this.shufflePort = shufflePort;

    try {
      if (this.sharedFetchEnabled) {
        this.localFs.mkdirs(this.lockPath);
      }
    } catch (Exception e) {
      LOG.warn("Error initializing local dirs for shared transfer " + e);
    }
  }

  // helper method to populate the remaining map
  void populateRemainingMap(List<InputAttemptIdentifier> origlist) {
    if (srcAttemptsRemaining == null) {
      srcAttemptsRemaining = new LinkedHashMap<String, InputAttemptIdentifier>(origlist.size());
    }
    for (InputAttemptIdentifier id : origlist) {
      srcAttemptsRemaining.put(id.toString(), id);
    }
  }

  @Override
  protected FetchResult callInternal() throws Exception {
    boolean multiplex = (this.sharedFetchEnabled && this.localDiskFetchEnabled);

    if (srcAttempts.size() == 0) {
      return new FetchResult(host, port, partition, srcAttempts);
    }

    populateRemainingMap(srcAttempts);
    for (InputAttemptIdentifier in : srcAttemptsRemaining.values()) {
      pathToAttemptMap.put(in.getPathComponent(), in);
      // do only if all of them are shared fetches
      multiplex &= in.isShared();
    }

    if (multiplex) {
      Preconditions.checkArgument(partition == 0,
          "Shared fetches cannot be done for partitioned input"
              + "- partition is non-zero (%d)", partition);
    }

    HostFetchResult hostFetchResult;

    if (localDiskFetchEnabled && host.equals(localHostname) && port == shufflePort) {
      hostFetchResult = setupLocalDiskFetch();
    } else if (multiplex) {
      hostFetchResult = doSharedFetch();
    } else{
      hostFetchResult = doHttpFetch();
    }

    if (hostFetchResult.failedInputs != null && hostFetchResult.failedInputs.length > 0) {
      if (!isShutDown.get()) {
        LOG.warn("copyInputs failed for tasks " + Arrays.toString(hostFetchResult.failedInputs));
        for (InputAttemptIdentifier left : hostFetchResult.failedInputs) {
          fetcherCallback.fetchFailed(host, left, hostFetchResult.connectFailed);
        }
      } else {
        if (isDebugEnabled) {
          LOG.debug("Ignoring failed fetch reports for " + hostFetchResult.failedInputs.length +
              " inputs since the fetcher has already been stopped");
        }
      }
    }

    shutdown();

    // Sanity check
    if (hostFetchResult.failedInputs == null && !srcAttemptsRemaining.isEmpty()) {
      if (!multiplex) {
        throw new IOException("server didn't return all expected map outputs: "
            + srcAttemptsRemaining.size() + " left.");
      }
    }

    return hostFetchResult.fetchResult;
  }

  private final class CachingCallBack {
    // this is a closure object wrapping this in an inner class
    public void cache(String host,
        InputAttemptIdentifier srcAttemptId, FetchedInput fetchedInput,
        long compressedLength, long decompressedLength) {
      try {
        // this breaks badly on partitioned input - please use responsibly
        Preconditions.checkArgument(partition == 0, "Partition == 0");
        final String tmpSuffix = System.currentTimeMillis() + ".tmp";
        final String finalOutput = getMapOutputFile(srcAttemptId.getPathComponent());
        final Path outputPath = localDirAllocator.getLocalPathForWrite(finalOutput, compressedLength, conf);
        final TezSpillRecord spillRec = new TezSpillRecord(1);
        final TezIndexRecord indexRec;
        Path tmpIndex = outputPath.suffix(Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING+tmpSuffix);

        if (localFs.exists(tmpIndex)) {
          LOG.warn("Found duplicate instance of input index file " + tmpIndex);
          return;
        }

        Path tmpPath = null;

        switch (fetchedInput.getType()) {
        case DISK: {
          DiskFetchedInput input = (DiskFetchedInput) fetchedInput;
          indexRec = new TezIndexRecord(0, decompressedLength, compressedLength);
          localFs.mkdirs(outputPath.getParent());
          // avoid pit-falls of speculation
          tmpPath = outputPath.suffix(tmpSuffix);
          // JDK7 - TODO: use Files implementation to speed up this process
          localFs.copyFromLocalFile(input.getInputPath(), tmpPath);
          // rename is atomic
          boolean renamed = localFs.rename(tmpPath, outputPath);
          if(!renamed) {
            LOG.warn("Could not rename to cached file name " + outputPath);
            localFs.delete(tmpPath, false);
            return;
          }
        }
        break;
        default:
          LOG.warn("Incorrect use of CachingCallback for " + srcAttemptId);
          return;
        }

        spillRec.putIndex(indexRec, 0);
        spillRec.writeToFile(tmpIndex, conf);
        // everything went well so far - rename it
        boolean renamed = localFs.rename(tmpIndex, outputPath
            .suffix(Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING));
        if (!renamed) {
          localFs.delete(tmpIndex, false);
          // invariant: outputPath was renamed from tmpPath
          localFs.delete(outputPath, false);
          LOG.warn("Could not rename the index file to "
              + outputPath
                  .suffix(Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING));
          return;
        }
      } catch (IOException ioe) {
        // do mostly nothing
        LOG.warn("Cache threw an error " + ioe);
      }
    }
  }

  private int findInputs() throws IOException {
    int k = 0;
    for (InputAttemptIdentifier src : srcAttemptsRemaining.values()) {
      try {
        if (getShuffleInputFileName(src.getPathComponent(),
            Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING) != null) {
          k++;
        }
      } catch (DiskErrorException de) {
        // missing file, ignore
      }
    }
    return k;
  }

  private FileLock getLock() throws OverlappingFileLockException, InterruptedException, IOException {
    File lockFile = localFs.pathToFile(new Path(lockPath, host + ".lock"));

    final boolean created = lockFile.createNewFile();

    if (created == false && !lockFile.exists()) {
      // bail-out cleanly
      return null;
    }

    // invariant - file created (winner writes to this file)
    // caveat: closing lockChannel does close the file (do not double close)
    // JDK7 - TODO: use AsynchronousFileChannel instead of RandomAccessFile
    FileChannel lockChannel = new RandomAccessFile(lockFile, "rws")
        .getChannel();
    FileLock xlock = null;

    xlock = lockChannel.tryLock(0, Long.MAX_VALUE, false);
    if (xlock != null) {
      return xlock;
    }
    lockChannel.close();
    return null;
  }

  private void releaseLock(FileLock lock) throws IOException {
    if (lock != null && lock.isValid()) {
      FileChannel lockChannel = lock.channel();
      lock.release();
      lockChannel.close();
    }
  }

  protected HostFetchResult doSharedFetch() throws IOException {
    int inputs = findInputs();

    if (inputs == srcAttemptsRemaining.size()) {
      if (isDebugEnabled) {
        LOG.debug("Using the copies found locally");
      }
      return doLocalDiskFetch(true);
    }

    if (inputs > 0) {
      if (isDebugEnabled) {
        LOG.debug("Found " + input
            + " local fetches right now, using them first");
      }
      return doLocalDiskFetch(false);
    }

    FileLock lock = null;
    try {
      lock = getLock();
      if (lock == null) {
        // re-queue until we get a lock
        return new HostFetchResult(new FetchResult(host, port, partition,
            srcAttemptsRemaining.values(), "Requeuing as we didn't get a lock"), null, false);
      } else {
        if (findInputs() == srcAttemptsRemaining.size()) {
          // double checked after lock
          releaseLock(lock);
          lock = null;
          return doLocalDiskFetch(true);
        }
        // cache data if possible
        return doHttpFetch(new CachingCallBack());
      }
    } catch (OverlappingFileLockException jvmCrossLock) {
      // fall back to HTTP fetch below
      LOG.warn("Double locking detected for " + host);
    } catch (InterruptedException sleepInterrupted) {
      Thread.currentThread().interrupt();
      // fall back to HTTP fetch below
      LOG.warn("Lock was interrupted for " + host);
    } finally {
      releaseLock(lock);
    }

    if (isShutDown.get()) {
      // if any exception was due to shut-down don't bother firing any more
      // requests
      return new HostFetchResult(new FetchResult(host, port, partition,
          srcAttemptsRemaining.values()), null, false);
    }
    // no more caching
    return doHttpFetch();
  }

  @VisibleForTesting
  protected HostFetchResult doHttpFetch() {
    return doHttpFetch(null);
  }

  private HostFetchResult setupConnection(Collection<InputAttemptIdentifier> attempts) {
    try {
      StringBuilder baseURI = ShuffleUtils.constructBaseURIForShuffleHandler(host,
          port, partition, appId.toString(), dagIdentifier, httpConnectionParams.isSslShuffle());
      this.url = ShuffleUtils.constructInputURL(baseURI.toString(), attempts,
          httpConnectionParams.isKeepAlive());

      httpConnection = ShuffleUtils.getHttpConnection(asyncHttp, url, httpConnectionParams,
          logIdentifier, jobTokenSecretMgr);
      httpConnection.connect();
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      // ioErrs.increment(1);
      // If connect did not succeed, just mark all the maps as failed,
      // indirectly penalizing the host
      InputAttemptIdentifier[] failedFetches = null;
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug(
              "Not reporting fetch failure during connection establishment, since an Exception was caught after shutdown." +
                  e.getClass().getName() + ", Message: " + e.getMessage());
        }
      } else {
        failedFetches = srcAttemptsRemaining.values().
            toArray(new InputAttemptIdentifier[srcAttemptsRemaining.values().size()]);
      }
      return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()), failedFetches, true);
    }
    if (isShutDown.get()) {
      // shutdown would have no effect if in the process of establishing the connection.
      shutdownInternal();
      if (isDebugEnabled) {
        LOG.debug("Detected fetcher has been shutdown after connection establishment. Returning");
      }
      return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()), null, false);
    }

    try {
      input = httpConnection.getInputStream();
      httpConnection.validate();
      //validateConnectionResponse(msgToEncode, encHash);
    } catch (IOException e) {
      // ioErrs.increment(1);
      // If we got a read error at this stage, it implies there was a problem
      // with the first map, typically lost map. So, penalize only that map
      // and add the rest
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug(
              "Not reporting fetch failure during connection establishment, since an Exception was caught after shutdown." +
                  e.getClass().getName() + ", Message: " + e.getMessage());
        }
      } else {
        InputAttemptIdentifier firstAttempt = attempts.iterator().next();
        LOG.warn("Fetch Failure from host while connecting: " + host + ", attempt: " + firstAttempt
            + " Informing ShuffleManager: ", e);
        return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()),
            new InputAttemptIdentifier[] { firstAttempt }, false);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); //reset status
      return null;
    }
    return null;
  }

  @VisibleForTesting
  protected HostFetchResult doHttpFetch(CachingCallBack callback) {

    HostFetchResult connectionsWithRetryResult =
        setupConnection(srcAttemptsRemaining.values());
    if (connectionsWithRetryResult != null) {
      return connectionsWithRetryResult;
    }
    // By this point, the connection is setup and the response has been
    // validated.

    // Handle any shutdown which may have been invoked.
    if (isShutDown.get()) {
      // shutdown would have no effect if in the process of establishing the connection.
      shutdownInternal();
      if (isDebugEnabled) {
        LOG.debug("Detected fetcher has been shutdown after opening stream. Returning");
      }
      return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()), null, false);
    }
    // After this point, closing the stream and connection, should cause a
    // SocketException,
    // which will be ignored since shutdown has been invoked.

    // Loop through available map-outputs and fetch them
    // On any error, faildTasks is not null and we exit
    // after putting back the remaining maps to the
    // yet_to_be_fetched list and marking the failed tasks.
    InputAttemptIdentifier[] failedInputs = null;
    while (!srcAttemptsRemaining.isEmpty() && failedInputs == null) {
      if (isShutDown.get()) {
        shutdownInternal(true);
        if (isDebugEnabled) {
          LOG.debug("Fetcher already shutdown. Aborting queued fetches for " +
              srcAttemptsRemaining.size() + " inputs");
        }
        return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()), null,
            false);
      }
      try {
        failedInputs = fetchInputs(input, callback);
      } catch (FetcherReadTimeoutException e) {
        //clean up connection
        shutdownInternal(true);
        if (isShutDown.get()) {
          if (isDebugEnabled) {
            LOG.debug("Fetcher already shutdown. Aborting reconnection and queued fetches for " +
                srcAttemptsRemaining.size() + " inputs");
          }
          return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()), null,
              false);
        }
        // Connect again.
        connectionsWithRetryResult = setupConnection(srcAttemptsRemaining.values());
        if (connectionsWithRetryResult != null) {
          break;
        }
      }
    }

    if (isShutDown.get() && failedInputs != null && failedInputs.length > 0) {
      if (isDebugEnabled) {
        LOG.debug("Fetcher already shutdown. Not reporting fetch failures for: " +
            failedInputs.length + " failed inputs");
      }
      failedInputs = null;
    }
    return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()), failedInputs,
        false);
  }

  @VisibleForTesting
  protected HostFetchResult setupLocalDiskFetch() {
    return doLocalDiskFetch(true);
  }

  @VisibleForTesting
  private HostFetchResult doLocalDiskFetch(boolean failMissing) {

    Iterator<Entry<String, InputAttemptIdentifier>> iterator = srcAttemptsRemaining.entrySet().iterator();
    while (iterator.hasNext()) {
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug(
              "Already shutdown. Skipping fetch for " + srcAttemptsRemaining.size() + " inputs");
        }
        break;
      }
      InputAttemptIdentifier srcAttemptId = iterator.next().getValue();
      long startTime = System.currentTimeMillis();

      FetchedInput fetchedInput = null;
      try {
        TezIndexRecord idxRecord;
        // for missing files, this will throw an exception
        idxRecord = getTezIndexRecord(srcAttemptId);

        fetchedInput = new LocalDiskFetchedInput(idxRecord.getStartOffset(),
            idxRecord.getRawLength(), idxRecord.getPartLength(), srcAttemptId,
            getShuffleInputFileName(srcAttemptId.getPathComponent(), null), conf,
            new FetchedInputCallback() {
              @Override
              public void fetchComplete(FetchedInput fetchedInput) {}

              @Override
              public void fetchFailed(FetchedInput fetchedInput) {}

              @Override
              public void freeResources(FetchedInput fetchedInput) {}
            });
        if (isDebugEnabled) {
          LOG.debug("fetcher" + " about to shuffle output of srcAttempt (direct disk)" + srcAttemptId
              + " decomp: " + idxRecord.getRawLength() + " len: " + idxRecord.getPartLength()
              + " to " + fetchedInput.getType());
        }

        long endTime = System.currentTimeMillis();
        fetcherCallback.fetchSucceeded(host, srcAttemptId, fetchedInput, idxRecord.getPartLength(),
            idxRecord.getRawLength(), (endTime - startTime));
        iterator.remove();
      } catch (IOException e) {
        cleanupFetchedInput(fetchedInput);
        if (isShutDown.get()) {
          if (isDebugEnabled) {
            LOG.debug(
                "Already shutdown. Ignoring Local Fetch Failure for " + srcAttemptId +
                    " from host " +
                    host + " : " + e.getClass().getName() + ", message=" + e.getMessage());
          }
          break;
        }
        if (failMissing) {
          LOG.warn(
              "Failed to shuffle output of " + srcAttemptId + " from " + host + "(local fetch)",
              e);
        }
      }
    }

    InputAttemptIdentifier[] failedFetches = null;
    if (failMissing && srcAttemptsRemaining.size() > 0) {
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug(
              "Already shutdown, not reporting fetch failures for: " + srcAttemptsRemaining.size() +
                  " remaining inputs");
        }
      } else {
        failedFetches = srcAttemptsRemaining.values().
            toArray(new InputAttemptIdentifier[srcAttemptsRemaining.values().size()]);
      }
    } else {
      // nothing needs to be done to requeue remaining entries
    }
    return new HostFetchResult(new FetchResult(host, port, partition, srcAttemptsRemaining.values()),
        failedFetches, false);
  }

  @VisibleForTesting
  protected TezIndexRecord getTezIndexRecord(InputAttemptIdentifier srcAttemptId) throws
      IOException {
    TezIndexRecord idxRecord;
    Path indexFile = getShuffleInputFileName(srcAttemptId.getPathComponent(),
        Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);

    TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
    idxRecord = spillRecord.getIndex(partition);
    return idxRecord;
  }

  private static final String getMapOutputFile(String pathComponent) {
    return Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR + Path.SEPARATOR
        + pathComponent + Path.SEPARATOR
        + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING;
  }

  @VisibleForTesting
  protected Path getShuffleInputFileName(String pathComponent, String suffix)
      throws IOException {
    suffix = suffix != null ? suffix : "";

    String pathFromLocalDir = getMapOutputFile(pathComponent) + suffix;
    return localDirAllocator.getLocalPathToRead(pathFromLocalDir, conf);
  }

  static class HostFetchResult {
    private final FetchResult fetchResult;
    private final InputAttemptIdentifier[] failedInputs;
    private final boolean connectFailed;

    public HostFetchResult(FetchResult fetchResult, InputAttemptIdentifier[] failedInputs,
                           boolean connectFailed) {
      this.fetchResult = fetchResult;
      this.failedInputs = failedInputs;
      this.connectFailed = connectFailed;
    }
  }

  public void shutdown() {
    if (!isShutDown.getAndSet(true)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Shutting down fetcher for host: " + host);
      }
      shutdownInternal();
    }
  }

  private void shutdownInternal() {
    shutdownInternal(false);
  }

  private void shutdownInternal(boolean disconnect) {
    // Synchronizing on isShutDown to ensure we don't run into a parallel close
    // Can't synchronize on the main class itself since that would cause the
    // shutdown request to block
    synchronized (isShutDown) {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(disconnect);
        }
      } catch (IOException e) {
        LOG.info("Exception while shutting down fetcher on " + logIdentifier + " : "
            + e.getMessage());
        if (isDebugEnabled) {
          LOG.debug(StringUtils.EMPTY, e);
        }
      }
    }
  }

  private InputAttemptIdentifier[] fetchInputs(DataInputStream input,
      CachingCallBack callback) throws FetcherReadTimeoutException {
    FetchedInput fetchedInput = null;
    InputAttemptIdentifier srcAttemptId = null;
    long decompressedLength = -1;
    long compressedLength = -1;

    try {
      long startTime = System.currentTimeMillis();
      int responsePartition = -1;
      // Read the shuffle header
      String pathComponent = null;
      try {
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(input);
        pathComponent = header.getMapId();

        srcAttemptId = pathToAttemptMap.get(pathComponent);
        compressedLength = header.getCompressedLength();
        decompressedLength = header.getUncompressedLength();
        responsePartition = header.getPartition();
      } catch (IllegalArgumentException e) {
        // badIdErrs.increment(1);
        if (!isShutDown.get()) {
          LOG.warn("Invalid src id ", e);
          // Don't know which one was bad, so consider all of them as bad
          return srcAttemptsRemaining.values().toArray(new InputAttemptIdentifier[srcAttemptsRemaining.size()]);
        } else {
          if (isDebugEnabled) {
            LOG.debug("Already shutdown. Ignoring badId error with message: " + e.getMessage());
          }
          return null;
        }
      }

      // Do some basic sanity verification
      if (!verifySanity(compressedLength, decompressedLength,
          responsePartition, srcAttemptId, pathComponent)) {
        if (!isShutDown.get()) {
          if (srcAttemptId == null) {
            LOG.warn("Was expecting " + getNextRemainingAttempt() + " but got null");
            srcAttemptId = getNextRemainingAttempt();
          }
          assert (srcAttemptId != null);
          return new InputAttemptIdentifier[]{srcAttemptId};
        } else {
          if (isDebugEnabled) {
            LOG.debug("Already shutdown. Ignoring verification failure.");
          }
          return null;
        }
      }

      if (isDebugEnabled) {
        LOG.debug("header: " + srcAttemptId + ", len: " + compressedLength
            + ", decomp len: " + decompressedLength);
      }
      
      // TODO TEZ-957. handle IOException here when Broadcast has better error checking
      if (srcAttemptId.isShared() && callback != null) {
        // force disk if input is being shared
        fetchedInput = inputManager.allocateType(Type.DISK, decompressedLength,
            compressedLength, srcAttemptId);
      } else {
        fetchedInput = inputManager.allocate(decompressedLength,
            compressedLength, srcAttemptId);
      }
      // No concept of WAIT at the moment.
      // // Check if we can shuffle *now* ...
      // if (fetchedInput.getType() == FetchedInput.WAIT) {
      // LOG.info("fetcher#" + id +
      // " - MergerManager returned Status.WAIT ...");
      // //Not an error but wait to process data.
      // return EMPTY_ATTEMPT_ID_ARRAY;
      // }

      // Go!
      if (isDebugEnabled) {
        LOG.debug("fetcher" + " about to shuffle output of srcAttempt "
            + fetchedInput.getInputAttemptIdentifier() + " decomp: "
            + decompressedLength + " len: " + compressedLength + " to "
            + fetchedInput.getType());
      }

      if (fetchedInput.getType() == Type.MEMORY) {
        ShuffleUtils.shuffleToMemory(((MemoryFetchedInput) fetchedInput).getBytes(),
          input, (int) decompressedLength, (int) compressedLength, codec,
          ifileReadAhead, ifileReadAheadLength, LOG,
          fetchedInput.getInputAttemptIdentifier().toString());
      } else if (fetchedInput.getType() == Type.DISK) {
        ShuffleUtils.shuffleToDisk(((DiskFetchedInput) fetchedInput).getOutputStream(),
          (host +":" +port), input, compressedLength, decompressedLength, LOG,
          fetchedInput.getInputAttemptIdentifier().toString());
      } else {
        throw new TezUncheckedException("Bad fetchedInput type while fetching shuffle data " +
            fetchedInput);
      }

      // offer the fetched input for caching
      if (srcAttemptId.isShared() && callback != null) {
        // this has to be before the fetchSucceeded, because that goes across
        // threads into the reader thread and can potentially shutdown this thread
        // while it is still caching.
        callback.cache(host, srcAttemptId, fetchedInput, compressedLength, decompressedLength);
      }

      // Inform the shuffle scheduler
      long endTime = System.currentTimeMillis();
      // Reset retryStartTime as map task make progress if retried before.
      retryStartTime = 0;
      fetcherCallback.fetchSucceeded(host, srcAttemptId, fetchedInput,
          compressedLength, decompressedLength, (endTime - startTime));

      // Note successful shuffle
      srcAttemptsRemaining.remove(srcAttemptId.toString());

      // metrics.successFetch();
      return null;
    } catch (IOException ioe) {
      if (isShutDown.get()) {
        cleanupFetchedInput(fetchedInput);
        if (isDebugEnabled) {
          LOG.debug(
              "Already shutdown. Ignoring exception during fetch " + ioe.getClass().getName() +
                  ", Message: " + ioe.getMessage());
        }
        return null;
      }
      if (shouldRetry(srcAttemptId, ioe)) {
        //release mem/file handles
        cleanupFetchedInput(fetchedInput);
        throw new FetcherReadTimeoutException(ioe);
      }
      // ioErrs.increment(1);
      if (srcAttemptId == null || fetchedInput == null) {
        LOG.info("fetcher" + " failed to read map header" + srcAttemptId
            + " decomp: " + decompressedLength + ", " + compressedLength, ioe);
        // Cleanup the fetchedInput before returning.
        cleanupFetchedInput(fetchedInput);
        if (srcAttemptId == null) {
          return srcAttemptsRemaining.values()
              .toArray(new InputAttemptIdentifier[srcAttemptsRemaining.size()]);
        } else {
          return new InputAttemptIdentifier[] { srcAttemptId };
        }
      }
      LOG.warn("Failed to shuffle output of " + srcAttemptId + " from " + host,
          ioe);

      // Cleanup the fetchedInput
      cleanupFetchedInput(fetchedInput);
      // metrics.failedFetch();
      return new InputAttemptIdentifier[] { srcAttemptId };
    }
  }

  private void cleanupFetchedInput(FetchedInput fetchedInput) {
    if (fetchedInput != null) {
      try {
        fetchedInput.abort();
      } catch (IOException e) {
        LOG.info("Failure to cleanup fetchedInput: " + fetchedInput);
      }
    }
  }

  /**
   * Check connection needs to be re-established.
   *
   * @param srcAttemptId
   * @param ioe
   * @return true to indicate connection retry. false otherwise.
   * @throws IOException
   */
  private boolean shouldRetry(InputAttemptIdentifier srcAttemptId, IOException ioe) {
    if (!(ioe instanceof SocketTimeoutException)) {
      return false;
    }
    // First time to retry.
    long currentTime = System.currentTimeMillis();
    if (retryStartTime == 0) {
      retryStartTime = currentTime;
    }

    if (currentTime - retryStartTime < httpConnectionParams.getReadTimeout()) {
      LOG.warn("Shuffle output from " + srcAttemptId +
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
   * 
   * @param compressedLength
   * @param decompressedLength
   * @param fetchPartition
   * @param srcAttemptId
   * @param pathComponent
   * @return true/false, based on if the verification succeeded or not
   */
  private boolean verifySanity(long compressedLength, long decompressedLength,
      int fetchPartition, InputAttemptIdentifier srcAttemptId, String pathComponent) {
    if (compressedLength < 0 || decompressedLength < 0) {
      // wrongLengthErrs.increment(1);
      LOG.warn(" invalid lengths in input header -> headerPathComponent: "
          + pathComponent + ", nextRemainingSrcAttemptId: "
          + getNextRemainingAttempt() + ", mappedSrcAttemptId: " + srcAttemptId
          + " len: " + compressedLength + ", decomp len: " + decompressedLength);
      return false;
    }

    if (fetchPartition != this.partition) {
      // wrongReduceErrs.increment(1);
      LOG.warn(" data for the wrong reduce -> headerPathComponent: "
          + pathComponent + "nextRemainingSrcAttemptId: "
          + getNextRemainingAttempt() + ", mappedSrcAttemptId: " + srcAttemptId
          + " len: " + compressedLength + " decomp len: " + decompressedLength
          + " for reduce " + fetchPartition);
      return false;
    }

    // Sanity check
    // we are guaranteed that key is not null
    if (srcAttemptsRemaining.get(srcAttemptId.toString()) == null) {
      // wrongMapErrs.increment(1);
      LOG.warn("Invalid input. Received output for headerPathComponent: "
          + pathComponent + "nextRemainingSrcAttemptId: "
          + getNextRemainingAttempt() + ", mappedSrcAttemptId: " + srcAttemptId);
      return false;
    }
    return true;
  }
  
  private InputAttemptIdentifier getNextRemainingAttempt() {
    if (srcAttemptsRemaining.size() > 0) {
      return srcAttemptsRemaining.values().iterator().next();
    } else {
      return null;
    }
  }

  /**
   * Builder for the construction of Fetchers
   */
  public static class FetcherBuilder {
    private Fetcher fetcher;
    private boolean workAssigned = false;

    public FetcherBuilder(FetcherCallback fetcherCallback,
        HttpConnectionParams params, FetchedInputAllocator inputManager,
        ApplicationId appId, int dagIdentifier,  JobTokenSecretManager jobTokenSecretMgr, String srcNameTrimmed,
        Configuration conf, boolean localDiskFetchEnabled, String localHostname, int shufflePort,
        boolean asyncHttp) {
      this.fetcher = new Fetcher(fetcherCallback, params, inputManager, appId, dagIdentifier,
          jobTokenSecretMgr, srcNameTrimmed, conf, null, null, null, localDiskFetchEnabled,
          false, localHostname, shufflePort, asyncHttp);
    }

    public FetcherBuilder(FetcherCallback fetcherCallback,
        HttpConnectionParams params, FetchedInputAllocator inputManager,
        ApplicationId appId, int dagIdentifier, JobTokenSecretManager jobTokenSecretMgr, String srcNameTrimmed,
        Configuration conf, RawLocalFileSystem localFs,
        LocalDirAllocator localDirAllocator, Path lockPath,
        boolean localDiskFetchEnabled, boolean sharedFetchEnabled,
        String localHostname, int shufflePort, boolean asyncHttp) {
      this.fetcher = new Fetcher(fetcherCallback, params, inputManager, appId, dagIdentifier,
          jobTokenSecretMgr, srcNameTrimmed, conf, localFs, localDirAllocator,
          lockPath, localDiskFetchEnabled, sharedFetchEnabled, localHostname, shufflePort, asyncHttp);
    }

    public FetcherBuilder setHttpConnectionParameters(HttpConnectionParams httpParams) {
      fetcher.httpConnectionParams = httpParams;
      return this;
    }

    public FetcherBuilder setCompressionParameters(CompressionCodec codec) {
      fetcher.codec = codec;
      return this;
    }

    public FetcherBuilder setIFileParams(boolean readAhead, int readAheadBytes) {
      fetcher.ifileReadAhead = readAhead;
      fetcher.ifileReadAheadLength = readAheadBytes;
      return this;
    }

    public FetcherBuilder assignWork(String host, int port, int partition,
        List<InputAttemptIdentifier> inputs) {
      fetcher.host = host;
      fetcher.port = port;
      fetcher.partition = partition;
      fetcher.srcAttempts = inputs;
      workAssigned = true;
      return this;
    }

    public Fetcher build() {
      Preconditions.checkState(workAssigned == true,
          "Cannot build a fetcher withot assigning work to it");
      return fetcher;
    }
  }

  @Override
  public int hashCode() {
    return fetcherIdentifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Fetcher other = (Fetcher) obj;
    if (fetcherIdentifier != other.fetcherIdentifier)
      return false;
    return true;
  }
}

