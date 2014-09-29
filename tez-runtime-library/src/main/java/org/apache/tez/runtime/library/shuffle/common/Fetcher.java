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

package org.apache.tez.runtime.library.shuffle.common;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput.Type;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection.HttpConnectionParams;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * Responsible for fetching inputs served by the ShuffleHandler for a single
 * host. Construct using {@link FetcherBuilder}
 */
public class Fetcher implements Callable<FetchResult> {

  private static final Log LOG = LogFactory.getLog(Fetcher.class);

  private static final AtomicInteger fetcherIdGen = new AtomicInteger(0);
  private final Configuration conf;

  // Configurable fields.
  private CompressionCodec codec;

  private boolean ifileReadAhead = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT;
  private int ifileReadAheadLength = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT;
  
  private final SecretKey shuffleSecret;

  private final FetcherCallback fetcherCallback;
  private final FetchedInputAllocator inputManager;
  private final ApplicationId appId;
  
  private final String logIdentifier;
  
  private final AtomicBoolean isShutDown = new AtomicBoolean(false);

  private final int fetcherIdentifier;

  // Parameters to track work.
  private List<InputAttemptIdentifier> srcAttempts;
  private String host;
  private int port;
  private int partition;

  // Maps from the pathComponents (unique per srcTaskId) to the specific taskId
  private final Map<String, InputAttemptIdentifier> pathToAttemptMap;
  private LinkedHashSet<InputAttemptIdentifier> remaining;

  private URL url;
  private volatile DataInputStream input;
  
  private HttpConnection httpConnection;
  private HttpConnectionParams httpConnectionParams;

  private final boolean localDiskFetchEnabled;
  private final boolean sharedFetchEnabled;

  private final LocalDirAllocator localDirAllocator;
  private final Path lockPath;
  private final RawLocalFileSystem localFs;

  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  private Fetcher(FetcherCallback fetcherCallback, HttpConnectionParams params,
      FetchedInputAllocator inputManager, ApplicationId appId,
      SecretKey shuffleSecret, String srcNameTrimmed, Configuration conf,
      RawLocalFileSystem localFs,
      LocalDirAllocator localDirAllocator,
      Path lockPath,
      boolean localDiskFetchEnabled,
      boolean sharedFetchEnabled) {
    this.fetcherCallback = fetcherCallback;
    this.inputManager = inputManager;
    this.shuffleSecret = shuffleSecret;
    this.appId = appId;
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

    try {
      if (this.sharedFetchEnabled) {
        this.localFs.mkdirs(this.lockPath);
      }
    } catch (Exception e) {
      LOG.warn("Error initializing local dirs for shared transfer " + e);
    }
  }

  @Override
  public FetchResult call() throws Exception {
    boolean multiplex = (this.sharedFetchEnabled && this.localDiskFetchEnabled);

    if (srcAttempts.size() == 0) {
      return new FetchResult(host, port, partition, srcAttempts);
    }

    for (InputAttemptIdentifier in : srcAttempts) {
      pathToAttemptMap.put(in.getPathComponent(), in);
      // do only if all of them are shared fetches
      multiplex &= in.isShared();
    }

    if (multiplex) {
      Preconditions.checkArgument(partition == 0,
          "Shared fetches cannot be done for partitioned input"
              + "- partition is non-zero (%d)", partition);
    }

    remaining = new LinkedHashSet<InputAttemptIdentifier>(srcAttempts);

    HostFetchResult hostFetchResult;

    if (localDiskFetchEnabled &&
        host.equals(System.getenv(ApplicationConstants.Environment.NM_HOST.toString()))) {
      hostFetchResult = setupLocalDiskFetch();
    } else if (multiplex) {
      hostFetchResult = doSharedFetch();
    } else{
      hostFetchResult = doHttpFetch();
    }

    if (hostFetchResult.failedInputs != null && hostFetchResult.failedInputs.length > 0) {
      LOG.warn("copyInputs failed for tasks " + Arrays.toString(hostFetchResult.failedInputs));
      for (InputAttemptIdentifier left : hostFetchResult.failedInputs) {
        fetcherCallback.fetchFailed(host, left, hostFetchResult.connectFailed);
      }
    }

    shutdown();

    // Sanity check
    if (hostFetchResult.failedInputs == null && !remaining.isEmpty()) {
      if (!multiplex) {
        throw new IOException("server didn't return all expected map outputs: "
            + remaining.size() + " left.");
      } else {
        LOG.info("Shared fetch failed to return " + remaining.size() + " inputs on this try");
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
        final String tmpSuffix = "." + System.currentTimeMillis() + ".tmp";
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
          if (outputPath != null) {
            // invariant: outputPath was renamed from tmpPath
            localFs.delete(outputPath, false);
          }
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
    for (InputAttemptIdentifier src : srcAttempts) {
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

    if (inputs == srcAttempts.size()) {
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
        LOG.info("Requeuing " + host + ":" + port
            + " downloads because we didn't get a lock");
        return new HostFetchResult(new FetchResult(host, port, partition,
            remaining), null, false);
      } else {
        if (findInputs() == srcAttempts.size()) {
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
      // fall back to HTTP fetch below
      LOG.warn("Lock was interrupted for " + host);
    } finally {
      releaseLock(lock);
    }

    if (isShutDown.get()) {
      // if any exception was due to shut-down don't bother firing any more
      // requests
      return new HostFetchResult(new FetchResult(host, port, partition,
          remaining), null, false);
    }
    // no more caching
    return doHttpFetch();
  }

  @VisibleForTesting
  protected HostFetchResult doHttpFetch() {
    return doHttpFetch(null);
  }

  @VisibleForTesting
  protected HostFetchResult doHttpFetch(CachingCallBack callback) {
    try {
      StringBuilder baseURI = ShuffleUtils.constructBaseURIForShuffleHandler(host,
          port, partition, appId.toString(), httpConnectionParams.isSSLShuffleEnabled());
      this.url = ShuffleUtils.constructInputURL(baseURI.toString(), srcAttempts,
          httpConnectionParams.getKeepAlive());

      httpConnection = new HttpConnection(url, httpConnectionParams, logIdentifier, shuffleSecret);
      httpConnection.connect();
    } catch (IOException e) {
      // ioErrs.increment(1);
      // If connect did not succeed, just mark all the maps as failed,
      // indirectly penalizing the host
      InputAttemptIdentifier[] failedFetches = null;
      if (isShutDown.get()) {
        LOG.info("Not reporting fetch failure, since an Exception was caught after shutdown");
      } else {
        failedFetches = remaining.toArray(new InputAttemptIdentifier[remaining.size()]);
      }
      return new HostFetchResult(new FetchResult(host, port, partition, remaining), failedFetches, true);
    }
    if (isShutDown.get()) {
      // shutdown would have no effect if in the process of establishing the connection.
      shutdownInternal();
      LOG.info("Detected fetcher has been shutdown after connection establishment. Returning");
      return new HostFetchResult(new FetchResult(host, port, partition, remaining), null, false);
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
        LOG.info("Not reporting fetch failure, since an Exception was caught after shutdown");
      } else {
        InputAttemptIdentifier firstAttempt = srcAttempts.get(0);
        LOG.warn("Fetch Failure from host while connecting: " + host + ", attempt: " + firstAttempt
            + " Informing ShuffleManager: ", e);
        return new HostFetchResult(new FetchResult(host, port, partition, remaining),
            new InputAttemptIdentifier[] { firstAttempt }, false);
      }
    }

    // By this point, the connection is setup and the response has been
    // validated.

    // Handle any shutdown which may have been invoked.
    if (isShutDown.get()) {
      // shutdown would have no effect if in the process of establishing the connection.
      shutdownInternal();
      LOG.info("Detected fetcher has been shutdown after opening stream. Returning");
      return new HostFetchResult(new FetchResult(host, port, partition, remaining), null, false);
    }
    // After this point, closing the stream and connection, should cause a
    // SocketException,
    // which will be ignored since shutdown has been invoked.

    // Loop through available map-outputs and fetch them
    // On any error, faildTasks is not null and we exit
    // after putting back the remaining maps to the
    // yet_to_be_fetched list and marking the failed tasks.
    InputAttemptIdentifier[] failedInputs = null;
    while (!remaining.isEmpty() && failedInputs == null) {
      failedInputs = fetchInputs(input, callback);
    }

    return new HostFetchResult(new FetchResult(host, port, partition, remaining), failedInputs,
        false);
  }

  @VisibleForTesting
  protected HostFetchResult setupLocalDiskFetch() {
    return doLocalDiskFetch(true);
  }

  @VisibleForTesting
  private HostFetchResult doLocalDiskFetch(boolean failMissing) {

    Iterator<InputAttemptIdentifier> iterator = remaining.iterator();
    while (iterator.hasNext()) {
      InputAttemptIdentifier srcAttemptId = iterator.next();
      //TODO: check for shutdown? - See TEZ-1480
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
        LOG.info("fetcher" + " about to shuffle output of srcAttempt (direct disk)" + srcAttemptId
            + " decomp: " + idxRecord.getRawLength() + " len: " + idxRecord.getPartLength()
            + " to " + fetchedInput.getType());

        long endTime = System.currentTimeMillis();
        fetcherCallback.fetchSucceeded(host, srcAttemptId, fetchedInput, idxRecord.getPartLength(),
            idxRecord.getRawLength(), (endTime - startTime));
        iterator.remove();
      } catch (IOException e) {
        LOG.warn("Failed to shuffle output of " + srcAttemptId + " from " + host + "(local fetch)",
            e);
        if (fetchedInput != null) {
          try {
            fetchedInput.abort();
          } catch (IOException e1) {
            LOG.info("Failed to cleanup fetchedInput " + fetchedInput);
          }
        }
      }
    }

    InputAttemptIdentifier[] failedFetches = null;
    if (failMissing && remaining.size() > 0) {
      failedFetches = remaining.toArray(new InputAttemptIdentifier[remaining.size()]);
    } else {
      // nothing needs to be done to requeue remaining entries
    }
    return new HostFetchResult(new FetchResult(host, port, partition, remaining),
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
      shutdownInternal();
    }
  }

  private void shutdownInternal() {
    // Synchronizing on isShutDown to ensure we don't run into a parallel close
    // Can't synchronize on the main class itself since that would cause the
    // shutdown request to block
    synchronized (isShutDown) {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(false);
        }
      } catch (IOException e) {
        LOG.info("Exception while shutting down fetcher on " + logIdentifier + " : "
            + e.getMessage());
        if (LOG.isDebugEnabled()) {
          LOG.debug(e);
        }
      }
    }
  }

  private InputAttemptIdentifier[] fetchInputs(DataInputStream input, CachingCallBack callback) {
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
        LOG.warn("Invalid src id ", e);
        // Don't know which one was bad, so consider all of them as bad
        return remaining.toArray(new InputAttemptIdentifier[remaining.size()]);
      }

      // Do some basic sanity verification
      if (!verifySanity(compressedLength, decompressedLength,
          responsePartition, srcAttemptId, pathComponent)) {
        if (srcAttemptId == null) {
          LOG.warn("Was expecting " + getNextRemainingAttempt() + " but got null");
          srcAttemptId = getNextRemainingAttempt();
        }
        assert(srcAttemptId != null);
        return new InputAttemptIdentifier[] { srcAttemptId };
      }

      if (LOG.isDebugEnabled()) {
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
      // TODO NEWTEZ No concept of WAIT at the moment.
      // // Check if we can shuffle *now* ...
      // if (fetchedInput.getType() == FetchedInput.WAIT) {
      // LOG.info("fetcher#" + id +
      // " - MergerManager returned Status.WAIT ...");
      // //Not an error but wait to process data.
      // return EMPTY_ATTEMPT_ID_ARRAY;
      // }

      // Go!
      LOG.info("fetcher" + " about to shuffle output of srcAttempt "
          + fetchedInput.getInputAttemptIdentifier() + " decomp: "
          + decompressedLength + " len: " + compressedLength + " to "
          + fetchedInput.getType());

      if (fetchedInput.getType() == Type.MEMORY) {
        ShuffleUtils.shuffleToMemory(((MemoryFetchedInput) fetchedInput).getBytes(),
          input, (int) decompressedLength, (int) compressedLength, codec,
          ifileReadAhead, ifileReadAheadLength, LOG,
          fetchedInput.getInputAttemptIdentifier().toString());
      } else if (fetchedInput.getType() == Type.DISK) {
        ShuffleUtils.shuffleToDisk(((DiskFetchedInput) fetchedInput).getOutputStream(),
          (host +":" +port), input, compressedLength, LOG,
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
      fetcherCallback.fetchSucceeded(host, srcAttemptId, fetchedInput,
          compressedLength, decompressedLength, (endTime - startTime));

      // Note successful shuffle
      remaining.remove(srcAttemptId);

      // metrics.successFetch();
      return null;
    } catch (IOException ioe) {
      // ZZZ Add some shutdown code here
      // ZZZ Make sure any assigned memory inputs are aborted
      // ioErrs.increment(1);
      if (srcAttemptId == null || fetchedInput == null) {
        LOG.info("fetcher" + " failed to read map header" + srcAttemptId
            + " decomp: " + decompressedLength + ", " + compressedLength, ioe);
        if (srcAttemptId == null) {
          return remaining
              .toArray(new InputAttemptIdentifier[remaining.size()]);
        } else {
          return new InputAttemptIdentifier[] { srcAttemptId };
        }
      }
      LOG.warn("Failed to shuffle output of " + srcAttemptId + " from " + host,
          ioe);

      // Inform the shuffle-scheduler
      try {
        fetchedInput.abort();
      } catch (IOException e) {
        LOG.info("Failure to cleanup fetchedInput: " + fetchedInput);
      }
      // metrics.failedFetch();
      return new InputAttemptIdentifier[] { srcAttemptId };
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
    if (!remaining.contains(srcAttemptId)) {
      // wrongMapErrs.increment(1);
      LOG.warn("Invalid input. Received output for headerPathComponent: "
          + pathComponent + "nextRemainingSrcAttemptId: "
          + getNextRemainingAttempt() + ", mappedSrcAttemptId: " + srcAttemptId);
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

  /**
   * Builder for the construction of Fetchers
   */
  public static class FetcherBuilder {
    private Fetcher fetcher;
    private boolean workAssigned = false;

    public FetcherBuilder(FetcherCallback fetcherCallback,
        HttpConnectionParams params, FetchedInputAllocator inputManager,
        ApplicationId appId, SecretKey shuffleSecret, String srcNameTrimmed,
        Configuration conf, boolean localDiskFetchEnabled) {
      this.fetcher = new Fetcher(fetcherCallback, params, inputManager, appId,
          shuffleSecret, srcNameTrimmed, conf, null, null, null, localDiskFetchEnabled,
          false);
    }

    public FetcherBuilder(FetcherCallback fetcherCallback,
        HttpConnectionParams params, FetchedInputAllocator inputManager,
        ApplicationId appId, SecretKey shuffleSecret, String srcNameTrimmed,
        Configuration conf, RawLocalFileSystem localFs,
        LocalDirAllocator localDirAllocator, Path lockPath,
        boolean localDiskFetchEnabled, boolean sharedFetchEnabled) {
      this.fetcher = new Fetcher(fetcherCallback, params, inputManager, appId,
          shuffleSecret, srcNameTrimmed, conf, localFs, localDirAllocator,
          lockPath, localDiskFetchEnabled, sharedFetchEnabled);
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

