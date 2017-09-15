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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.Fetcher.FetcherBuilder;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.apache.tez.runtime.library.common.shuffle.HostPort;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.InputHost.PartitionToInputs;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

// This only knows how to deal with a single srcIndex for a given targetIndex.
// In case the src task generates multiple outputs for the same target Index
// (multiple src-indices), modifications will be required.
public class ShuffleManager implements FetcherCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManager.class);
  private static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  private static final FetchStatsLogger fetchStatsLogger = new FetchStatsLogger(LOG_FETCH, LOG);

  private final InputContext inputContext;
  private final int numInputs;

  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final FetchedInputAllocator inputManager;

  @VisibleForTesting
  final ListeningExecutorService fetcherExecutor;

  private final ListeningExecutorService schedulerExecutor;
  private final RunShuffleCallable schedulerCallable;
  
  private final BlockingQueue<FetchedInput> completedInputs;
  private final AtomicBoolean inputReadyNotificationSent = new AtomicBoolean(false);
  @VisibleForTesting
  final BitSet completedInputSet;
  private final ConcurrentMap<HostPort, InputHost> knownSrcHosts;
  private final BlockingQueue<InputHost> pendingHosts;
  private final Set<InputAttemptIdentifier> obsoletedInputs;
  private Set<Fetcher> runningFetchers;
  
  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);
  private final AtomicInteger numFetchedSpills = new AtomicInteger(0);

  private final long startTime;
  private long lastProgressTime;
  private long totalBytesShuffledTillNow;

  // Required to be held when manipulating pendingHosts
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();
  
  private final int numFetchers;
  private final boolean asyncHttp;
  
  // Parameters required by Fetchers
  private final JobTokenSecretManager jobTokenSecretMgr;
  private final CompressionCodec codec;
  private final boolean localDiskFetchEnabled;
  private final boolean sharedFetchEnabled;
  private final boolean verifyDiskChecksum;
  private final boolean compositeFetch;
  
  private final int ifileBufferSize;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  
  private final String srcNameTrimmed;

  private final int maxTaskOutputAtOnce;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final TezCounter shuffledInputsCounter;
  private final TezCounter failedShufflesCounter;
  private final TezCounter bytesShuffledCounter;
  private final TezCounter decompressedDataSizeCounter;
  private final TezCounter bytesShuffledToDiskCounter;
  private final TezCounter bytesShuffledToMemCounter;
  private final TezCounter bytesShuffledDirectDiskCounter;
  
  private volatile Throwable shuffleError;
  private final HttpConnectionParams httpConnectionParams;
  

  private final LocalDirAllocator localDirAllocator;
  private final RawLocalFileSystem localFs;
  private final Path[] localDisks;
  private final String localhostName;
  private final int shufflePort;

  private final TezCounter shufflePhaseTime;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;

  //To track shuffleInfo events when finalMerge is disabled OR pipelined shuffle is enabled in source.
  @VisibleForTesting
  final Map<Integer, ShuffleEventInfo> shuffleInfoEventsMap;

  // TODO More counters - FetchErrors, speed?
  
  public ShuffleManager(InputContext inputContext, Configuration conf, int numInputs,
      int bufferSize, boolean ifileReadAheadEnabled, int ifileReadAheadLength,
      CompressionCodec codec, FetchedInputAllocator inputAllocator) throws IOException {
    this.inputContext = inputContext;
    this.numInputs = numInputs;
    
    this.shuffledInputsCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    this.failedShufflesCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.decompressedDataSizeCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.bytesShuffledToDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.bytesShuffledToMemCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);
    this.bytesShuffledDirectDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
  
    this.ifileBufferSize = bufferSize;
    this.ifileReadAhead = ifileReadAheadEnabled;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.codec = codec;
    this.inputManager = inputAllocator;
    this.localDiskFetchEnabled = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);
    this.sharedFetchEnabled = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT);
    this.verifyDiskChecksum = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT);

    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);
    this.firstEventReceived = inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived = inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);
    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
    
    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
  
    completedInputSet = new BitSet(numInputs);
    /**
     * In case of pipelined shuffle, it is possible to get multiple FetchedInput per attempt.
     * We do not know upfront the number of spills from source.
     */
    completedInputs = new LinkedBlockingDeque<FetchedInput>();
    knownSrcHosts = new ConcurrentHashMap<HostPort, InputHost>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();
    obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());
    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<Fetcher, Boolean>());

    int maxConfiguredFetchers = 
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    
    this.numFetchers = Math.min(maxConfiguredFetchers, numInputs);

    final ExecutorService fetcherRawExecutor;
    if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL_DEFAULT)) {
      fetcherRawExecutor = inputContext.createTezFrameworkExecutorService(numFetchers,
          "Fetcher_B {" + srcNameTrimmed + "} #%d");
    } else {
      fetcherRawExecutor = Executors.newFixedThreadPool(numFetchers, new ThreadFactoryBuilder()
          .setDaemon(true).setNameFormat("Fetcher_B {" + srcNameTrimmed + "} #%d").build());
    }
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);

    ExecutorService schedulerRawExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ShuffleRunner {" + srcNameTrimmed + "}").build());
    this.schedulerExecutor = MoreExecutors.listeningDecorator(schedulerRawExecutor);
    this.schedulerCallable = new RunShuffleCallable(conf);
    
    this.startTime = System.currentTimeMillis();
    this.lastProgressTime = startTime;

    String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    SecretKey shuffleSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(auxiliaryService));
    this.jobTokenSecretMgr = new JobTokenSecretManager(shuffleSecret);
    this.asyncHttp = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_USE_ASYNC_HTTP, false);
    httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf);

    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();

    this.localDirAllocator = new LocalDirAllocator(
        TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    this.localDisks = Iterables.toArray(
        localDirAllocator.getAllLocalPathsToRead(".", conf), Path.class);
    this.localhostName = inputContext.getExecutionContext().getHostName();
    final ByteBuffer shuffleMetaData =
        inputContext.getServiceProviderMetaData(auxiliaryService);
    this.shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetaData);

    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would
     * be approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce = Math.max(1, Math.min(75, conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));

    Arrays.sort(this.localDisks);

    shuffleInfoEventsMap = new ConcurrentHashMap<Integer, ShuffleEventInfo>();

    LOG.info(srcNameTrimmed + ": numInputs=" + numInputs + ", compressionCodec="
        + (codec == null ? "NoCompressionCodec" : codec.getClass().getName()) + ", numFetchers="
        + numFetchers + ", ifileBufferSize=" + ifileBufferSize + ", ifileReadAheadEnabled="
        + ifileReadAhead + ", ifileReadAheadLength=" + ifileReadAheadLength +", "
        + "localDiskFetchEnabled=" + localDiskFetchEnabled + ", "
        + "sharedFetchEnabled=" + sharedFetchEnabled + ", "
        + httpConnectionParams.toString() + ", maxTaskOutputAtOnce=" + maxTaskOutputAtOnce);
  }

  public void run() throws IOException {
    Preconditions.checkState(inputManager != null, "InputManager must be configured");

    ListenableFuture<Void> runShuffleFuture = schedulerExecutor.submit(schedulerCallable);
    Futures.addCallback(runShuffleFuture, new SchedulerFutureCallback());
    // Shutdown this executor once this task, and the callback complete.
    schedulerExecutor.shutdown();
  }
  
  private class RunShuffleCallable extends CallableWithNdc<Void> {

    private final Configuration conf;

    public RunShuffleCallable(Configuration conf) {
      this.conf = conf;
    }

    @Override
    protected Void callInternal() throws Exception {
      while (!isShutdown.get() && numCompletedInputs.get() < numInputs) {
        lock.lock();
        try {
          while ((runningFetchers.size() >= numFetchers || pendingHosts.isEmpty())
              && numCompletedInputs.get() < numInputs) {
            inputContext.notifyProgress();
            boolean ret = wakeLoop.await(1000, TimeUnit.MILLISECONDS);
          }
        } finally {
          lock.unlock();
        }

        if (shuffleError != null) {
          // InputContext has already been informed of a fatal error. Relying on
          // tez to kill the task.
          break;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(srcNameTrimmed + ": " + "NumCompletedInputs: " + numCompletedInputs);
        }
        if (numCompletedInputs.get() < numInputs && !isShutdown.get()) {
          lock.lock();
          try {
            int maxFetchersToRun = numFetchers - runningFetchers.size();
            int count = 0;
            while (pendingHosts.peek() != null && !isShutdown.get()) {
              InputHost inputHost = null;
              try {
                inputHost = pendingHosts.take();
              } catch (InterruptedException e) {
                if (isShutdown.get()) {
                  LOG.info(srcNameTrimmed + ": " + "Interrupted and hasBeenShutdown, Breaking out of ShuffleScheduler Loop");
                  Thread.currentThread().interrupt();
                  break;
                } else {
                  throw e;
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug(srcNameTrimmed + ": " + "Processing pending host: " +
                    inputHost.toDetailedString());
              }
              if (inputHost.getNumPendingPartitions() > 0 && !isShutdown.get()) {
                Fetcher fetcher = constructFetcherForHost(inputHost, conf);
                runningFetchers.add(fetcher);
                if (isShutdown.get()) {
                  LOG.info(srcNameTrimmed + ": " + "hasBeenShutdown," +
                      "Breaking out of ShuffleScheduler Loop");
                  break;
                }
                ListenableFuture<FetchResult> future = fetcherExecutor
                    .submit(fetcher);
                Futures.addCallback(future, new FetchFutureCallback(fetcher));
                if (++count >= maxFetchersToRun) {
                  break;
                }
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(srcNameTrimmed + ": " + "Skipping host: " +
                      inputHost.getIdentifier() +
                      " since it has no inputs to process");
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      }
      shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);
      LOG.info(srcNameTrimmed + ": " + "Shutting down FetchScheduler, Was Interrupted: " + Thread.currentThread().isInterrupted());
      if (!fetcherExecutor.isShutdown()) {
        fetcherExecutor.shutdownNow();
      }
      return null;
    }
  }

  private boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    //For pipelined shuffle.
    //TODO: TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        if (eventInfo.scheduledForDownload || !eventInfo.eventsProcessed.isEmpty()) {
          IOException exception = new IOException("Previous event already got scheduled for " +
              input + ". Previous attempt's data could have been already merged "
              + "to memory/disk outputs.  Killing (self) this task early."
              + " currentAttemptNum=" + eventInfo.attemptNum
              + ", eventsProcessed=" + eventInfo.eventsProcessed
              + ", scheduledForDownload=" + eventInfo.scheduledForDownload
              + ", newAttemptNum=" + input.getAttemptNumber());
          String message = "Killing self as previous attempt data could have been consumed";
          killSelf(exception, message);
          return false;
        }
      }
    }
    return true;
  }

  void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    this.inputContext.killSelf(exception, message);
  }

  @VisibleForTesting
  Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {

    Path lockDisk = null;

    if (sharedFetchEnabled) {
      // pick a single lock disk from the edge name's hashcode + host hashcode
      final int h = Math.abs(Objects.hashCode(this.srcNameTrimmed, inputHost.getHost()));
      lockDisk = new Path(this.localDisks[h % this.localDisks.length], "locks");
    }

    FetcherBuilder fetcherBuilder = new FetcherBuilder(ShuffleManager.this,
      httpConnectionParams, inputManager, inputContext.getApplicationId(), inputContext.getDagIdentifier(),
        jobTokenSecretMgr, srcNameTrimmed, conf, localFs, localDirAllocator,
        lockDisk, localDiskFetchEnabled, sharedFetchEnabled,
        localhostName, shufflePort, asyncHttp, verifyDiskChecksum, compositeFetch);

    if (codec != null) {
      fetcherBuilder.setCompressionParameters(codec);
    }
    fetcherBuilder.setIFileParams(ifileReadAhead, ifileReadAheadLength);

    // Remove obsolete inputs from the list being given to the fetcher. Also
    // remove from the obsolete list.
    PartitionToInputs pendingInputsOfOnePartitionRange = inputHost
        .clearAndGetOnePartitionRange();
    int includedMaps = 0;
    for (Iterator<InputAttemptIdentifier> inputIter =
        pendingInputsOfOnePartitionRange.getInputs().iterator();
            inputIter.hasNext();) {
      InputAttemptIdentifier input = inputIter.next();

      //For pipelined shuffle.
      if (!validateInputAttemptForPipelinedShuffle(input)) {
        continue;
      }

      // Avoid adding attempts which have already completed.
      boolean alreadyCompleted;
      if (input instanceof CompositeInputAttemptIdentifier) {
        CompositeInputAttemptIdentifier compositeInput = (CompositeInputAttemptIdentifier)input;
        int nextClearBit = completedInputSet.nextClearBit(compositeInput.getInputIdentifier());
        int maxClearBit = compositeInput.getInputIdentifier() + compositeInput.getInputIdentifierCount();
        alreadyCompleted = nextClearBit > maxClearBit;
      } else {
          alreadyCompleted = completedInputSet.get(input.getInputIdentifier());
      }
      // Avoid adding attempts which have already completed or have been marked as OBSOLETE
      if (alreadyCompleted || obsoletedInputs.contains(input)) {
        inputIter.remove();
        continue;
      }

      // Check if max threshold is met
      if (includedMaps >= maxTaskOutputAtOnce) {
        inputIter.remove();
        //add to inputHost
        inputHost.addKnownInput(pendingInputsOfOnePartitionRange.getPartition(),
            pendingInputsOfOnePartitionRange.getPartitionCount(), input);
      } else {
        includedMaps++;
      }
    }
    if (inputHost.getNumPendingPartitions() > 0) {
      pendingHosts.add(inputHost); //add it to queue
    }
    for(InputAttemptIdentifier input : pendingInputsOfOnePartitionRange.getInputs()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null) {
        eventInfo.scheduledForDownload = true;
      }
    }
    fetcherBuilder.assignWork(inputHost.getHost(), inputHost.getPort(),
        pendingInputsOfOnePartitionRange.getPartition(),
        pendingInputsOfOnePartitionRange.getPartitionCount(),
            pendingInputsOfOnePartitionRange.getInputs());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Fetcher for host: " + inputHost.getHost()
          + ", info: " + inputHost.getAdditionalInfo()
          + ", with inputs: " + pendingInputsOfOnePartitionRange);
    }
    return fetcherBuilder.build();
  }
  
  /////////////////// Methods for InputEventHandler
  
  public void addKnownInput(String hostName, int port,
                            CompositeInputAttemptIdentifier srcAttemptIdentifier, int srcPhysicalIndex) {
    HostPort identifier = new HostPort(hostName, port);
    InputHost host = knownSrcHosts.get(identifier);
    if (host == null) {
      host = new InputHost(identifier);
      InputHost old = knownSrcHosts.putIfAbsent(identifier, host);
      if (old != null) {
        host = old;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(srcNameTrimmed + ": " + "Adding input: " +
          srcAttemptIdentifier + ", to host: " + host);
    }

    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
      if (shuffleInfoEventsMap.get(inputIdentifier + i) == null) {
        shuffleInfoEventsMap.put(inputIdentifier + i, new ShuffleEventInfo(srcAttemptIdentifier.expand(i)));
      }
    }

    host.addKnownInput(srcPhysicalIndex, srcAttemptIdentifier.getInputIdentifierCount(), srcAttemptIdentifier);
    lock.lock();
    try {
      boolean added = pendingHosts.offer(host);
      if (!added) {
        String errorMessage = "Unable to add host: " +
            host.getIdentifier() + " to pending queue";
        LOG.error(errorMessage);
        throw new TezUncheckedException(errorMessage);
      }
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
  }

  public void addCompletedInputWithNoData(
      InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    }
    lock.lock();
    try {
      if (!completedInputSet.get(inputIdentifier)) {
        NullFetchedInput fetchedInput = new NullFetchedInput(srcAttemptIdentifier);
        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }
      }
      // Awake the loop to check for termination.
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
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

  void obsoleteKnownInput(InputAttemptIdentifier srcAttemptIdentifier) {
    obsoletedInputs.add(srcAttemptIdentifier);
    // TODO NEWTEZ Maybe inform the fetcher about this. For now, this is used during the initial fetch list construction.
  }

  /////////////////// End of Methods for InputEventHandler
  /////////////////// Methods from FetcherCallbackHandler

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same
   * attempt.
   */
  static class ShuffleEventInfo {
    BitSet eventsProcessed;
    int finalEventId = -1; //0 indexed
    int attemptNum;
    String id;
    boolean scheduledForDownload; // whether chunks got scheduled for download


    ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier() + "_" + input.getAttemptNumber();
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("finalEventId=" + finalEventId + ", eventsProcessed cardinality=" +
            eventsProcessed.cardinality());
      }
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    public String toString() {
      return "[eventsProcessed=" + eventsProcessed + ", finalEventId=" + finalEventId
          +  ", id=" + id + ", attemptNum=" + attemptNum
          + ", scheduledForDownload=" + scheduledForDownload + "]";
    }
  }

  @Override
  public void fetchSucceeded(String host, InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput, long fetchedBytes, long decompressedLength, long copyDuration)
      throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    // Count irrespective of whether this is a copy of an already fetched input
    lock.lock();
    try {
      lastProgressTime = System.currentTimeMillis();
      inputContext.notifyProgress();
      if (!completedInputSet.get(inputIdentifier)) {
        fetchedInput.commit();
        fetchStatsLogger.logIndividualFetchComplete(copyDuration,
            fetchedBytes, decompressedLength, fetchedInput.getType().toString(), srcAttemptIdentifier);

        // Processing counters for completed and commit fetches only. Need
        // additional counters for excessive fetches - which primarily comes
        // in after speculation or retries.
        shuffledInputsCounter.increment(1);
        bytesShuffledCounter.increment(fetchedBytes);
        if (fetchedInput.getType() == Type.MEMORY) {
          bytesShuffledToMemCounter.increment(fetchedBytes);
        } else if (fetchedInput.getType() == Type.DISK) {
          bytesShuffledToDiskCounter.increment(fetchedBytes);
        } else if (fetchedInput.getType() == Type.DISK_DIRECT) {
          bytesShuffledDirectDiskCounter.increment(fetchedBytes);
        }
        decompressedDataSizeCounter.increment(decompressedLength);

        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }

        totalBytesShuffledTillNow += fetchedBytes;
        logProgress();
        wakeLoop.signal();
      } else {
        fetchedInput.abort(); // If this fails, the fetcher may attempt another abort.
      }
    } finally {
      lock.unlock();
    }
    // TODO NEWTEZ Maybe inform fetchers, in case they have an alternate attempt of the same task in their queue.
  }

  private void registerCompletedInput(FetchedInput fetchedInput) {
    lock.lock();
    try {
      maybeInformInputReady(fetchedInput);
      adjustCompletedInputs(fetchedInput);
      numFetchedSpills.getAndIncrement();
    } finally {
      lock.unlock();
    }
  }

  private void maybeInformInputReady(FetchedInput fetchedInput) {
    lock.lock();
    try {
      if (!(fetchedInput instanceof NullFetchedInput)) {
        completedInputs.add(fetchedInput);
      }
      if (!inputReadyNotificationSent.getAndSet(true)) {
        // TODO Should eventually be controlled by Inputs which are processing the data.
        inputContext.inputIsReady();
      }
    } finally {
      lock.unlock();
    }
  }

  private void adjustCompletedInputs(FetchedInput fetchedInput) {
    lock.lock();
    try {
      completedInputSet.set(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());

      int numComplete = numCompletedInputs.incrementAndGet();
      if (numComplete == numInputs) {
        // Poison pill End of Input message to awake blocking take call
        if (fetchedInput instanceof NullFetchedInput) {
          completedInputs.add(fetchedInput);
        }
        LOG.info("All inputs fetched for input vertex : " + inputContext.getSourceVertexName());
      }
    } finally {
      lock.unlock();
    }
  }

  private void registerCompletedInputForPipelinedShuffle(InputAttemptIdentifier
      srcAttemptIdentifier, FetchedInput fetchedInput) {
    /**
     * For pipelinedshuffle it is possible to get multiple spills. Claim success only when
     * all spills pertaining to an attempt are done.
     */
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

    //for empty partition case
    if (eventInfo == null && fetchedInput instanceof NullFetchedInput) {
      eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
      shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
    }

    assert(eventInfo != null);
    eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
    numFetchedSpills.getAndIncrement();

    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    lock.lock();
    try {
      /**
       * When fetch is complete for a spill, add it to completedInputs to ensure that it is
       * available for downstream processing. Final success will be claimed only when all
       * spills are downloaded from the source.
       */
      maybeInformInputReady(fetchedInput);


      //check if we downloaded all spills pertaining to this InputAttemptIdentifier
      if (eventInfo.isDone()) {
        adjustCompletedInputs(fetchedInput);
        shuffleInfoEventsMap.remove(srcAttemptIdentifier.getInputIdentifier());
      }
    } finally {
      lock.unlock();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("eventInfo " + eventInfo.toString());
    }
  }

  private void reportFatalError(Throwable exception, String message) {
    LOG.error(message);
    inputContext.reportFailure(TaskFailureType.NON_FATAL, exception, message);
  }

  @Override
  public void fetchFailed(String host,
      InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
    // TODO NEWTEZ. Implement logic to report fetch failures after a threshold.
    // For now, reporting immediately.
    LOG.info(srcNameTrimmed + ": " + "Fetch failed for src: " + srcAttemptIdentifier
        + "InputIdentifier: " + srcAttemptIdentifier + ", connectFailed: "
        + connectFailed);
    failedShufflesCounter.increment(1);
    inputContext.notifyProgress();
    if (srcAttemptIdentifier == null) {
      reportFatalError(null, "Received fetchFailure for an unknown src (null)");
    } else {
    InputReadErrorEvent readError = InputReadErrorEvent.create(
        "Fetch failure while fetching from "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(),
            srcAttemptIdentifier.getInputIdentifier(),
            srcAttemptIdentifier.getAttemptNumber()),
        srcAttemptIdentifier.getInputIdentifier(),
        srcAttemptIdentifier.getAttemptNumber());

    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(readError);
    inputContext.sendEvents(failedEvents);
    }
  }
  /////////////////// End of Methods from FetcherCallbackHandler

  public void shutdown() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      //TODO: need to cleanup all FetchedInput (DiskFetchedInput, LocalDisFetchedInput), lockFile
      //As of now relying on job cleanup (when all directories would be cleared)
      LOG.info(srcNameTrimmed + ": " + "Thread interrupted. Need to cleanup the local dirs");
    }
    if (!isShutdown.getAndSet(true)) {
      // Shut down any pending fetchers
      LOG.info("Shutting down pending fetchers on source" + srcNameTrimmed + ": "
          + runningFetchers.size());
      lock.lock();
      try {
        wakeLoop.signal(); // signal the fetch-scheduler
        for (Fetcher fetcher : runningFetchers) {
          try {
            fetcher.shutdown(); // This could be parallelized.
          } catch (Exception e) {
            LOG.warn(
                "Error while stopping fetcher during shutdown. Ignoring and continuing. Message={}",
                e.getMessage());
          }
        }
      } finally {
        lock.unlock();
      }

      if (this.schedulerExecutor != null && !this.schedulerExecutor.isShutdown()) {
        this.schedulerExecutor.shutdownNow();
      }
      if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
        this.fetcherExecutor.shutdownNow(); // Interrupts all running fetchers.
      }
    }
  }

  /**
   * @return true if all of the required inputs have been fetched.
   */
  public boolean allInputsFetched() {
    lock.lock();
    try {
      return numCompletedInputs.get() == numInputs;
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return the next available input, or null if there are no available inputs.
   *         This method will block if there are currently no available inputs,
   *         but more may become available.
   */
  public FetchedInput getNextInput() throws InterruptedException {
    // Check for no additional inputs
    lock.lock();
    try {
      if (completedInputs.peek() == null && allInputsFetched()) {
        return null;
      }
    } finally {
      lock.unlock();
    }
    // Block until next input or End of Input message
    FetchedInput fetchedInput = completedInputs.take();
    if (fetchedInput instanceof NullFetchedInput) {
      fetchedInput = null;
    }
    return fetchedInput;
  }

  public int getNumInputs() {
    return numInputs;
  }

  public float getNumCompletedInputsFloat() {
    return numCompletedInputs.floatValue();
  }

  /////////////////// End of methods for walking the available inputs


  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   *
   */
  @VisibleForTesting
  static class NullFetchedInput extends FetchedInput {

    public NullFetchedInput(InputAttemptIdentifier inputAttemptIdentifier) {
      super(inputAttemptIdentifier, null);
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }

    @Override
    public long getSize() {
      return -1;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public InputStream getInputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void commit() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void abort() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void free() {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }
  }

  private final AtomicInteger nextProgressLineEventCount = new AtomicInteger(0);

  private void logProgress() {
    int inputsDone = numCompletedInputs.get();

    if (inputsDone > nextProgressLineEventCount.get() || inputsDone == numInputs) {
      nextProgressLineEventCount.addAndGet(50);
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

      double transferRate = mbs / secsSinceStart;
      LOG.info("copy(" + inputsDone + " (spillsFetched=" + numFetchedSpills.get() + ") of " +
          numInputs +
          ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) "
          + mbpsFormat.format(transferRate) + " MB/s)");

    }
  }


  private class SchedulerFutureCallback implements FutureCallback<Void> {

    @Override
    public void onSuccess(Void result) {
      LOG.info(srcNameTrimmed + ": " + "Scheduler thread completed");
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(srcNameTrimmed + ": " + "Already shutdown. Ignoring error: " + t);
        }
      } else {
        LOG.error(srcNameTrimmed + ": " + "Scheduler failed with error: ", t);
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Shuffle Scheduler Failed");
      }
    }
    
  }
  
  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private final Fetcher fetcher;
    
    public FetchFutureCallback(Fetcher fetcher) {
      this.fetcher = fetcher;
    }
    
    private void doBookKeepingForFetcherComplete() {
      lock.lock();
      try {
        runningFetchers.remove(fetcher);
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }
    
    @Override
    public void onSuccess(FetchResult result) {
      fetcher.shutdown();
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(srcNameTrimmed + ": " + "Already shutdown. Ignoring event from fetcher");
        }
      } else {
        Iterable<InputAttemptIdentifier> pendingInputs = result.getPendingInputs();
        if (pendingInputs != null && pendingInputs.iterator().hasNext()) {
          HostPort identifier = new HostPort(result.getHost(),
              result.getPort());
          InputHost inputHost = knownSrcHosts.get(identifier);
          assert inputHost != null;
          for (InputAttemptIdentifier input : pendingInputs) {
            inputHost.addKnownInput(result.getPartition(), result.getPartitionCount(), input);
          }
          inputHost.setAdditionalInfo(result.getAdditionalInfo());
          pendingHosts.add(inputHost);
        }
        doBookKeepingForFetcherComplete();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Unsuccessful - the fetcher may not have shutdown correctly. Try shutting it down.
      fetcher.shutdown();
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(srcNameTrimmed + ": " + "Already shutdown. Ignoring error from fetcher: " + t);
        }
      } else {
        LOG.error(srcNameTrimmed + ": " + "Fetcher failed with error: ", t);
        shuffleError = t;
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Fetch failed");
        doBookKeepingForFetcherComplete();
      }
    }
  }
}

