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

package org.apache.tez.runtime.library.shuffle.common.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.shuffle.common.FetchResult;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput;
import org.apache.tez.runtime.library.shuffle.common.FetchedInputAllocator;
import org.apache.tez.runtime.library.shuffle.common.Fetcher;
import org.apache.tez.runtime.library.shuffle.common.FetcherCallback;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection.HttpConnectionParams;
import org.apache.tez.runtime.library.shuffle.common.InputHost;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput.Type;
import org.apache.tez.runtime.library.shuffle.common.Fetcher.FetcherBuilder;

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

  private static final Log LOG = LogFactory.getLog(ShuffleManager.class);
  
  private final InputContext inputContext;
  private final int numInputs;

  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final FetchedInputAllocator inputManager;

  private final ListeningExecutorService fetcherExecutor;

  private final ListeningExecutorService schedulerExecutor;
  private final RunShuffleCallable schedulerCallable;
  
  private final BlockingQueue<FetchedInput> completedInputs;
  private final AtomicBoolean inputReadyNotificationSent = new AtomicBoolean(false);
  private final Set<InputIdentifier> completedInputSet;
  private final ConcurrentMap<String, InputHost> knownSrcHosts;
  private final BlockingQueue<InputHost> pendingHosts;
  private final Set<InputAttemptIdentifier> obsoletedInputs;
  private Set<Fetcher> runningFetchers;
  
  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);
  
  private final long startTime;
  private long lastProgressTime;
  private long totalBytesShuffledTillNow;

  // Required to be held when manipulating pendingHosts
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();
  
  private final int numFetchers;
  
  // Parameters required by Fetchers
  private final SecretKey shuffleSecret;
  private final CompressionCodec codec;
  private final boolean localDiskFetchEnabled;
  private final boolean sharedFetchEnabled;
  
  private final int ifileBufferSize;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  
  private final String srcNameTrimmed; 
  
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
    
    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
  
    completedInputSet = Collections.newSetFromMap(new ConcurrentHashMap<InputIdentifier, Boolean>(numInputs));
    completedInputs = new LinkedBlockingQueue<FetchedInput>(numInputs);
    knownSrcHosts = new ConcurrentHashMap<String, InputHost>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();
    obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());
    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<Fetcher, Boolean>());

    int maxConfiguredFetchers = 
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES, 
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    
    this.numFetchers = Math.min(maxConfiguredFetchers, numInputs);
    
    ExecutorService fetcherRawExecutor = Executors.newFixedThreadPool(
        numFetchers,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Fetcher [" + srcNameTrimmed + "] #%d").build());
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);
    
    ExecutorService schedulerRawExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ShuffleRunner [" + srcNameTrimmed + "]").build());
    this.schedulerExecutor = MoreExecutors.listeningDecorator(schedulerRawExecutor);
    this.schedulerCallable = new RunShuffleCallable(conf);
    
    this.startTime = System.currentTimeMillis();
    this.lastProgressTime = startTime;
    
    this.shuffleSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID));
    httpConnectionParams =
        ShuffleUtils.constructHttpShuffleConnectionParams(conf);

    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();

    this.localDirAllocator = new LocalDirAllocator(
        TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    this.localDisks = Iterables.toArray(
        localDirAllocator.getAllLocalPathsToRead(".", conf), Path.class);

    Arrays.sort(this.localDisks);

    LOG.info(this.getClass().getSimpleName() + " : numInputs=" + numInputs + ", compressionCodec="
        + (codec == null ? "NoCompressionCodec" : codec.getClass().getName()) + ", numFetchers="
        + numFetchers + ", ifileBufferSize=" + ifileBufferSize + ", ifileReadAheadEnabled="
        + ifileReadAhead + ", ifileReadAheadLength=" + ifileReadAheadLength +", "
        + "localDiskFetchEnabled=" + localDiskFetchEnabled + ", "
        + "sharedFetchEnabled=" + sharedFetchEnabled + ", "
        + httpConnectionParams.toString());
  }

  public void run() throws IOException {
    Preconditions.checkState(inputManager != null, "InputManager must be configured");

    ListenableFuture<Void> runShuffleFuture = schedulerExecutor.submit(schedulerCallable);
    Futures.addCallback(runShuffleFuture, new SchedulerFutureCallback());
    // Shutdown this executor once this task, and the callback complete.
    schedulerExecutor.shutdown();
  }
  
  private class RunShuffleCallable implements Callable<Void> {

    private final Configuration conf;

    public RunShuffleCallable(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Void call() throws Exception {
      while (!isShutdown.get() && numCompletedInputs.get() < numInputs) {
        lock.lock();
        try {
          if (runningFetchers.size() >= numFetchers || pendingHosts.isEmpty()) {
            if (numCompletedInputs.get() < numInputs) {
              wakeLoop.await();
            }
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
          LOG.debug("NumCompletedInputs: " + numCompletedInputs);
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
                  LOG.info("Interrupted and hasBeenShutdown, Breaking out of ShuffleScheduler Loop");
                  break;
                } else {
                  throw e;
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Processing pending host: " + inputHost.toDetailedString());
              }
              if (inputHost.getNumPendingInputs() > 0 && !isShutdown.get()) {
                LOG.info("Scheduling fetch for inputHost: " + inputHost.getIdentifier());
                Fetcher fetcher = constructFetcherForHost(inputHost, conf);
                runningFetchers.add(fetcher);
                if (isShutdown.get()) {
                  LOG.info("hasBeenShutdown, Breaking out of ShuffleScheduler Loop");
                }
                ListenableFuture<FetchResult> future = fetcherExecutor
                    .submit(fetcher);
                Futures.addCallback(future, new FetchFutureCallback(fetcher));
                if (++count >= maxFetchersToRun) {
                  break;
                }
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Skipping host: " + inputHost.getIdentifier()
                      + " since it has no inputs to process");
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      }
      LOG.info("Shutting down FetchScheduler, Was Interrupted: " + Thread.currentThread().isInterrupted());
      // TODO NEWTEZ Maybe clean up inputs.
      if (!fetcherExecutor.isShutdown()) {
        fetcherExecutor.shutdownNow();
      }
      return null;
    }
  }
  
  private Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {

    Path lockDisk = null;

    if (sharedFetchEnabled) {
      // pick a single lock disk from the edge name's hashcode + host hashcode
      final int h = Math.abs(Objects.hashCode(this.srcNameTrimmed, inputHost.getHost()));
      lockDisk = new Path(this.localDisks[h % this.localDisks.length], "locks");
    }

    FetcherBuilder fetcherBuilder = new FetcherBuilder(ShuffleManager.this,
      httpConnectionParams, inputManager, inputContext.getApplicationId(),
        shuffleSecret, srcNameTrimmed, conf, localFs, localDirAllocator,
        lockDisk, localDiskFetchEnabled, sharedFetchEnabled);

    if (codec != null) {
      fetcherBuilder.setCompressionParameters(codec);
    }
    fetcherBuilder.setIFileParams(ifileReadAhead, ifileReadAheadLength);

    // Remove obsolete inputs from the list being given to the fetcher. Also
    // remove from the obsolete list.
    List<InputAttemptIdentifier> pendingInputsForHost = inputHost
        .clearAndGetPendingInputs();
    for (Iterator<InputAttemptIdentifier> inputIter = pendingInputsForHost
        .iterator(); inputIter.hasNext();) {
      InputAttemptIdentifier input = inputIter.next();
      // Avoid adding attempts which have already completed.
      if (completedInputSet.contains(input.getInputIdentifier())) {
        inputIter.remove();
        continue;
      }
      // Avoid adding attempts which have been marked as OBSOLETE 
      if (obsoletedInputs.contains(input)) {
        inputIter.remove();
      }
    }
    // TODO NEWTEZ Maybe limit the number of inputs being given to a single
    // fetcher, especially in the case where #hosts < #fetchers
    fetcherBuilder.assignWork(inputHost.getHost(), inputHost.getPort(),
        inputHost.getSrcPhysicalIndex(), pendingInputsForHost);
    LOG.info("Created Fetcher for host: " + inputHost.getHost()
        + ", with inputs: " + pendingInputsForHost);
    return fetcherBuilder.build();
  }
  
  /////////////////// Methods for InputEventHandler
  
  public void addKnownInput(String hostName, int port,
      InputAttemptIdentifier srcAttemptIdentifier, int srcPhysicalIndex) {
    String identifier = InputHost.createIdentifier(hostName, port);
    InputHost host = knownSrcHosts.get(identifier);
    if (host == null) {
      host = new InputHost(hostName, port, inputContext.getApplicationId(), srcPhysicalIndex);
      assert identifier.equals(host.getIdentifier());
      InputHost old = knownSrcHosts.putIfAbsent(identifier, host);
      if (old != null) {
        host = old;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding input: " + srcAttemptIdentifier + ", to host: " + host);
    }
    host.addKnownInput(srcAttemptIdentifier);
    lock.lock();
    try {
      boolean added = pendingHosts.offer(host);
      if (!added) {
        String errorMessage = "Unable to add host: " + host.getIdentifier() + " to pending queue";
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
    InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    LOG.info("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    
    if (!completedInputSet.contains(inputIdentifier)) {
      synchronized (completedInputSet) {
        if (!completedInputSet.contains(inputIdentifier)) {
          registerCompletedInput(new NullFetchedInput(srcAttemptIdentifier));
        }
      }
    }

    // Awake the loop to check for termination.
    lock.lock();
    try {
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
  }

  public void addCompletedInputWithData(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput)
      throws IOException {
    InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    LOG.info("Received Data via Event: " + srcAttemptIdentifier + " to "
        + fetchedInput.getType());
    // Count irrespective of whether this is a copy of an already fetched input
    lock.lock();
    try {
      lastProgressTime = System.currentTimeMillis();
    } finally {
      lock.unlock();
    }

    boolean committed = false;
    if (!completedInputSet.contains(inputIdentifier)) {
      synchronized (completedInputSet) {
        if (!completedInputSet.contains(inputIdentifier)) {
          fetchedInput.commit();
          committed = true;
          registerCompletedInput(fetchedInput);
        }
      }
    }
    if (!committed) {
      fetchedInput.abort(); // If this fails, the fetcher may attempt another
                            // abort.
    } else {
      lock.lock();
      try {
        // Signal the wakeLoop to check for termination.
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }
  }

  public synchronized void obsoleteKnownInput(InputAttemptIdentifier srcAttemptIdentifier) {
    obsoletedInputs.add(srcAttemptIdentifier);
    // TODO NEWTEZ Maybe inform the fetcher about this. For now, this is used during the initial fetch list construction.
  }

  /////////////////// End of Methods for InputEventHandler
  /////////////////// Methods from FetcherCallbackHandler
  
  @Override
  public void fetchSucceeded(String host, InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput, long fetchedBytes, long decompressedLength, long copyDuration)
      throws IOException {
    InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    // Count irrespective of whether this is a copy of an already fetched input
    lock.lock();
    try {
      lastProgressTime = System.currentTimeMillis();
    } finally {
      lock.unlock();
    }
    
    boolean committed = false;
    if (!completedInputSet.contains(inputIdentifier)) {
      synchronized (completedInputSet) {
        if (!completedInputSet.contains(inputIdentifier)) {
          fetchedInput.commit();
          committed = true;
          logIndividualFetchComplete(copyDuration, fetchedBytes, decompressedLength, fetchedInput,
              srcAttemptIdentifier);

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

          registerCompletedInput(fetchedInput);
          lock.lock();
          try {
            totalBytesShuffledTillNow += fetchedBytes;
          } finally {
            lock.unlock();
          }
          logProgress();
        }
      }
    }
    if (!committed) {
      fetchedInput.abort(); // If this fails, the fetcher may attempt another abort.
    } else {
      lock.lock();
      try {
        // Signal the wakeLoop to check for termination.
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }
    // TODO NEWTEZ Maybe inform fetchers, in case they have an alternate attempt of the same task in their queue.
  }

  @Override
  public void fetchFailed(String host,
      InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
    // TODO NEWTEZ. Implement logic to report fetch failures after a threshold.
    // For now, reporting immediately.
    LOG.info("Fetch failed for src: " + srcAttemptIdentifier
        + "InputIdentifier: " + srcAttemptIdentifier + ", connectFailed: "
        + connectFailed);
    failedShufflesCounter.increment(1);
    if (srcAttemptIdentifier == null) {
      String message = "Received fetchFailure for an unknown src (null)";
      LOG.fatal(message);
      inputContext.fatalError(null, message);
    } else {
    InputReadErrorEvent readError = InputReadErrorEvent.create(
        "Fetch failure while fetching from "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(),
            srcAttemptIdentifier.getInputIdentifier().getInputIndex(),
            srcAttemptIdentifier.getAttemptNumber()),
        srcAttemptIdentifier.getInputIdentifier().getInputIndex(),
        srcAttemptIdentifier.getAttemptNumber());
    
    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(readError);
    inputContext.sendEvents(failedEvents);
    }
  }
  /////////////////// End of Methods from FetcherCallbackHandler

  public void shutdown() throws InterruptedException {
    if (!isShutdown.getAndSet(true)) {
      // Shut down any pending fetchers
      LOG.info("Shutting down pending fetchers on source" + srcNameTrimmed + ": "
          + runningFetchers.size());
      lock.lock();
      try {
        wakeLoop.signal(); // signal the fetch-scheduler
        for (Fetcher fetcher : runningFetchers) {
          fetcher.shutdown(); // This could be parallelized.
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
    //All threads are shutdown.  It is safe to shutdown SSL factory
    if (httpConnectionParams.isSSLShuffleEnabled()) {
      HttpConnection.cleanupSSLFactory();
    }
  }

  private void registerCompletedInput(FetchedInput fetchedInput) {
    lock.lock();
    try {
      completedInputSet.add(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());
      completedInputs.add(fetchedInput);
      if (!inputReadyNotificationSent.getAndSet(true)) {
        // TODO Should eventually be controlled by Inputs which are processing the data.
        inputContext.inputIsReady();
      }
      int numComplete = numCompletedInputs.incrementAndGet();
      if (numComplete == numInputs) {
        LOG.info("All inputs fetched for input vertex : " + inputContext.getSourceVertexName());
      }
    } finally {
      lock.unlock();
    }
  }
  
  /////////////////// Methods for walking the available inputs
  
  /**
   * @return true if there is another input ready for consumption.
   */
  public boolean newInputAvailable() {
    FetchedInput head = completedInputs.peek();
    if (head == null || head instanceof NullFetchedInput) {
      return false;
    } else {
      return true;
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
    FetchedInput input = null;
    do {
      // Check for no additional inputs
      lock.lock();
      try {
        input = completedInputs.peek();
        if (input == null && allInputsFetched()) {
          break;
        }
      } finally {
        lock.unlock();
      }
      input = completedInputs.take(); // block
    } while (input instanceof NullFetchedInput);
    return input;
  }
  /////////////////// End of methods for walking the available inputs


  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   *
   */
  private class NullFetchedInput extends FetchedInput {

    public NullFetchedInput(InputAttemptIdentifier inputAttemptIdentifier) {
      super(Type.MEMORY, -1, -1, inputAttemptIdentifier, null);
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

  private void logProgress() {
    double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
    int inputsDone = numInputs - numCompletedInputs.get();
    long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

    double transferRate = mbs / secsSinceStart;
    LOG.info("copy(" + inputsDone + " of " + numInputs +
        ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) "
        + mbpsFormat.format(transferRate) + " MB/s)");
  }

  private void logIndividualFetchComplete(long millis, long fetchedBytes, long decompressedLength,
                                          FetchedInput fetchedInput,
                                          InputAttemptIdentifier srcAttemptIdentifier) {
    double rate = 0;
    if (millis != 0) {
      rate = fetchedBytes / ((double) millis / 1000);
      rate = rate / (1024 * 1024);
    }

    LOG.info(
        "Completed fetch for attempt: " + srcAttemptIdentifier + " to " + fetchedInput.getType() +
            ", CompressedSize=" + fetchedBytes + ", DecompressedSize=" + decompressedLength +
            ",EndTime=" + System.currentTimeMillis() + ", TimeTaken=" + millis + ", Rate=" +
            mbpsFormat.format(rate) + " MB/s");
  }
  
  private class SchedulerFutureCallback implements FutureCallback<Void> {

    @Override
    public void onSuccess(Void result) {
      LOG.info("Scheduler thread completed");
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutdown.get()) {
        LOG.info("Already shutdown. Ignoring error: " + t);
      } else {
        LOG.error("Scheduler failed with error: ", t);
        inputContext.fatalError(t, "Shuffle Scheduler Failed");
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
        LOG.info("Already shutdown. Ignoring event from fetcher");
      } else {
        Iterable<InputAttemptIdentifier> pendingInputs = result.getPendingInputs();
        if (pendingInputs != null && pendingInputs.iterator().hasNext()) {
          InputHost inputHost = knownSrcHosts.get(InputHost.createIdentifier(result.getHost(), result.getPort()));
          assert inputHost != null;
          for (InputAttemptIdentifier input : pendingInputs) {
            inputHost.addKnownInput(input);
          }
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
        LOG.info("Already shutdown. Ignoring error from fetcher: " + t);
      } else {
        LOG.error("Fetcher failed with error: ", t);
        shuffleError = t;
        inputContext.fatalError(t, "Fetch failed");
        doBookKeepingForFetcherComplete();
      }
    }
  }
}

