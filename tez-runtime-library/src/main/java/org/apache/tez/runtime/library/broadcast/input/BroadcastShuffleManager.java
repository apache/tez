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

package org.apache.tez.runtime.library.broadcast.input;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.shuffle.common.FetchResult;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput;
import org.apache.tez.runtime.library.shuffle.common.FetchedInputAllocator;
import org.apache.tez.runtime.library.shuffle.common.Fetcher;
import org.apache.tez.runtime.library.shuffle.common.Fetcher.FetcherBuilder;
import org.apache.tez.runtime.library.shuffle.common.FetcherCallback;
import org.apache.tez.runtime.library.shuffle.common.InputHost;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class BroadcastShuffleManager implements FetcherCallback, MemoryUpdateCallback {

  private static final Log LOG = LogFactory.getLog(BroadcastShuffleManager.class);
  
  private final TezInputContext inputContext;
  private final Configuration conf;
  private final int numInputs;
  
  private BroadcastShuffleInputEventHandler inputEventHandler;
  private FetchedInputAllocator inputManager;
  
  private ExecutorService fetcherRawExecutor;
  private ListeningExecutorService fetcherExecutor;

  private ExecutorService schedulerRawExecutor;
  private ListeningExecutorService schedulerExecutor;
  private RunBroadcastShuffleCallable schedulerCallable = new RunBroadcastShuffleCallable();
  
  private BlockingQueue<FetchedInput> completedInputs;
  private AtomicBoolean inputReadyNotificationSent = new AtomicBoolean(false);
  private Set<InputIdentifier> completedInputSet;
  private ConcurrentMap<String, InputHost> knownSrcHosts;
  private BlockingQueue<InputHost> pendingHosts;
  private Set<InputAttemptIdentifier> obsoletedInputs;
  
  private AtomicInteger numCompletedInputs = new AtomicInteger(0);
  
  private long startTime;
  private long lastProgressTime;

  // Required to be held when manipulating pendingHosts
  private ReentrantLock lock = new ReentrantLock();
  private Condition wakeLoop = lock.newCondition();
  
  private int numFetchers;
  private AtomicInteger numRunningFetchers = new AtomicInteger(0);
  
  // Parameters required by Fetchers
  private SecretKey shuffleSecret;
  private int connectionTimeout;
  private int readTimeout;
  private CompressionCodec codec;
  
  private boolean ifileReadAhead;
  private int ifileReadAheadLength;
  private int ifileBufferSize;
  
  private final FetchFutureCallback fetchFutureCallback = new FetchFutureCallback();
  
  private volatile Throwable shuffleError;
  
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  
  private volatile long initialMemoryAvailable = -1l;

  // TODO NEWTEZ Add counters.
  
  public BroadcastShuffleManager(TezInputContext inputContext, Configuration conf, int numInputs) throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;
    this.numInputs = numInputs;
    long initalMemReq = getInitialMemoryReq();
    this.inputContext.requestInitialMemory(initalMemReq, this);
  }

  private void configureAndStart() throws IOException {
    Preconditions.checkState(initialMemoryAvailable != -1,
        "Initial memory available must be configured before starting");
    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass = ConfigUtils
          .getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
    } else {
      codec = null;
    }

    this.ifileReadAhead = conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
          TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
          TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }
    this.ifileBufferSize = conf.getInt("io.file.buffer.size",
        TezJobConfig.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);
    
    this.inputManager = new BroadcastInputManager(inputContext.getUniqueIdentifier(), conf,
        inputContext.getTotalMemoryAvailableToTask());
    ((BroadcastInputManager)this.inputManager).setInitialMemoryAvailable(initialMemoryAvailable);
    ((BroadcastInputManager)this.inputManager).configureAndStart();
    this.inputEventHandler = new BroadcastShuffleInputEventHandler(
        inputContext, this, this.inputManager, codec, ifileReadAhead,
        ifileReadAheadLength);

    completedInputSet = Collections.newSetFromMap(new ConcurrentHashMap<InputIdentifier, Boolean>(numInputs));
    completedInputs = new LinkedBlockingQueue<FetchedInput>(numInputs);
    knownSrcHosts = new ConcurrentHashMap<String, InputHost>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();
    obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());
    
    int maxConfiguredFetchers = 
        conf.getInt(
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    
    this.numFetchers = Math.min(maxConfiguredFetchers, numInputs);
    
    this.fetcherRawExecutor = Executors.newFixedThreadPool(
        numFetchers,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(
                "Fetcher [" + inputContext.getUniqueIdentifier() + "] #%d")
            .build());
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);
    
    this.schedulerRawExecutor = Executors.newFixedThreadPool(
        1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(
                "ShuffleRunner [" + inputContext.getUniqueIdentifier() + "]")
            .build());
    this.schedulerExecutor = MoreExecutors.listeningDecorator(schedulerRawExecutor);
    
    this.startTime = System.currentTimeMillis();
    this.lastProgressTime = startTime;
    
    this.shuffleSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(TezConfiguration.TEZ_SHUFFLE_HANDLER_SERVICE_ID));
    
    this.connectionTimeout = conf.getInt(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT);
    this.readTimeout = conf.getInt(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    
    
    LOG.info("BroadcastShuffleManager -> numInputs: " + numInputs
        + " compressionCodec: " + (codec == null ? "NoCompressionCodec" : codec.getClass()
        .getName()) + ", numFetchers: " + numFetchers);
  }
  
  private long getInitialMemoryReq() {
    return BroadcastInputManager.getInitialMemoryReq(conf,
        inputContext.getTotalMemoryAvailableToTask());
  }
  
  public void setInitialMemoryAvailable(long available) {
    this.initialMemoryAvailable = available;
  }

  public void run() throws IOException {
    configureAndStart();
    ListenableFuture<Void> runShuffleFuture = schedulerExecutor.submit(schedulerCallable);
    Futures.addCallback(runShuffleFuture, new SchedulerFutureCallback());
    // Shutdown this executor once this task, and the callback complete.
    schedulerExecutor.shutdown();
  }
  
  private class RunBroadcastShuffleCallable implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      while (!isShutdown.get() && numCompletedInputs.get() < numInputs) {
        lock.lock();
        try {
          if (numRunningFetchers.get() >= numFetchers || pendingHosts.size() == 0) {
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
        if (numCompletedInputs.get() < numInputs) {
          lock.lock();
          try {
            int maxFetchersToRun = numFetchers - numRunningFetchers.get();
            int count = 0;
            while (pendingHosts.peek() != null) {
              InputHost inputHost = null;
              try {
                inputHost = pendingHosts.take();
              } catch (InterruptedException e) {
                if (isShutdown.get()) {
                  LOG.info("Interrupted and hasBeenShutdown, Breaking out of BroadcastScheduler Loop");
                  break;
                } else {
                  throw e;
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Processing pending host: " + inputHost.toDetailedString());
              }
              if (inputHost.getNumPendingInputs() > 0) {
                LOG.info("Scheduling fetch for inputHost: " + inputHost.getHost());
                Fetcher fetcher = constructFetcherForHost(inputHost);
                numRunningFetchers.incrementAndGet();
                if (isShutdown.get()) {
                  LOG.info("hasBeenShutdown, Breaking out of BroadcastScheduler Loop");
                }
                ListenableFuture<FetchResult> future = fetcherExecutor
                    .submit(fetcher);
                Futures.addCallback(future, fetchFutureCallback);
                if (++count >= maxFetchersToRun) {
                  break;
                }
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Skipping host: " + inputHost.getHost()
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
  
  private Fetcher constructFetcherForHost(InputHost inputHost) {
    FetcherBuilder fetcherBuilder = new FetcherBuilder(
        BroadcastShuffleManager.this, inputManager,
        inputContext.getApplicationId(), shuffleSecret, conf);
    fetcherBuilder.setConnectionParameters(connectionTimeout, readTimeout);
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
      }
      // Avoid adding attempts which have been marked as OBSOLETE 
      if (obsoletedInputs.contains(input)) {
        inputIter.remove();
        obsoletedInputs.remove(input);
      }
    }
    // TODO NEWTEZ Maybe limit the number of inputs being given to a single
    // fetcher, especially in the case where #hosts < #fetchers
    fetcherBuilder.assignWork(inputHost.getHost(), inputHost.getPort(), 0,
        pendingInputsForHost);
    LOG.info("Created Fetcher for host: " + inputHost.getHost()
        + ", with inputs: " + pendingInputsForHost);
    return fetcherBuilder.build();
  }
  
  /////////////////// Methods for InputEventHandler
  
  public void addKnownInput(String hostName, int port,
      InputAttemptIdentifier srcAttemptIdentifier, int partition) {
    InputHost host = knownSrcHosts.get(hostName);
    if (host == null) {
      host = new InputHost(hostName, port, inputContext.getApplicationId());
      InputHost old = knownSrcHosts.putIfAbsent(hostName, host);
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
        String errorMessage = "Unable to add host: " + host.getHost() + " to pending queue";
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
  
  
  public void handleEvents(List<Event> events) throws IOException {
    inputEventHandler.handleEvents(events);
  }

  /////////////////// End of Methods for InputEventHandler
  /////////////////// Methods from FetcherCallbackHandler
  
  @Override
  public void fetchSucceeded(String host,
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput, long fetchedBytes,
      long copyDuration) throws IOException {
    InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();    

    LOG.info("Completed fetch for attempt: " + srcAttemptIdentifier + " to " + fetchedInput.getType());

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
    if (srcAttemptIdentifier == null) {
      String message = "Received fetchFailure for an unknown src (null)";
      LOG.fatal(message);
      inputContext.fatalError(null, message);
    } else {
    InputReadErrorEvent readError = new InputReadErrorEvent(
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
    isShutdown.set(true);
    if (this.schedulerExecutor != null && !this.schedulerExecutor.isShutdown()) {
      this.schedulerExecutor.shutdownNow(); // Interrupt all running fetchers
    }
    if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
      this.fetcherExecutor.shutdownNow(); // Interrupt all running fetchers
    }
  }
  
  private void registerCompletedInput(FetchedInput fetchedInput) {
    lock.lock();
    try {
      completedInputSet.add(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());
      completedInputs.add(fetchedInput);
      if (!inputReadyNotificationSent.getAndSet(true)) {
        inputContext.inputIsReady();
      }
      numCompletedInputs.incrementAndGet();
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

  @SuppressWarnings("rawtypes")
  public BroadcastKVReader createReader() throws IOException {
    return new BroadcastKVReader(this, conf, codec, ifileReadAhead, ifileReadAheadLength, ifileBufferSize);
  }
  
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
        inputContext.fatalError(t, "Broadcast Scheduler Failed");
      }
    }
    
  }
  
  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private void doBookKeepingForFetcherComplete() {
      numRunningFetchers.decrementAndGet();
      lock.lock();
      try {
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }
    
    @Override
    public void onSuccess(FetchResult result) {
      Iterable<InputAttemptIdentifier> pendingInputs = result.getPendingInputs();
      if (pendingInputs != null && pendingInputs.iterator().hasNext()) {
        InputHost inputHost = knownSrcHosts.get(result.getHost());
        assert inputHost != null;
        for (InputAttemptIdentifier input : pendingInputs) {
          inputHost.addKnownInput(input);
        }
        pendingHosts.add(inputHost);
      }
      doBookKeepingForFetcherComplete();
    }

    @Override
    public void onFailure(Throwable t) {
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

  @Override
  public void memoryAssigned(long assignedSize) {
    this.initialMemoryAvailable = assignedSize;
  }
}
