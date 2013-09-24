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
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.server.ShuffleHandler;
import org.apache.tez.runtime.library.shuffle.common.FetchResult;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput;
import org.apache.tez.runtime.library.shuffle.common.FetchedInputAllocator;
import org.apache.tez.runtime.library.shuffle.common.Fetcher;
import org.apache.tez.runtime.library.shuffle.common.FetcherCallback;
import org.apache.tez.runtime.library.shuffle.common.InputHost;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.common.Fetcher.FetcherBuilder;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class BroadcastShuffleManager implements FetcherCallback {

  private static final Log LOG = LogFactory.getLog(BroadcastShuffleManager.class);
  
  private TezInputContext inputContext;
  private int numInputs;
  private Configuration conf;
  
  private final BroadcastShuffleInputEventHandler inputEventHandler;
  private final FetchedInputAllocator inputManager;
  
  private final ExecutorService fetcherRawExecutor;
  private final ListeningExecutorService fetcherExecutor;

  private final BlockingQueue<FetchedInput> completedInputs;
  private final Set<InputIdentifier> completedInputSet;
  private final Set<InputIdentifier> pendingInputs;
  private final ConcurrentMap<String, InputHost> knownSrcHosts;
  private final Set<InputHost> pendingHosts;
  private final Set<InputAttemptIdentifier> obsoletedInputs;
  
  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);
  
  private final long startTime;
  private long lastProgressTime;
  
  private FutureTask<Void> runShuffleFuture;
  
  // Required to be held when manipulating pendingHosts
  private ReentrantLock lock = new ReentrantLock();
  private Condition wakeLoop = lock.newCondition();
  
  private final int numFetchers;
  private final AtomicInteger numRunningFetchers = new AtomicInteger(0);
  
  // Parameters required by Fetchers
  private final SecretKey shuffleSecret;
  private final int connectionTimeout;
  private final int readTimeout;
  private final CompressionCodec codec;
  private final Decompressor decompressor;
  
  private final FetchFutureCallback fetchFutureCallback = new FetchFutureCallback();
  
  private volatile Throwable shuffleError;
  
  // TODO NEWTEZ Add counters.
  
  public BroadcastShuffleManager(TezInputContext inputContext, Configuration conf, int numInputs) throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;
    this.numInputs = numInputs;
    
    this.inputEventHandler = new BroadcastShuffleInputEventHandler(inputContext, this);
    this.inputManager = new BroadcastInputManager(inputContext, conf);

    pendingInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputIdentifier, Boolean>(numInputs));
    completedInputSet = Collections.newSetFromMap(new ConcurrentHashMap<InputIdentifier, Boolean>(numInputs));
    completedInputs = new LinkedBlockingQueue<FetchedInput>(numInputs);
    knownSrcHosts = new ConcurrentHashMap<String, InputHost>();
    pendingHosts = Collections.newSetFromMap(new ConcurrentHashMap<InputHost, Boolean>());
    obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());
    
    int maxConfiguredFetchers = 
        conf.getInt(
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    
    this.numFetchers = Math.min(maxConfiguredFetchers, numInputs);
    
    this.fetcherRawExecutor = Executors.newFixedThreadPool(numFetchers,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Fetcher #%d")
            .build());
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);
    
    this.startTime = System.currentTimeMillis();
    this.lastProgressTime = startTime;
    
    this.shuffleSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID));
    
    this.connectionTimeout = conf.getInt(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT);
    this.readTimeout = conf.getInt(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    
    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass = ConfigUtils
          .getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
      decompressor = CodecPool.getDecompressor(codec);
    } else {
      codec = null;
      decompressor = null;
    }
  }
  
  public void run() {
    RunBroadcastShuffleCallable callable = new RunBroadcastShuffleCallable();
    runShuffleFuture = new FutureTask<Void>(callable);
    new Thread(runShuffleFuture, "ShuffleRunner");
  }
  
  private class RunBroadcastShuffleCallable implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      while (numCompletedInputs.get() < numInputs) {
        if (numRunningFetchers.get() >= numFetchers || pendingHosts.size() == 0) {
          synchronized(lock) {
            wakeLoop.await();
          }
          if (shuffleError != null) {
            // InputContext has already been informed of a fatal error.
            // Initiate shutdown.
            break;
          }
          
          if (numCompletedInputs.get() < numInputs) {
            synchronized (lock) {
              int numFetchersToRun = Math.min(pendingHosts.size(), numFetchers - numRunningFetchers.get());
              int count = 0;
              for (Iterator<InputHost> inputHostIter = pendingHosts.iterator() ; inputHostIter.hasNext() ; ) {
                InputHost inputHost = inputHostIter.next();
                inputHostIter.remove();
                if (inputHost.getNumPendingInputs() > 0) {
                  Fetcher fetcher = constructFetcherForHost(inputHost);
                  numRunningFetchers.incrementAndGet();
                  ListenableFuture<FetchResult> future = fetcherExecutor
                      .submit(fetcher);
                  Futures.addCallback(future, fetchFutureCallback);
                  if (++count >= numFetchersToRun) {
                    break;
                  }
                }
              }
            }
          }
        }
      }
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
    fetcherBuilder.setCompressionParameters(codec, decompressor);

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
        inputHost.clearAndGetPendingInputs());
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
    host.addKnownInput(srcAttemptIdentifier);
    synchronized(lock) {
      pendingHosts.add(host);
      wakeLoop.signal();
    }
  }

  public void addCompletedInputWithNoData(
      InputAttemptIdentifier srcAttemptIdentifier) {
    InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    LOG.info("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    if (pendingInputs.remove(inputIdentifier)) {
      completedInputSet.add(inputIdentifier);
      completedInputs.add(new NullFetchedInput(srcAttemptIdentifier));
      numCompletedInputs.incrementAndGet();
    }

    // Awake the loop to check for termination.
    synchronized (lock) {
      wakeLoop.signal();
    } 
  }

  public synchronized void obsoleteKnownInput(InputAttemptIdentifier srcAttemptIdentifier) {
    obsoletedInputs.add(srcAttemptIdentifier);
    // TODO NEWTEZ Maybe inform the fetcher about this. For now, this is used during the initial fetch list construction.
  }
  
  
  public void handleEvents(List<Event> events) {
    inputEventHandler.handleEvents(events);
  }

  /////////////////// End of Methods for InputEventHandler
  /////////////////// Methods from FetcherCallbackHandler
  
  @Override
  public void fetchSucceeded(String host,
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput, long fetchedBytes,
      long copyDuration) throws IOException {
    InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Complete fetch for attempt: " + srcAttemptIdentifier + " to " + fetchedInput.getType());
    }
    
    // Count irrespective of whether this is a copy of an already fetched input
    synchronized(lock) {
      lastProgressTime = System.currentTimeMillis();
    }
    
    boolean committed = false;
    if (!completedInputSet.contains(inputIdentifier)) {
      synchronized (completedInputSet) {
        if (!completedInputSet.contains(inputIdentifier)) {
          fetchedInput.commit();
          committed = true;
          pendingInputs.remove(inputIdentifier);
          completedInputSet.add(inputIdentifier);
          completedInputs.add(fetchedInput);
          numCompletedInputs.incrementAndGet();
        }
      }
    }
    if (!committed) {
      fetchedInput.abort(); // If this fails, the fetcher may attempt another abort.
    } else {
      synchronized(lock) {
        // Signal the wakeLoop to check for termination.
        wakeLoop.signal();
      }
    }
    // TODO NEWTEZ Maybe inform fetchers, in case they have an alternate attempt of the same task in their queue.
  }

  @Override
  public void fetchFailed(String host,
      InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
    // TODO NEWTEZ. Implement logic to report fetch failures after a threshold.
    // For now, reporting immediately.
    InputReadErrorEvent readError = new InputReadErrorEvent(
        "Fetch failure while fetching from "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
                inputContext.getSourceVertexName(),
                srcAttemptIdentifier.getInputIdentifier().getSrcTaskIndex(),
                srcAttemptIdentifier.getAttemptNumber()),
        srcAttemptIdentifier.getInputIdentifier().getSrcTaskIndex(),
        srcAttemptIdentifier.getAttemptNumber());
    
    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(readError);
    inputContext.sendEvents(failedEvents);
  }
  /////////////////// End of Methods from FetcherCallbackHandler

  public void shutdown() throws InterruptedException {
    if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
      this.fetcherExecutor.shutdown();
      this.fetcherExecutor.awaitTermination(2000l, TimeUnit.MILLISECONDS);
      if (!this.fetcherExecutor.isShutdown()) {
        this.fetcherExecutor.shutdownNow();
      }
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
    return numCompletedInputs.get() == numInputs;
  }

  /**
   * @return the next available input, or null if there are no available inputs.
   *         This method will block if there are currently no available inputs,
   *         but more may become available.
   */
  public FetchedInput getNextInput() throws InterruptedException {
    FetchedInput input = null;
    do {
      input = completedInputs.peek();
      if (input == null) {
        if (allInputsFetched()) {
          break;
        } else {
          input = completedInputs.take(); // block
        }
      } else {
        input = completedInputs.poll();
      }
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
      super(Type.MEMORY, -1, inputAttemptIdentifier, null);
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
  
  
  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private void doBookKeepingForFetcherComplete() {
      numRunningFetchers.decrementAndGet();
      synchronized(lock) {
        wakeLoop.signal();
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
      LOG.error("Fetcher failed with error: " + t);
      shuffleError = t;
      inputContext.fatalError(t, "Fetched failed");
      doBookKeepingForFetcherComplete();
    }
  }
}
