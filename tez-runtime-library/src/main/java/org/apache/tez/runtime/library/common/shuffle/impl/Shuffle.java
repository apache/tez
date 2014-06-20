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
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.exceptions.InputAlreadyClosedException;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection.HttpConnectionParams;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Usage: Create instance, setInitialMemoryAllocated(long), run()
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Shuffle implements ExceptionReporter {
  
  private static final Log LOG = LogFactory.getLog(Shuffle.class);
  private static final int PROGRESS_FREQUENCY = 2000;
  
  private final Configuration conf;
  private final TezInputContext inputContext;
  
  private final ShuffleClientMetrics metrics;

  private final ShuffleInputEventHandler eventHandler;
  private final ShuffleScheduler scheduler;
  private final MergeManager merger;

  private final SecretKey jobTokenSecret;
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final int numFetchers;
  
  private Throwable throwable = null;
  private String throwingThreadName = null;

  private final RunShuffleCallable runShuffleCallable;
  private volatile ListenableFuture<TezRawKeyValueIterator> runShuffleFuture;
  private final ListeningExecutorService executor;
  
  private final String srcNameTrimmed;
  
  private final List<Fetcher> fetchers;
  private final HttpConnectionParams httpConnectionParams;
  
  private AtomicBoolean isShutDown = new AtomicBoolean(false);
  private AtomicBoolean fetchersClosed = new AtomicBoolean(false);
  private AtomicBoolean schedulerClosed = new AtomicBoolean(false);
  private AtomicBoolean mergerClosed = new AtomicBoolean(false);

  public Shuffle(TezInputContext inputContext, Configuration conf, int numInputs,
      long initialMemoryAvailable) throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;
    this.httpConnectionParams =
        ShuffleUtils.constructHttpShuffleConnectionParams(conf);
    this.metrics = new ShuffleClientMetrics(inputContext.getDAGName(),
        inputContext.getTaskVertexName(), inputContext.getTaskIndex(),
        this.conf, UserGroupInformation.getCurrentUser().getShortUserName());
    
    this.srcNameTrimmed = TezUtils.cleanVertexName(inputContext.getSourceVertexName());
    
    this.jobTokenSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(TezConfiguration.TEZ_SHUFFLE_HANDLER_SERVICE_ID));
    
    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
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
    
    Combiner combiner = TezRuntimeUtils.instantiateCombiner(conf, inputContext);
    
    FileSystem localFS = FileSystem.getLocal(this.conf);
    LocalDirAllocator localDirAllocator = 
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    // TODO TEZ Get rid of Map / Reduce references.
    TezCounter shuffledInputsCounter = 
        inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    TezCounter reduceShuffleBytes =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    TezCounter reduceDataSizeDecompressed = inputContext.getCounters().findCounter(
        TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    TezCounter failedShuffleCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    TezCounter spilledRecordsCounter = 
        inputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter reduceCombineInputCounter =
        inputContext.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    TezCounter mergedMapOutputsCounter =
        inputContext.getCounters().findCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    TezCounter bytesShuffedToDisk = inputContext.getCounters().findCounter(
        TaskCounter.SHUFFLE_BYTES_TO_DISK);
    TezCounter bytesShuffedToMem = inputContext.getCounters().findCounter(
        TaskCounter.SHUFFLE_BYTES_TO_MEM);
    
    LOG.info("Shuffle assigned with " + numInputs + " inputs" + ", codec: "
        + (codec == null ? "None" : codec.getClass().getName()) + 
        "ifileReadAhead: " + ifileReadAhead);

    boolean sslShuffle = conf.getBoolean(TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
      TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT);
    scheduler = new ShuffleScheduler(
          this.inputContext,
          this.conf,
          numInputs,
          this,
          shuffledInputsCounter,
          reduceShuffleBytes,
          reduceDataSizeDecompressed,
          failedShuffleCounter,
          bytesShuffedToDisk,
          bytesShuffedToMem);
    eventHandler= new ShuffleInputEventHandler(
      inputContext,
      scheduler,
      sslShuffle);
    merger = new MergeManager(
          this.conf,
          localFS,
          localDirAllocator,
          inputContext,
          combiner,
          spilledRecordsCounter,
          reduceCombineInputCounter,
          mergedMapOutputsCounter,
          this,
          initialMemoryAvailable,
          codec,
          ifileReadAhead,
          ifileReadAheadLength);
    
    ExecutorService rawExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ShuffleAndMergeRunner [" + srcNameTrimmed + "]").build());

    int configuredNumFetchers = 
        conf.getInt(
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES, 
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    numFetchers = Math.min(configuredNumFetchers, numInputs);
    LOG.info("Num fetchers being started: " + numFetchers);
    fetchers = Lists.newArrayListWithCapacity(numFetchers);
    
    executor = MoreExecutors.listeningDecorator(rawExecutor);
    runShuffleCallable = new RunShuffleCallable();
  }

  public void handleEvents(List<Event> events) {
    if (!isShutDown.get()) {
      eventHandler.handleEvents(events);
    } else {
      LOG.info("Ignoring events since already shutdown. EventCount: " + events.size());
    }

  }
  
  /**
   * Indicates whether the Shuffle and Merge processing is complete.
   * @return false if not complete, true if complete or if an error occurred.
   * @throws InterruptedException 
   * @throws IOException 
   * @throws InputAlreadyClosedException 
   */
  // ZZZ Deal with these methods.
  public boolean isInputReady() throws IOException, InterruptedException {
    if (isShutDown.get()) {
      throw new InputAlreadyClosedException();
    }
    if (throwable != null) {
      handleThrowable(throwable);
    }
    if (runShuffleFuture == null) {
      return false;
    }
    // Don't need to check merge status, since runShuffleFuture will only
    // complete once merge is complete.
    return runShuffleFuture.isDone();
  }

  private void handleThrowable(Throwable t) throws IOException, InterruptedException {
    if (t instanceof IOException) {
      throw (IOException) t;
    } else if (t instanceof InterruptedException) {
      throw (InterruptedException) t;
    } else {
      throw new UndeclaredThrowableException(t);
    }
  }

  /**
   * Waits for the Shuffle and Merge to complete, and returns an iterator over the input.
   * @return an iterator over the fetched input.
   * @throws IOException
   * @throws InterruptedException
   */
  // ZZZ Deal with these methods.
  public TezRawKeyValueIterator waitForInput() throws IOException, InterruptedException {
    Preconditions.checkState(runShuffleFuture != null,
        "waitForInput can only be called after run");
    TezRawKeyValueIterator kvIter = null;
    try {
      kvIter = runShuffleFuture.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      handleThrowable(cause);
    }
    if (isShutDown.get()) {
      throw new InputAlreadyClosedException();
    }
    if (throwable != null) {
      handleThrowable(throwable);
    }
    return kvIter;
  }

  public void run() throws IOException {
    merger.configureAndStart();
    runShuffleFuture = executor.submit(runShuffleCallable);
    Futures.addCallback(runShuffleFuture, new ShuffleRunnerFutureCallback());
    executor.shutdown();
  }

  public void shutdown() {
    if (!isShutDown.getAndSet(true)) {
      // Interrupt so that the scheduler / merger sees this interrupt.
      LOG.info("Shutting down Shuffle for source: " + srcNameTrimmed);
      runShuffleFuture.cancel(true);
      cleanupIgnoreErrors();
    }
  }

  private class RunShuffleCallable implements Callable<TezRawKeyValueIterator> {
    @Override
    public TezRawKeyValueIterator call() throws IOException, InterruptedException {

      synchronized (this) {
        for (int i = 0; i < numFetchers; ++i) {
          Fetcher fetcher = new Fetcher(httpConnectionParams, scheduler, merger,
            metrics, Shuffle.this, jobTokenSecret, ifileReadAhead, ifileReadAheadLength,
            codec, inputContext);
          fetchers.add(fetcher);
          fetcher.start();
        }
      }

      
      while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
        synchronized (this) {
          if (throwable != null) {
            throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                   throwable);
          }
        }
      }
      
      // Stop the map-output fetcher threads
      cleanupFetchers(false);
      
      // stop the scheduler
      cleanupShuffleScheduler(false);

      // Finish the on-going merges...
      TezRawKeyValueIterator kvIter = null;
      try {
        kvIter = merger.close();
      } catch (Throwable e) {
        throw new ShuffleError("Error while doing final merge " , e);
      }
      
      // Sanity check
      synchronized (Shuffle.this) {
        if (throwable != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                 throwable);
        }
      }

      inputContext.inputIsReady();
      LOG.info("merge complete for input vertex : " + inputContext.getSourceVertexName());
      return kvIter;
    }
  }
  
  private synchronized void cleanupFetchers(boolean ignoreErrors) throws InterruptedException {
    // Stop the fetcher threads
    InterruptedException ie = null;
    if (!fetchersClosed.getAndSet(true)) {
      for (Fetcher fetcher : fetchers) {
        try {
          fetcher.shutDown();
        } catch (InterruptedException e) {
          if (ignoreErrors) {
            LOG.info("Interrupted while shutting down fetchers. Ignoring.");
          } else {
            if (ie != null) {
              ie = e;
            } else {
              LOG.warn("Ignoring exception while shutting down fetcher since a previous one was seen and will be thrown "
                  + e);
            }
          }
        }
      }
      fetchers.clear();
      //All threads are shutdown.  It is safe to shutdown SSL factory
      if (httpConnectionParams.isSSLShuffleEnabled()) {
        HttpConnection.cleanupSSLFactory();
      }
      // throw only the first exception while attempting to shutdown.
      if (ie != null) {
        throw ie;
      }
    }
  }

  private void cleanupShuffleScheduler(boolean ignoreErrors) throws InterruptedException {

    if (!schedulerClosed.getAndSet(true)) {
      try {
        scheduler.close();
      } catch (InterruptedException e) {
        if (ignoreErrors) {
          LOG.info("Interrupted while attempting to close the scheduler during cleanup. Ignoring");
        } else {
          throw e;
        }
      }
    }
  }

  private void cleanupMerger(boolean ignoreErrors) throws Throwable {
    if (!mergerClosed.getAndSet(true)) {
      try {
        merger.close();
      } catch (Throwable e) {
        if (ignoreErrors) {
          LOG.info("Exception while trying to shutdown merger, Ignoring", e);
        } else {
          throw e;
        }
      }
    }
  }

  private void cleanupIgnoreErrors() {
    try {
      cleanupFetchers(true);
      cleanupShuffleScheduler(true);
      cleanupMerger(true);
    } catch (Throwable t) {
      // Ignore
    }
  }

  @Private
  public synchronized void reportException(Throwable t) {
    if (throwable == null) {
      throwable = t;
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the 
      // exception immediately.
      synchronized (scheduler) {
        scheduler.notifyAll();
      }
    }
  }
  
  public static class ShuffleError extends IOException {
    private static final long serialVersionUID = 5753909320586607881L;

    ShuffleError(String msg, Throwable t) {
      super(msg, t);
    }
  }

  @Private
  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    return MergeManager.getInitialMemoryRequirement(conf, maxAvailableTaskMemory);
  }
  
  private class ShuffleRunnerFutureCallback implements FutureCallback<TezRawKeyValueIterator> {
    @Override
    public void onSuccess(TezRawKeyValueIterator result) {
      LOG.info("Shuffle Runner thread complete");
    }

    @Override
    public void onFailure(Throwable t) {
      // ZZZ Handle failures during shutdown.
      if (isShutDown.get()) {
        LOG.info("Already shutdown. Ignoring error: ",  t);
      } else {
        LOG.error("ShuffleRunner failed with error", t);
        inputContext.fatalError(t, "Shuffle Runner Failed");
        cleanupIgnoreErrors();
      }
    }
  }
}
