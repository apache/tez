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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.exceptions.InputAlreadyClosedException;
import org.apache.tez.runtime.library.common.shuffle.HttpConnection.HttpConnectionParams;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

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
  
  private static final Logger LOG = LoggerFactory.getLogger(Shuffle.class);
  private static final int PROGRESS_FREQUENCY = 2000;
  
  private final Configuration conf;
  private final InputContext inputContext;
  
  private final ShuffleClientMetrics metrics;

  private final ShuffleInputEventHandlerOrderedGrouped eventHandler;
  private final ShuffleScheduler scheduler;
  private final MergeManager merger;

  private final SecretKey jobTokenSecret;
  private final JobTokenSecretManager jobTokenSecretMgr;
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final int numFetchers;
  private final boolean localDiskFetchEnabled;
  private final String localHostname;
  private final int shufflePort;
  
  private AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();
  private String throwingThreadName = null;

  private final RunShuffleCallable runShuffleCallable;
  private volatile ListenableFuture<TezRawKeyValueIterator> runShuffleFuture;
  private final ListeningExecutorService executor;
  
  private final String srcNameTrimmed;
  
  private final List<FetcherOrderedGrouped> fetchers;
  private final HttpConnectionParams httpConnectionParams;
  
  private AtomicBoolean isShutDown = new AtomicBoolean(false);
  private AtomicBoolean fetchersClosed = new AtomicBoolean(false);
  private AtomicBoolean schedulerClosed = new AtomicBoolean(false);
  private AtomicBoolean mergerClosed = new AtomicBoolean(false);

  private final long startTime;
  private final TezCounter mergePhaseTime;
  private final TezCounter shufflePhaseTime;

  public Shuffle(InputContext inputContext, Configuration conf, int numInputs,
      long initialMemoryAvailable) throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;
    this.httpConnectionParams =
        ShuffleUtils.constructHttpShuffleConnectionParams(conf);
    this.metrics = new ShuffleClientMetrics(inputContext.getDAGName(),
        inputContext.getTaskVertexName(), inputContext.getTaskIndex(),
        this.conf, UserGroupInformation.getCurrentUser().getShortUserName());
    
    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
    
    this.jobTokenSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID));
    this.jobTokenSecretMgr = new JobTokenSecretManager(jobTokenSecret);
    
    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
    } else {
      codec = null;
    }
    this.ifileReadAhead = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }
    
    Combiner combiner = TezRuntimeUtils.instantiateCombiner(conf, inputContext);
    
    FileSystem localFS = FileSystem.getLocal(this.conf);
    LocalDirAllocator localDirAllocator = 
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    this.localHostname = inputContext.getExecutionContext().getHostName();
    final ByteBuffer shuffleMetadata =
        inputContext.getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    this.shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);

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
    TezCounter bytesShuffedToDiskDirect = inputContext.getCounters().findCounter(
        TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
    TezCounter bytesShuffedToMem = inputContext.getCounters().findCounter(
        TaskCounter.SHUFFLE_BYTES_TO_MEM);
    
    LOG.info(srcNameTrimmed + ": " + "Shuffle assigned with " + numInputs + " inputs" + ", codec: "
        + (codec == null ? "None" : codec.getClass().getName()) + 
        "ifileReadAhead: " + ifileReadAhead);

    boolean sslShuffle = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
      TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT);
    startTime = System.currentTimeMillis();
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
          bytesShuffedToDiskDirect,
          bytesShuffedToMem,
          startTime,
          srcNameTrimmed);
    this.mergePhaseTime = inputContext.getCounters().findCounter(TaskCounter.MERGE_PHASE_TIME);
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);

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

    eventHandler= new ShuffleInputEventHandlerOrderedGrouped(
        inputContext,
        scheduler,
        sslShuffle);
    
    ExecutorService rawExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ShuffleAndMergeRunner {" + srcNameTrimmed + "}").build());

    int configuredNumFetchers = 
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    numFetchers = Math.min(configuredNumFetchers, numInputs);
    LOG.info(srcNameTrimmed + ": " + "Num fetchers being started: " + numFetchers);
    fetchers = Lists.newArrayListWithCapacity(numFetchers);
    localDiskFetchEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);

    executor = MoreExecutors.listeningDecorator(rawExecutor);
    runShuffleCallable = new RunShuffleCallable();
  }

  public void handleEvents(List<Event> events) throws IOException {
    if (!isShutDown.get()) {
      eventHandler.handleEvents(events);
    } else {
      LOG.info(srcNameTrimmed + ": " + "Ignoring events since already shutdown. EventCount: " + events.size());
    }

  }
  
  /**
   * Indicates whether the Shuffle and Merge processing is complete.
   * @return false if not complete, true if complete or if an error occurred.
   * @throws InterruptedException 
   * @throws IOException 
   * @throws InputAlreadyClosedException 
   */
  public boolean isInputReady() throws IOException, InterruptedException, TezException {
    if (isShutDown.get()) {
      throw new InputAlreadyClosedException();
    }
    if (throwable.get() != null) {
      handleThrowable(throwable.get());
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
  public TezRawKeyValueIterator waitForInput() throws IOException, InterruptedException,
      TezException {
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
    if (throwable.get() != null) {
      handleThrowable(throwable.get());
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

  // Not handling any shutdown logic here. That's handled by the callback from this invocation.
  private class RunShuffleCallable extends CallableWithNdc<TezRawKeyValueIterator> {
    @Override
    protected TezRawKeyValueIterator callInternal() throws IOException, InterruptedException {

      synchronized (this) {
        synchronized (fetchers) {
          for (int i = 0; i < numFetchers; ++i) {
            if (!isShutDown.get()) {
              FetcherOrderedGrouped
                fetcher = new FetcherOrderedGrouped(httpConnectionParams, scheduler, merger,
                metrics, Shuffle.this, jobTokenSecretMgr, ifileReadAhead, ifileReadAheadLength,
                codec, inputContext, conf, localDiskFetchEnabled, localHostname, shufflePort);
              fetchers.add(fetcher);
              fetcher.start();
            }
          }
        }
      }

      
      while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
        synchronized (Shuffle.this) {
          if (throwable.get() != null) {
            throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                   throwable.get());
          }
        }
      }
      shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);

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
      mergePhaseTime.setValue(System.currentTimeMillis() - startTime);

      // Sanity check
      synchronized (Shuffle.this) {
        if (throwable.get() != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                 throwable.get());
        }
      }

      inputContext.inputIsReady();
      LOG.info("merge complete for input vertex : " + srcNameTrimmed);
      return kvIter;
    }
  }
  
  private synchronized void cleanupFetchers(boolean ignoreErrors) throws InterruptedException {
    // Stop the fetcher threads
    InterruptedException ie = null;
    if (!fetchersClosed.getAndSet(true)) {
      synchronized (fetchers) {
        for (FetcherOrderedGrouped fetcher : fetchers) {
          try {
            fetcher.shutDown();
            LOG.info(srcNameTrimmed + ": " + "Shutdown.." + fetcher.getName() + ", status:" + fetcher.isAlive() + ", "
                + "isInterrupted:" + fetcher.isInterrupted());
          } catch (InterruptedException e) {
            if (ignoreErrors) {
              LOG.info(srcNameTrimmed + ": " + "Interrupted while shutting down fetchers. Ignoring.");
            } else {
              if (ie != null) {
                ie = e;
              } else {
                LOG.warn(srcNameTrimmed + ": " +
                    "Ignoring exception while shutting down fetcher since a previous one was seen and will be thrown "
                        + e);
              }
            }
          }
        }
        fetchers.clear();
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
          LOG.info(srcNameTrimmed + ": " + "Interrupted while attempting to close the scheduler during cleanup. Ignoring");
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
          LOG.info(srcNameTrimmed + ": " + "Exception while trying to shutdown merger, Ignoring", e);
        } else {
          throw e;
        }
      }
    }
  }

  private void cleanupIgnoreErrors() {
    try {
      if (eventHandler != null) {
        eventHandler.logProgress(true);
      }
      cleanupFetchers(true);
      cleanupShuffleScheduler(true);
      cleanupMerger(true);
    } catch (Throwable t) {
      LOG.info(srcNameTrimmed + ": " + "Error in cleaning up.., ", t);
    }
  }

  @Private
  public synchronized void reportException(Throwable t) {
    // RunShuffleCallable onFailure deals with ignoring errors on shutdown.
    if (throwable.get() == null) {
      LOG.info(srcNameTrimmed + ": " + "Setting throwable in reportException with message [" + t.getMessage() +
          "] from thread [" + Thread.currentThread().getName());
      throwable.set(t);
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
      LOG.info(srcNameTrimmed + ": " + "Shuffle Runner thread complete");
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutDown.get()) {
        LOG.info(srcNameTrimmed + ": " + "Already shutdown. Ignoring error");
      } else {
        LOG.error(srcNameTrimmed + ": " + "ShuffleRunner failed with error", t);
        inputContext.fatalError(t, "Shuffle Runner Failed");
        cleanupIgnoreErrors();
      }
    }
  }
}
