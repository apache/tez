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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


import com.google.common.annotations.VisibleForTesting;
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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.exceptions.InputAlreadyClosedException;

import com.google.common.base.Preconditions;
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
  
  private final ShuffleInputEventHandlerOrderedGrouped eventHandler;
  @VisibleForTesting
  final ShuffleScheduler scheduler;
  @VisibleForTesting
  final MergeManager merger;

  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  private AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();
  private String throwingThreadName = null;

  private final RunShuffleCallable runShuffleCallable;
  private volatile ListenableFuture<TezRawKeyValueIterator> runShuffleFuture;
  private final ListeningExecutorService executor;
  
  private final String srcNameTrimmed;
  
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

    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
    

    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
      // Work around needed for HADOOP-12191. Avoids the native initialization synchronization race
      codec.getDecompressorType();
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

    // TODO TEZ Get rid of Map / Reduce references.
    TezCounter spilledRecordsCounter =
        inputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter reduceCombineInputCounter =
        inputContext.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    TezCounter mergedMapOutputsCounter =
        inputContext.getCounters().findCounter(TaskCounter.MERGED_MAP_OUTPUTS);

    LOG.info(srcNameTrimmed + ": " + "Shuffle assigned with " + numInputs + " inputs" + ", codec: "
        + (codec == null ? "None" : codec.getClass().getName())
        + ", ifileReadAhead: " + ifileReadAhead);

    boolean sslShuffle = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
      TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT);
    startTime = System.currentTimeMillis();
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

    scheduler = new ShuffleScheduler(
          this.inputContext,
          this.conf,
          numInputs,
          this,
          merger,
          merger,
          startTime,
          codec,
          ifileReadAhead,
          ifileReadAheadLength,
          srcNameTrimmed);

    this.mergePhaseTime = inputContext.getCounters().findCounter(TaskCounter.MERGE_PHASE_TIME);
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);



    eventHandler= new ShuffleInputEventHandlerOrderedGrouped(
        inputContext,
        scheduler,
        sslShuffle);
    
    ExecutorService rawExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ShuffleAndMergeRunner {" + srcNameTrimmed + "}").build());


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
      // Processor interrupted while waiting for errors, will see an InterruptedException.
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

      if (!isShutDown.get()) {
        try {
          scheduler.start();
        } catch (Throwable e) {
          throw new ShuffleError("Error during shuffle", e);
        }
      }
      // The ShuffleScheduler may have exited cleanly as a result of a shutdown invocation
      // triggered by a previously reportedException. Check before proceeding further.s
      synchronized (Shuffle.this) {
        if (throwable.get() != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
              throwable.get());
        }
      }

      shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);

      // stop the scheduler
      cleanupShuffleScheduler();

      // Finish the on-going merges...
      TezRawKeyValueIterator kvIter = null;
      inputContext.notifyProgress();
      try {
        kvIter = merger.close();
      } catch (Throwable e) {
        // Set the throwable so that future.get() sees the reported errror.
        throwable.set(e);
        throw new ShuffleError("Error while doing final merge ", e);
      }
      mergePhaseTime.setValue(System.currentTimeMillis() - startTime);
      
      inputContext.notifyProgress();
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

  private void cleanupShuffleSchedulerIgnoreErrors() {
    try {
      cleanupShuffleScheduler();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info(srcNameTrimmed + ": " + "Interrupted while attempting to close the scheduler during cleanup. Ignoring");
    }
  }

  private void cleanupShuffleScheduler() throws InterruptedException {
    if (!schedulerClosed.getAndSet(true)) {
      scheduler.close();
    }
  }

  private void cleanupMerger(boolean ignoreErrors) throws Throwable {
    if (!mergerClosed.getAndSet(true)) {
      try {
        merger.close();
      } catch (InterruptedException e) {
        if (ignoreErrors) {
          //Reset the status
          Thread.currentThread().interrupt();
          LOG.info(srcNameTrimmed + ": " + "Interrupted while attempting to close the merger during cleanup. Ignoring");
        } else {
          throw e;
        }
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
      cleanupShuffleSchedulerIgnoreErrors();
      cleanupMerger(true);
    } catch (Throwable t) {
      LOG.info(srcNameTrimmed + ": " + "Error in cleaning up.., ", t);
    }
  }

  @Private
  @Override
  public synchronized void reportException(Throwable t) {
    // RunShuffleCallable onFailure deals with ignoring errors on shutdown.
    if (throwable.get() == null) {
      LOG.info(srcNameTrimmed + ": " + "Setting throwable in reportException with message [" + t.getMessage() +
          "] from thread [" + Thread.currentThread().getName());
      throwable.set(t);
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the 
      // exception immediately.
      cleanupShuffleSchedulerIgnoreErrors();
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
        // In case of an abort / Interrupt - the runtime makes sure that this is ignored.
        inputContext.fatalError(t, "Shuffle Runner Failed");
        cleanupIgnoreErrors();
      }
    }
  }
}
