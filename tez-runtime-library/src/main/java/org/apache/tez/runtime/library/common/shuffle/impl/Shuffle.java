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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.shuffle.server.ShuffleHandler;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;

import com.google.common.base.Preconditions;

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
  private Throwable throwable = null;
  private String throwingThreadName = null;
  private final int numInputs;
  private final AtomicInteger reduceStartId;
  private final SecretKey jobTokenSecret;
  private AtomicInteger reduceRange = new AtomicInteger(
      TezJobConfig.TEZ_RUNTIME_SHUFFLE_PARTITION_RANGE_DEFAULT);

  private FutureTask<TezRawKeyValueIterator> runShuffleFuture;

  public Shuffle(TezInputContext inputContext, Configuration conf, int numInputs) throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;
    this.metrics = new ShuffleClientMetrics(inputContext.getDAGName(),
        inputContext.getTaskVertexName(), inputContext.getTaskIndex(),
        this.conf, UserGroupInformation.getCurrentUser().getShortUserName());
            
    this.numInputs = numInputs;
    
    this.jobTokenSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID));
    
    Combiner combiner = TezRuntimeUtils.instantiateCombiner(conf, inputContext);
    
    FileSystem localFS = FileSystem.getLocal(this.conf);
    LocalDirAllocator localDirAllocator = 
        new LocalDirAllocator(TezJobConfig.LOCAL_DIRS);

    // TODO TEZ Get rid of Map / Reduce references.
    TezCounter shuffledMapsCounter = 
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLED_MAPS);
    TezCounter reduceShuffleBytes =
        inputContext.getCounters().findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
    TezCounter failedShuffleCounter =
        inputContext.getCounters().findCounter(TaskCounter.FAILED_SHUFFLE);
    TezCounter spilledRecordsCounter = 
        inputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter reduceCombineInputCounter =
        inputContext.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    TezCounter mergedMapOutputsCounter =
        inputContext.getCounters().findCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    
    reduceStartId = new AtomicInteger(inputContext.getTaskIndex());
    LOG.info("Shuffle assigned reduce start id: " + reduceStartId.get()
        + " with default reduce range: " + reduceRange.get());

    scheduler = new ShuffleScheduler(
          this.inputContext,
          this.conf,
          this.numInputs,
          this,
          shuffledMapsCounter,
          reduceShuffleBytes,
          failedShuffleCounter);
    eventHandler= new ShuffleInputEventHandler(
          inputContext,
          this,
          scheduler);
    merger = new MergeManager(
          this.conf,
          localFS,
          localDirAllocator,
          inputContext,
          combiner,
          spilledRecordsCounter,
          reduceCombineInputCounter,
          mergedMapOutputsCounter,
          this);
  }

  public void handleEvents(List<Event> events) {
    eventHandler.handleEvents(events);
  }
  
  /**
   * Indicates whether the Shuffle and Merge processing is complete.
   * @return false if not complete, true if complete or if an error occurred.
   */
  public boolean isInputReady() {
    if (runShuffleFuture == null) {
      return false;
    }
    return runShuffleFuture.isDone();
    //return scheduler.isDone() && merger.isMergeComplete();
  }

  /**
   * Waits for the Shuffle and Merge to complete, and returns an iterator over the input.
   * @return an iterator over the fetched input.
   * @throws IOException
   * @throws InterruptedException
   */
  public TezRawKeyValueIterator waitForInput() throws IOException, InterruptedException {
    Preconditions.checkState(runShuffleFuture != null,
        "waitForInput can only be called after run");
    TezRawKeyValueIterator kvIter;
    try {
      kvIter = runShuffleFuture.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else {
        throw new TezUncheckedException(
            "Unexpected exception type while running Shuffle and Merge", cause);
      }
    }
    return kvIter;
  }

  public void run() {
    RunShuffleCallable runShuffle = new RunShuffleCallable();
    runShuffleFuture = new FutureTask<TezRawKeyValueIterator>(runShuffle);
    new Thread(runShuffleFuture, "ShuffleMergeRunner").start();
  }
  
  private class RunShuffleCallable implements Callable<TezRawKeyValueIterator> {
    @Override
    public TezRawKeyValueIterator call() throws IOException, InterruptedException {
      // TODO NEWTEZ Limit # fetchers to number of inputs
      final int numFetchers = 
          conf.getInt(
              TezJobConfig.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES, 
              TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
      Fetcher[] fetchers = new Fetcher[numFetchers];
      for (int i = 0; i < numFetchers; ++i) {
        fetchers[i] = new Fetcher(conf, scheduler, merger, metrics, Shuffle.this, jobTokenSecret, inputContext);
        fetchers[i].start();
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
      for (Fetcher fetcher : fetchers) {
        fetcher.shutDown();
      }
      fetchers = null;
      
      // stop the scheduler
      scheduler.close();


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
      return kvIter;
    }
  }
  
  public int getReduceStartId() {
    return reduceStartId.get();
  }
  
  public int getReduceRange() {
    return reduceRange.get();
  }
  
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

  public void setPartitionRange(int range) {
    if (range == reduceRange.get()) {
      return;
    }
    if (reduceRange.compareAndSet(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_PARTITION_RANGE_DEFAULT, range)) {
      LOG.info("Reduce range set to: " + range);
    } else {
      TezUncheckedException e = 
          new TezUncheckedException("Reduce range can be set only once.");
      reportException(e);
      throw e; 
    }
  }
}
