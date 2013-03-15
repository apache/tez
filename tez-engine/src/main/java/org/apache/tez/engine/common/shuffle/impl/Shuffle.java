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
package org.apache.tez.engine.common.shuffle.impl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.util.Progress;
import org.apache.tez.api.Processor;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTask;
import org.apache.tez.common.TezTaskReporter;
import org.apache.tez.common.TezTaskStatus;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Shuffle implements ExceptionReporter {
  
  private static final Log LOG = LogFactory.getLog(Shuffle.class);
  private static final int PROGRESS_FREQUENCY = 2000;
  private static final int MAX_EVENTS_TO_FETCH = 10000;
  private static final int MIN_EVENTS_TO_FETCH = 100;
  private static final int MAX_RPC_OUTSTANDING_EVENTS = 3000000;

  private final TezTask task;
  private final Configuration conf;
  private final TezTaskReporter reporter;
  private final ShuffleClientMetrics metrics;
  
  private final ShuffleScheduler scheduler;
  private final MergeManager merger;
  private Throwable throwable = null;
  private String throwingThreadName = null;
  private final Progress copyPhase;
  private final Progress mergePhase;
  private final int tasksInDegree;
  
  public Shuffle(TezTask task, 
                 Configuration conf,
                 int tasksInDegree,
                 TezTaskReporter reporter,
                 Processor combineProcessor
                 ) throws IOException {
    this.task = task;
    this.conf = conf;
    this.reporter = reporter;
    this.metrics = 
        new ShuffleClientMetrics(
            task.getTaskAttemptId(), this.conf, 
            this.task.getUser(), this.task.getJobName());
    this.tasksInDegree = tasksInDegree;
    
    FileSystem localFS = FileSystem.getLocal(this.conf);
    LocalDirAllocator localDirAllocator = 
        new LocalDirAllocator(TezJobConfig.LOCAL_DIR);
    
    copyPhase = this.task.getProgress().addPhase("copy", 0.33f);
    mergePhase = this.task.getProgress().addPhase("merge", 0.66f);

    TezCounter shuffledMapsCounter = 
        reporter.getCounter(TaskCounter.SHUFFLED_MAPS);
    TezCounter reduceShuffleBytes =
        reporter.getCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
    TezCounter failedShuffleCounter =
        reporter.getCounter(TaskCounter.FAILED_SHUFFLE);
    TezCounter spilledRecordsCounter = 
        reporter.getCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter reduceCombineInputCounter =
        reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    TezCounter mergedMapOutputsCounter =
        reporter.getCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    
    scheduler = 
      new ShuffleScheduler(this.conf, tasksInDegree, task.getStatus(), 
                                this, copyPhase, 
                                shuffledMapsCounter, 
                                reduceShuffleBytes, 
                                failedShuffleCounter);
    merger = new MergeManager(this.task.getTaskAttemptId(), 
                                    this.conf, localFS, 
                                    localDirAllocator, reporter, 
                                    combineProcessor, 
                                    spilledRecordsCounter, 
                                    reduceCombineInputCounter, 
                                    mergedMapOutputsCounter, 
                                    this, mergePhase);
  }

  public TezRawKeyValueIterator run() throws IOException, InterruptedException {
    // Scale the maximum events we fetch per RPC call to mitigate OOM issues
    // on the ApplicationMaster when a thundering herd of reducers fetch events
    // TODO: This should not be necessary after HADOOP-8942
    int eventsPerReducer = Math.max(MIN_EVENTS_TO_FETCH,
        MAX_RPC_OUTSTANDING_EVENTS / tasksInDegree);
    int maxEventsToFetch = Math.min(MAX_EVENTS_TO_FETCH, eventsPerReducer);

    // Start the map-completion events fetcher thread
    final EventFetcher eventFetcher = 
      new EventFetcher(task.getTaskAttemptId(), reporter, scheduler, this,
          maxEventsToFetch);
    eventFetcher.start();
    
    // Start the map-output fetcher threads
    final int numFetchers = 
        conf.getInt(
            TezJobConfig.TEZ_ENGINE_SHUFFLE_PARALLEL_COPIES, 
            TezJobConfig.DEFAULT_TEZ_ENGINE_SHUFFLE_PARALLEL_COPIES);
    Fetcher[] fetchers = new Fetcher[numFetchers];
    for (int i=0; i < numFetchers; ++i) {
      fetchers[i] = new Fetcher(conf, task.getTaskAttemptId(), 
                                     scheduler, merger, 
                                     reporter, metrics, this, 
                                     task.getJobTokenSecret());
      fetchers[i].start();
    }
    
    // Wait for shuffle to complete successfully
    while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
      reporter.progress();
      
      synchronized (this) {
        if (throwable != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                 throwable);
        }
      }
    }

    // Stop the event-fetcher thread
    eventFetcher.shutDown();
    
    // Stop the map-output fetcher threads
    for (Fetcher fetcher : fetchers) {
      fetcher.shutDown();
    }
    fetchers = null;
    
    // stop the scheduler
    scheduler.close();

    copyPhase.complete(); // copy is already complete
    task.getStatus().setPhase(TezTaskStatus.Phase.SORT);
    
    task.statusUpdate();
    
    // Finish the on-going merges...
    TezRawKeyValueIterator kvIter = null;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new ShuffleError("Error while doing final merge " , e);
    }

    // Sanity check
    synchronized (this) {
      if (throwable != null) {
        throw new ShuffleError("error in shuffle in " + throwingThreadName,
                               throwable);
      }
    }
    
    return kvIter;
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
}
