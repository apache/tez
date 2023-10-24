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

package org.apache.tez.runtime;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.metrics.TaskCounterUpdater;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tez.dag.api.TezConfiguration.TEZ_TASK_LOCAL_FS_WRITE_LIMIT_BYTES;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_TASK_LOCAL_FS_WRITE_LIMIT_BYTES_DEFAULT;

public abstract class RuntimeTask {

  protected final AtomicBoolean errorReported = new AtomicBoolean(false);
  protected float progress;
  protected final TezCounters tezCounters;
  private final Map<String, TezCounters> counterMap = Maps.newConcurrentMap();
  
  protected final TaskSpec taskSpec;
  protected final Configuration tezConf;
  protected final TezUmbilical tezUmbilical;
  protected final AtomicInteger eventCounter;
  protected final AtomicInteger nextFromEventId;
  protected final AtomicInteger nextPreRoutedEventId;
  private final AtomicBoolean taskDone;
  private final TaskCounterUpdater counterUpdater;
  private final TaskStatistics statistics;
  private final AtomicBoolean progressNotified = new AtomicBoolean(false);

  private final long lfsBytesWriteLimit;
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeTask.class);

  protected RuntimeTask(TaskSpec taskSpec, Configuration tezConf,
      TezUmbilical tezUmbilical, String pid, boolean setupSysCounterUpdater) {
    this.taskSpec = taskSpec;
    this.tezConf = tezConf;
    this.tezUmbilical = tezUmbilical;
    this.tezCounters = new TezCounters();
    this.eventCounter = new AtomicInteger(0);
    this.nextFromEventId = new AtomicInteger(0);
    this.nextPreRoutedEventId = new AtomicInteger(0);
    this.progress = 0.0f;
    this.taskDone = new AtomicBoolean(false);
    this.statistics = new TaskStatistics();
    if (setupSysCounterUpdater) {
      this.counterUpdater = new TaskCounterUpdater(tezCounters, tezConf, pid);
    } else {
      this.counterUpdater = null;
    }
    this.lfsBytesWriteLimit =
        tezConf.getLong(TEZ_TASK_LOCAL_FS_WRITE_LIMIT_BYTES, TEZ_TASK_LOCAL_FS_WRITE_LIMIT_BYTES_DEFAULT);
  }

  protected enum State {
    NEW, INITED, RUNNING, CLOSED;
  }

  protected final AtomicReference<State> state = new AtomicReference<State>();

  public boolean isRunning() {
    return (state.get() == State.RUNNING);
  }

  public TezCounters addAndGetTezCounter(String name) {
    TezCounters counter = new TezCounters();
    counterMap.put(name, counter);
    return counter;
  }
  
  public boolean hasInitialized() {
    return EnumSet.of(State.RUNNING, State.CLOSED).contains(state.get());
  }
  
  public String getVertexName() {
    return taskSpec.getVertexName();
  }

  public void registerError() {
    errorReported.set(true);
  }
  
  public final void notifyProgressInvocation() {
    progressNotified.lazySet(true);
  }
  
  public boolean getAndClearProgressNotification() {
    boolean retVal = progressNotified.getAndSet(false);
    return retVal;
  }

  public boolean wasErrorReported() {
    return errorReported.get();
  }

  public synchronized void setProgress(float progress) {
    this.progress = progress;
  }

  public synchronized float getProgress() {
    return this.progress;
  }

  public TezCounters getCounters() {
    TezCounters fullCounters = new TezCounters();
    fullCounters.incrAllCounters(tezCounters);
    for (TezCounters counter : counterMap.values()) {
      fullCounters.incrAllCounters(counter);
    }
    return fullCounters;
  }

  public TaskStatistics getTaskStatistics() {
    return statistics;
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return taskSpec.getTaskAttemptID();
  }

  public abstract int getMaxEventsToHandle();

  public abstract void handleEvents(Collection<TezEvent> events);

  public int getEventCounter() {
    return eventCounter.get();
  }
  
  public int getNextFromEventId() {
    return nextFromEventId.get();
  }
  
  public int getNextPreRoutedEventId() {
    return nextPreRoutedEventId.get();
  }
  
  public void setNextFromEventId(int nextFromEventId) {
    this.nextFromEventId.set(nextFromEventId);
  }
  
  public void setNextPreRoutedEventId(int nextPreRoutedEventId) {
    this.nextPreRoutedEventId.set(nextPreRoutedEventId);
  }

  public boolean isTaskDone() {
    return taskDone.get();
  }
  
  public void setFrameworkCounters() {
    if (counterUpdater != null) {
      this.counterUpdater.updateCounters();
    }
  }

  protected void setTaskDone() {
    taskDone.set(true);
  }

  public abstract void abortTask();

  protected final boolean isUpdatingSystemCounters() {
    return counterUpdater != null;
  }

  /**
   * Check whether the task has exceeded any configured limits.
   *
   * @throws TaskLimitException in case the limit is exceeded.
   */
  public void checkTaskLimits() throws TaskLimitException {
    // check the limit for writing to local file system
    if (lfsBytesWriteLimit >= 0) {
      Long lfsBytesWritten = null;
      try {
        LocalFileSystem localFS = FileSystem.getLocal(tezConf);
        lfsBytesWritten = FileSystem.getGlobalStorageStatistics().get(localFS.getScheme()).getLong("bytesWritten");
      } catch (IOException e) {
        LOG.warn("Could not get LocalFileSystem bytesWritten counter");
      }
      if (lfsBytesWritten != null && lfsBytesWritten > lfsBytesWriteLimit) {
        throw new TaskLimitException(
            "Too much write to local file system." + " current value is " + lfsBytesWritten + " the limit is "
                + lfsBytesWriteLimit);
      }
    }
  }

  /**
   * Exception thrown when the task exceeds some configured limits.
   */
  public static class TaskLimitException extends IOException {
    public TaskLimitException(String str) {
      super(str);
    }
  }
}
