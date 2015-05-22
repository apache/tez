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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.metrics.TaskCounterUpdater;

import com.google.common.collect.Maps;

public abstract class RuntimeTask {

  protected AtomicBoolean hasFatalError = new AtomicBoolean(false);
  protected AtomicReference<Throwable> fatalError = new AtomicReference<Throwable>();
  protected String fatalErrorMessage = null;
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

  protected RuntimeTask(TaskSpec taskSpec, Configuration tezConf,
      TezUmbilical tezUmbilical, String pid) {
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
    this.counterUpdater = new TaskCounterUpdater(tezCounters, tezConf, pid);
  }

  protected enum State {
    NEW, INITED, RUNNING, CLOSED;
  }

  protected final AtomicReference<State> state = new AtomicReference<State>();

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

  public void setFatalError(Throwable t, String message) {
    hasFatalError.set(true);
    this.fatalError.set(t);
    this.fatalErrorMessage = message;
  }
  
  public Throwable getFatalError() {
    return this.fatalError.get();
  }

  public boolean hadFatalError() {
    return hasFatalError.get();
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
    this.counterUpdater.updateCounters();
  }

  protected void setTaskDone() {
    taskDone.set(true);
  }

}
