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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.metrics.TaskCounterUpdater;

public abstract class RuntimeTask {

  protected AtomicBoolean hasFatalError = new AtomicBoolean(false);
  protected AtomicReference<Throwable> fatalError = new AtomicReference<Throwable>();
  protected String fatalErrorMessage = null;
  protected float progress;
  protected final TezCounters tezCounters;
  protected final TaskSpec taskSpec;
  protected final Configuration tezConf;
  protected final TezUmbilical tezUmbilical;
  protected final AtomicInteger eventCounter;
  private final AtomicBoolean taskDone;
  private final TaskCounterUpdater counterUpdater;

  protected RuntimeTask(TaskSpec taskSpec, Configuration tezConf,
      TezUmbilical tezUmbilical, String pid) {
    this.taskSpec = taskSpec;
    this.tezConf = tezConf;
    this.tezUmbilical = tezUmbilical;
    this.tezCounters = new TezCounters();
    this.eventCounter = new AtomicInteger(0);
    this.progress = 0.0f;
    this.taskDone = new AtomicBoolean(false);
    this.counterUpdater = new TaskCounterUpdater(tezCounters, tezConf, pid);
  }

  protected enum State {
    NEW, INITED, RUNNING, CLOSED;
  }

  protected final AtomicReference<State> state = new AtomicReference<State>();

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
    return this.tezCounters;
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return taskSpec.getTaskAttemptID();
  }

  public abstract void handleEvents(Collection<TezEvent> events);

  public int getEventCounter() {
    return eventCounter.get();
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
