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

package org.apache.tez.engine.newruntime;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tez.common.counters.TezCounters;

public abstract class RuntimeTask {

  protected AtomicBoolean hasFatalError = new AtomicBoolean(false);
  protected Throwable fatalError = null;
  protected String fatalErrorMessage = null;
  protected float progress;
  protected final TezCounters tezCounters = new TezCounters();

  protected enum State {
    NEW, INITED, RUNNING, CLOSED;
  }

  protected State state;

  public void setFatalError(Throwable t, String message) {
    hasFatalError.set(true);
    this.fatalError = t;
    this.fatalErrorMessage = message;
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

}
