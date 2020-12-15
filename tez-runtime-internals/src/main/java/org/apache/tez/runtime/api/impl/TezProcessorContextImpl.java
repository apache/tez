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

package org.apache.tez.runtime.api.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.common.resources.MemoryDistributor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezProcessorContextImpl extends TezTaskContextImpl implements ProcessorContext {

  private static final Logger LOG = LoggerFactory.getLogger(TezProcessorContextImpl.class);

  private volatile UserPayload userPayload;
  private volatile InputReadyTracker inputReadyTracker;
  private final EventMetaData sourceInfo;

  public TezProcessorContextImpl(Configuration conf, String[] workDirs, int appAttemptNumber,
      TezUmbilical tezUmbilical, String dagName, String vertexName,
      int vertexParallelism, TezTaskAttemptID taskAttemptID,
      @Nullable UserPayload userPayload, LogicalIOProcessorRuntimeTask runtimeTask,
      Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> auxServiceEnv, MemoryDistributor memDist,
      ProcessorDescriptor processorDescriptor, InputReadyTracker inputReadyTracker, ObjectRegistry objectRegistry,
      ExecutionContext ExecutionContext, long memAvailable, TezExecutors sharedExecutor) {
    super(conf, workDirs, appAttemptNumber, dagName, vertexName, vertexParallelism, taskAttemptID,
        runtimeTask.addAndGetTezCounter(vertexName), runtimeTask, tezUmbilical, serviceConsumerMetadata,
        auxServiceEnv, memDist, processorDescriptor, objectRegistry, ExecutionContext, memAvailable,
        sharedExecutor);
    Objects.requireNonNull(inputReadyTracker, "inputReadyTracker is null");
    this.userPayload = userPayload;
    this.sourceInfo = new EventMetaData(EventProducerConsumerType.PROCESSOR,
        taskVertexName, "", taskAttemptID);
    this.inputReadyTracker = inputReadyTracker;
  }

  @Override
  public void sendEvents(List<Event> events) {
    Objects.requireNonNull(events, "events are null");
    List<TezEvent> tezEvents = new ArrayList<TezEvent>(events.size());
    for (Event e : events) {
      TezEvent tEvt = new TezEvent(e, sourceInfo);
      tezEvents.add(tEvt);
    }
    tezUmbilical.addEvents(tezEvents);
  }

  @Override
  public UserPayload getUserPayload() {
    return userPayload;
  }

  @Override
  public void setProgressInternally(float progress) {
    if (Float.compare(progress, runtimeTask.getProgress()) != 0) {
      runtimeTask.setProgress(progress);
      notifyProgress();
    }
  }

  @Override
  public void fatalError(Throwable exception, String message) {
    super.signalFatalError(exception, message, sourceInfo);
  }

  @Override
  public void reportFailure(TaskFailureType taskFailureType, @Nullable Throwable exception,
                            @Nullable String message) {
    super.signalFailure(taskFailureType, exception, message, sourceInfo);
  }

  @Override
  public void killSelf(@Nullable Throwable exception, @Nullable String message) {
    super.signalKillSelf(exception, message, sourceInfo);
  }

  @Override
  public boolean canCommit() throws IOException {
    return tezUmbilical.canCommit(this.taskAttemptID);
  }

  @Override
  public Input waitForAnyInputReady(Collection<Input> inputs) throws InterruptedException {
    return waitForAnyInputReady(inputs, -1);
  }

  @Override
  public Input waitForAnyInputReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException {
    return inputReadyTracker.waitForAnyInputReady(inputs, timeoutMillis);
  }

  @Override
  public void waitForAllInputsReady(Collection<Input> inputs) throws InterruptedException {
    waitForAllInputsReady(inputs, -1);
  }

  @Override
  public boolean waitForAllInputsReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException {
    return inputReadyTracker.waitForAllInputsReady(inputs, timeoutMillis);
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.userPayload = null;
    this.inputReadyTracker = null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleared TezProcessorContextImpl related information");
    }
  }

}
