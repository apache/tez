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

import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputStatisticsReporter;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.common.resources.MemoryDistributor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezInputContextImpl extends TezTaskContextImpl
    implements InputContext {

  private static final Logger LOG = LoggerFactory.getLogger(TezInputContextImpl.class);

  private volatile UserPayload userPayload;
  private final String sourceVertexName;
  private final EventMetaData sourceInfo;
  private final int inputIndex;
  private final Map<String, LogicalInput> inputs;
  private volatile InputReadyTracker inputReadyTracker;
  private final InputStatisticsReporterImpl statsReporter;
  
  class InputStatisticsReporterImpl implements InputStatisticsReporter {

    @Override
    public synchronized void reportDataSize(long size) {
      // this is a concurrent map. Plus we are not adding/deleting entries
      runtimeTask.getTaskStatistics().getIOStatistics().get(sourceVertexName)
          .setDataSize(size);
    }

    @Override
    public void reportItemsProcessed(long items) {
      // this is a concurrent map. Plus we are not adding/deleting entries
      runtimeTask.getTaskStatistics().getIOStatistics().get(sourceVertexName)
          .setItemsProcessed(items);      
    }
    
  }

  @Private
  public TezInputContextImpl(Configuration conf, String[] workDirs,
                             int appAttemptNumber,
                             TezUmbilical tezUmbilical, String dagName, 
                             String taskVertexName, String sourceVertexName,
                             int vertexParallelism, TezTaskAttemptID taskAttemptID,
                             int inputIndex, @Nullable UserPayload userPayload,
                             LogicalIOProcessorRuntimeTask runtimeTask,
                             Map<String, ByteBuffer> serviceConsumerMetadata,
                             Map<String, String> auxServiceEnv, MemoryDistributor memDist,
                             InputDescriptor inputDescriptor, Map<String, LogicalInput> inputs,
                             InputReadyTracker inputReadyTracker, ObjectRegistry objectRegistry,
                             ExecutionContext ExecutionContext, long memAvailable) {
    super(conf, workDirs, appAttemptNumber, dagName, taskVertexName,
        vertexParallelism, taskAttemptID, wrapCounters(runtimeTask,
        taskVertexName, sourceVertexName, conf), runtimeTask, tezUmbilical,
        serviceConsumerMetadata, auxServiceEnv, memDist, inputDescriptor,
        objectRegistry, ExecutionContext, memAvailable);
    checkNotNull(inputIndex, "inputIndex is null");
    checkNotNull(sourceVertexName, "sourceVertexName is null");
    checkNotNull(inputs, "input map is null");
    checkNotNull(inputReadyTracker, "inputReadyTracker is null");
    this.userPayload = userPayload;
    this.inputIndex = inputIndex;
    this.sourceVertexName = sourceVertexName;
    this.sourceInfo = new EventMetaData(
        EventProducerConsumerType.INPUT, taskVertexName, sourceVertexName,
        taskAttemptID);
    this.inputs = inputs;
    this.inputReadyTracker = inputReadyTracker;
    runtimeTask.getTaskStatistics().addIO(sourceVertexName);
    statsReporter = new InputStatisticsReporterImpl();
  }

  private static TezCounters wrapCounters(LogicalIOProcessorRuntimeTask task, String taskVertexName,
      String edgeVertexName, Configuration conf) {
    TezCounters tezCounters = task.addAndGetTezCounter(edgeVertexName);
    if (conf.getBoolean(TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO,
        TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO_DEFAULT)) {
      return new TezCountersDelegate(tezCounters, taskVertexName, edgeVertexName, "INPUT");
    } else {
      return tezCounters;
    }
  }

  @Override
  public void sendEvents(List<Event> events) {
    Preconditions.checkNotNull(events, "events are null");
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
  public int getInputIndex() {
    return inputIndex;
  }

  @Override
  public String getSourceVertexName() {
    return sourceVertexName;
  }

  @Override
  public void fatalError(Throwable exception, String message) {
    super.signalFatalError(exception, message, sourceInfo);
  }

  @Override
  public void inputIsReady() {
    if (inputReadyTracker != null) {
      inputReadyTracker.setInputIsReady(inputs.get(sourceVertexName));
    } else {
      LOG.warn("Ignoring Input Ready notification since the Task has already been closed");
    }
  }

  @Override
  public InputStatisticsReporter getStatisticsReporter() {
    return statsReporter;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.userPayload = null;
    this.inputReadyTracker = null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleared TezInputContextImpl related information");
    }
  }
}
