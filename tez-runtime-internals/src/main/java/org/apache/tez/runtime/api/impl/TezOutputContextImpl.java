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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.runtime.internals.api.TezTrapEvent;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.common.resources.MemoryDistributor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class TezOutputContextImpl extends TezTaskContextImpl
    implements OutputContext {

  private static final Logger LOG = LoggerFactory.getLogger(TezOutputContextImpl.class);

  private volatile UserPayload userPayload;

  /**
   * Holds whether we can accept more events to send to the AM.
   */
  private volatile boolean trapEvents;
  private final String destinationVertexName;
  private final EventMetaData sourceInfo;
  private final int outputIndex;
  private final OutputStatisticsReporterImpl statsReporter;

  /**
   * Handler for the events after the trap flag is set.
   */
  private EventHandler<TezTrapEvent> trapEventHandler;

  class OutputStatisticsReporterImpl implements OutputStatisticsReporter {

    @Override
    public synchronized void reportDataSize(long size) {
      // this is a concurrent map. Plus we are not adding/deleting entries
      runtimeTask.getTaskStatistics().getIOStatistics().get(destinationVertexName)
      .setDataSize(size);
    }

    @Override
    public void reportItemsProcessed(long items) {
      // this is a concurrent map. Plus we are not adding/deleting entries
      runtimeTask.getTaskStatistics().getIOStatistics().get(destinationVertexName)
      .setItemsProcessed(items);
    }
    
  }

  @Private
  public TezOutputContextImpl(Configuration conf, String[] workDirs, int appAttemptNumber,
      TezUmbilical tezUmbilical, String dagName,
      String taskVertexName,
      String destinationVertexName,
      int vertexParallelism,
      TezTaskAttemptID taskAttemptID, int outputIndex,
      @Nullable UserPayload userPayload, LogicalIOProcessorRuntimeTask runtimeTask,
      Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> auxServiceEnv, MemoryDistributor memDist,
      OutputDescriptor outputDescriptor, ObjectRegistry objectRegistry,
      ExecutionContext executionContext, long memAvailable, TezExecutors sharedExecutor) {
    super(conf, workDirs, appAttemptNumber, dagName, taskVertexName, 
        vertexParallelism, taskAttemptID,
        wrapCounters(runtimeTask, taskVertexName, destinationVertexName, conf),
        runtimeTask, tezUmbilical, serviceConsumerMetadata,
        auxServiceEnv, memDist, outputDescriptor, objectRegistry, executionContext, memAvailable,
        sharedExecutor);
    Objects.requireNonNull(outputIndex, "outputIndex is null");
    Objects.requireNonNull(destinationVertexName, "destinationVertexName is null");
    this.userPayload = userPayload;
    this.outputIndex = outputIndex;
    this.destinationVertexName = destinationVertexName;
    this.sourceInfo = new EventMetaData(EventProducerConsumerType.OUTPUT,
        taskVertexName, destinationVertexName, taskAttemptID);
    runtimeTask.getTaskStatistics().addIO(destinationVertexName);
    statsReporter = new OutputStatisticsReporterImpl();
  }

  private static TezCounters wrapCounters(LogicalIOProcessorRuntimeTask runtimeTask, String taskVertexName,
      String edgeVertexName, Configuration conf) {
    TezCounters tezCounters = runtimeTask.addAndGetTezCounter(edgeVertexName);
    if (conf.getBoolean(TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO,
        TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO_DEFAULT)) {
      return new TezCountersDelegate(tezCounters, taskVertexName, edgeVertexName, "OUTPUT");
    } else {
      return tezCounters;
    }
  }

  @Override
  public void sendEvents(List<Event> events) {
    Objects.requireNonNull(events, "events are null");
    List<TezEvent> tezEvents = new ArrayList<TezEvent>(events.size());
    for (Event e : events) {
      TezEvent tEvt = new TezEvent(e, sourceInfo);
      tezEvents.add(tEvt);
    }
    if (trapEvents) {
      trapEventHandler.handle(new TezTrapEvent(tezEvents));
    } else {
      tezUmbilical.addEvents(tezEvents);
    }
  }

  @Override
  public UserPayload getUserPayload() {
    return userPayload;
  }

  @Override
  public String getDestinationVertexName() {
    return destinationVertexName;
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
  public int getOutputIndex() {
    return outputIndex;
  }

  @Override
  public OutputStatisticsReporter getStatisticsReporter() {
    return statsReporter;
  }

  /**
   * This will monitor some of the events that will be sent.
   */
  @Override
  public final void trapEvents(final EventHandler eventHandler) {
    trapEvents = true;
    this.trapEventHandler = eventHandler;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.userPayload = null;
    LOG.debug("Cleared TezOutputContextImpl related information");
  }
}
