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

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EntityDescriptor;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.RuntimeTask;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.common.resources.MemoryDistributor;

import com.google.common.base.Preconditions;

public abstract class TezTaskContextImpl implements TaskContext {

  private static final AtomicInteger ID_GEN = new AtomicInteger(10000);
  
  private final Configuration conf;
  protected final String taskVertexName;
  protected final TezTaskAttemptID taskAttemptID;
  private final TezCounters counters;
  private String[] workDirs;
  private String uniqueIdentifier;
  protected final RuntimeTask runtimeTask;
  protected final TezUmbilical tezUmbilical;
  private final Map<String, ByteBuffer> serviceConsumerMetadata;
  private final int appAttemptNumber;
  private final Map<String, String> auxServiceEnv;
  protected final MemoryDistributor initialMemoryDistributor;
  protected final EntityDescriptor<?> descriptor;
  private final String dagName;
  private final ObjectRegistry objectRegistry;
  private final int vertexParallelism;
  private final ExecutionContext ExecutionContext;

  @Private
  public TezTaskContextImpl(Configuration conf, String[] workDirs, int appAttemptNumber,
      String dagName, String taskVertexName, int vertexParallelism, 
      TezTaskAttemptID taskAttemptID, TezCounters counters, RuntimeTask runtimeTask,
      TezUmbilical tezUmbilical, Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> auxServiceEnv, MemoryDistributor memDist,
      EntityDescriptor<?> descriptor, ObjectRegistry objectRegistry,
      ExecutionContext ExecutionContext) {
    checkNotNull(conf, "conf is null");
    checkNotNull(dagName, "dagName is null");
    checkNotNull(taskVertexName, "taskVertexName is null");
    checkNotNull(taskAttemptID, "taskAttemptId is null");
    checkNotNull(counters, "counters is null");
    checkNotNull(runtimeTask, "runtimeTask is null");
    checkNotNull(auxServiceEnv, "auxServiceEnv is null");
    checkNotNull(memDist, "memDist is null");
    checkNotNull(descriptor, "descriptor is null");
    this.conf = conf;
    this.dagName = dagName;
    this.taskVertexName = taskVertexName;
    this.taskAttemptID = taskAttemptID;
    this.counters = counters;
    // TODO Maybe change this to be task id specific at some point. For now
    // Shuffle code relies on this being a path specified by YARN
    this.workDirs = workDirs;
    this.runtimeTask = runtimeTask;
    this.tezUmbilical = tezUmbilical;
    this.serviceConsumerMetadata = serviceConsumerMetadata;
    // TODO NEWTEZ at some point dag attempt should not map to app attempt
    this.appAttemptNumber = appAttemptNumber;
    this.auxServiceEnv = auxServiceEnv;
    this.uniqueIdentifier = String.format("%s_%05d", taskAttemptID.toString(),
        generateId());
    this.initialMemoryDistributor = memDist;
    this.descriptor = descriptor;
    this.objectRegistry = objectRegistry;
    this.vertexParallelism = vertexParallelism;
    this.ExecutionContext = ExecutionContext;
  }

  @Override
  public ApplicationId getApplicationId() {
    return taskAttemptID.getTaskID().getVertexID().getDAGId()
        .getApplicationId();
  }

  @Override
  public int getTaskIndex() {
    return taskAttemptID.getTaskID().getId();
  }

  @Override
  public int getDAGAttemptNumber() {
    return appAttemptNumber;
  }

  @Override
  public int getTaskAttemptNumber() {
    return taskAttemptID.getId();
  }

  @Override
  public String getDAGName() {
    return dagName;
  }

  @Override
  public String getTaskVertexName() {
    return taskVertexName;
  }

  @Override
  public int getTaskVertexIndex() {
    return taskAttemptID.getTaskID().getVertexID().getId();
  }

  @Override
  public TezCounters getCounters() {
    return counters;
  }

  @Override
  public int getVertexParallelism() {
    return this.vertexParallelism;
  }

  @Override
  public String[] getWorkDirs() {
    return Arrays.copyOf(workDirs, workDirs.length);
  }

  @Override
  public String getUniqueIdentifier() {
    return uniqueIdentifier;
  }
  
  @Override
  public ObjectRegistry getObjectRegistry() {
    return objectRegistry;
  }

  @Override
  public ByteBuffer getServiceConsumerMetaData(String serviceName) {
    return (ByteBuffer) serviceConsumerMetadata.get(serviceName)
        .asReadOnlyBuffer().rewind();
  }

  @Nullable
  @Override
  public ByteBuffer getServiceProviderMetaData(String serviceName) {
    Preconditions.checkNotNull(serviceName, "serviceName is null");
    return AuxiliaryServiceHelper.getServiceDataFromEnv(
        serviceName, auxServiceEnv);
  }

  @Override
  public void requestInitialMemory(long size, MemoryUpdateCallback callbackHandler) {
    // Nulls allowed since all IOs have to make this call.
    if (callbackHandler == null) {
      Preconditions.checkArgument(size == 0,
          "A Null callback handler can only be used with a request size of 0");
      callbackHandler = new MemoryUpdateCallback() {
        @Override
        public void memoryAssigned(long assignedSize) {
          
        }
      };
    }
    this.initialMemoryDistributor.requestMemory(size, callbackHandler, this, this.descriptor);
  }

  @Override
  public long getTotalMemoryAvailableToTask() {
    return Runtime.getRuntime().maxMemory();
  }
  
  protected void signalFatalError(Throwable t, String message, EventMetaData sourceInfo) {
    runtimeTask.setFrameworkCounters();
    runtimeTask.setFatalError(t, message);
    tezUmbilical.signalFatalError(taskAttemptID, t, message, sourceInfo);
  }

  @Override
  public ExecutionContext getExecutionContext() {
    return this.ExecutionContext;
  }

  private int generateId() {
    return ID_GEN.incrementAndGet();
  }
}
