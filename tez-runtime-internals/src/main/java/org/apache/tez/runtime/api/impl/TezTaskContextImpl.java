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



import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EntityDescriptor;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.common.resources.MemoryDistributor;

import org.apache.tez.common.Preconditions;

public abstract class TezTaskContextImpl implements TaskContext, Closeable {

  private static final AtomicInteger ID_GEN = new AtomicInteger(10000);

  protected final String taskVertexName;
  protected final TezTaskAttemptID taskAttemptID;
  private final TezCounters counters;
  private Configuration configuration;
  private String[] workDirs;
  private String uniqueIdentifier;
  protected final LogicalIOProcessorRuntimeTask runtimeTask;
  protected final TezUmbilical tezUmbilical;
  private final Map<String, ByteBuffer> serviceConsumerMetadata;
  private final int appAttemptNumber;
  private final Map<String, String> auxServiceEnv;
  protected volatile MemoryDistributor initialMemoryDistributor;
  protected final EntityDescriptor<?> descriptor;
  private final String dagName;
  private volatile ObjectRegistry objectRegistry;
  private final int vertexParallelism;
  private final ExecutionContext ExecutionContext;
  private final long memAvailable;
  private final TezExecutors sharedExecutor;

  @Private
  public TezTaskContextImpl(Configuration conf, String[] workDirs, int appAttemptNumber,
      String dagName, String taskVertexName, int vertexParallelism, 
      TezTaskAttemptID taskAttemptID, TezCounters counters, LogicalIOProcessorRuntimeTask runtimeTask,
      TezUmbilical tezUmbilical, Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> auxServiceEnv, MemoryDistributor memDist,
      EntityDescriptor<?> descriptor, ObjectRegistry objectRegistry,
      ExecutionContext ExecutionContext, long memAvailable, TezExecutors sharedExecutor) {
    Objects.requireNonNull(conf, "conf is null");
    Objects.requireNonNull(dagName, "dagName is null");
    Objects.requireNonNull(taskVertexName, "taskVertexName is null");
    Objects.requireNonNull(taskAttemptID, "taskAttemptId is null");
    Objects.requireNonNull(counters, "counters is null");
    Objects.requireNonNull(runtimeTask, "runtimeTask is null");
    Objects.requireNonNull(auxServiceEnv, "auxServiceEnv is null");
    Objects.requireNonNull(memDist, "memDist is null");
    Objects.requireNonNull(descriptor, "descriptor is null");
    Objects.requireNonNull(sharedExecutor, "sharedExecutor is null");
    this.dagName = dagName;
    this.configuration = conf;
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
    this.memAvailable = memAvailable;
    this.sharedExecutor = sharedExecutor;
  }

  @Override
  public ApplicationId getApplicationId() {
    return taskAttemptID.getApplicationId();
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
  public Configuration getContainerConfiguration() {
    return configuration;
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
    return taskAttemptID.getVertexID().getId();
  }

  @Override
  public int getDagIdentifier() {
    return taskAttemptID.getDAGID().getId();
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
  public final void notifyProgress() {
    runtimeTask.notifyProgressInvocation();
  }
  
  @Override
  public ByteBuffer getServiceConsumerMetaData(String serviceName) {
    return (ByteBuffer) serviceConsumerMetadata.get(serviceName)
        .asReadOnlyBuffer().rewind();
  }

  @Nullable
  @Override
  public ByteBuffer getServiceProviderMetaData(String serviceName) {
    Objects.requireNonNull(serviceName, "serviceName is null");
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
    return memAvailable;
  }
  
  protected void signalFatalError(Throwable t, String message, EventMetaData sourceInfo) {
    signalFailure(TaskFailureType.NON_FATAL, t, message, sourceInfo);
  }

  protected void signalFailure(TaskFailureType taskFailureType, Throwable t,
                               String message, EventMetaData sourceInfo) {
    Objects.requireNonNull(taskFailureType, "TaskFailureType must be specified");
    runtimeTask.setFrameworkCounters();
    runtimeTask.registerError();
    tezUmbilical.signalFailure(taskAttemptID, taskFailureType, t, message, sourceInfo);
  }

  protected void signalKillSelf(Throwable t, String message, EventMetaData sourceInfo) {
    runtimeTask.setFrameworkCounters();
    runtimeTask.registerError();
    tezUmbilical.signalKillSelf(taskAttemptID, t, message, sourceInfo);
  }

  @Override
  public ExecutionContext getExecutionContext() {
    return this.ExecutionContext;
  }

  @Override
  public ExecutorService createTezFrameworkExecutorService(
      int parallelism, String threadNameFormat) {
    return sharedExecutor.createExecutorService(parallelism, threadNameFormat);
  }

  private int generateId() {
    return ID_GEN.incrementAndGet();
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkState(runtimeTask.isTaskDone(),
        "Runtime task must be complete before calling cleanup");
    this.objectRegistry = null;
    this.initialMemoryDistributor = null;
  }
}
