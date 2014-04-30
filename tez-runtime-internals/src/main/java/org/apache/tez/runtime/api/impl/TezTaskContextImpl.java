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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezEntityDescriptor;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.RuntimeTask;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.TezTaskContext;
import org.apache.tez.runtime.common.resources.MemoryDistributor;

import com.google.common.base.Preconditions;

public abstract class TezTaskContextImpl implements TezTaskContext {

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
  protected final TezEntityDescriptor descriptor;
  private final String dagName;

  @Private
  public TezTaskContextImpl(Configuration conf, int appAttemptNumber,
      String dagName, String taskVertexName, TezTaskAttemptID taskAttemptID,
      TezCounters counters, RuntimeTask runtimeTask,
      TezUmbilical tezUmbilical, Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> auxServiceEnv, MemoryDistributor memDist,
      TezEntityDescriptor descriptor) {
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
    this.workDirs = this.conf.getStrings(TezJobConfig.LOCAL_DIRS);
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
  public String[] getWorkDirs() {
    return Arrays.copyOf(workDirs, workDirs.length);
  }

  @Override
  public String getUniqueIdentifier() {
    return uniqueIdentifier;
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
  
  protected void signalFatalError(Throwable t, String message,
      EventMetaData sourceInfo) {
    runtimeTask.setFatalError(t, message);
    String diagnostics;
    if (t != null && message != null) {
      diagnostics = "exceptionThrown=" + StringUtils.stringifyException(t)
          + ", errorMessage=" + message;
    } else if (t == null && message == null) {
      diagnostics = "Unknown error";
    } else {
      diagnostics = t != null ?
          "exceptionThrown=" + StringUtils.stringifyException(t)
          : " errorMessage=" + message;
    }
    tezUmbilical.signalFatalError(taskAttemptID, diagnostics, sourceInfo);
  }

  private int generateId() {
    return ID_GEN.incrementAndGet();
  }
}
