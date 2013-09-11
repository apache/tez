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

package org.apache.tez.engine.newapi.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.TezTaskContext;
import org.apache.tez.engine.newruntime.RuntimeTask;

public abstract class TezTaskContextImpl implements TezTaskContext {

  private final Configuration conf;
  protected final String taskVertexName;
  private final TezTaskAttemptID taskAttemptID;
  private final TezCounters counters;
  private String[] workDirs;
  protected String uniqueIdentifier;
  protected final RuntimeTask runtimeTask;
  protected final TezUmbilical tezUmbilical;

  @Private
  public TezTaskContextImpl(Configuration conf,
      String taskVertexName, TezTaskAttemptID taskAttemptID,
      TezCounters counters, RuntimeTask runtimeTask,
      TezUmbilical tezUmbilical) {
    this.conf = conf;
    this.taskVertexName = taskVertexName;
    this.taskAttemptID = taskAttemptID;
    this.counters = counters;
    // TODO Maybe change this to be task id specific at some point. For now
    // Shuffle code relies on this being a path specified by YARN
    this.workDirs = this.conf.getStrings(TezJobConfig.LOCAL_DIRS);
    this.runtimeTask = runtimeTask;
    this.tezUmbilical = tezUmbilical;
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
  public int getAttemptNumber() {
    return taskAttemptID.getId();
  }

  @Override
  public String getDAGName() {
    // TODO NEWTEZ Change to some form of the DAG name, for now using dagId as
    // the unique identifier.
    return taskAttemptID.getTaskID().getVertexID().getDAGId().toString();
  }

  @Override
  public String getTaskVertexName() {
    return taskVertexName;
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
    // TODO NEWTEZ Make sure this data is set by the AM for the Shuffle service name.
    return null;
  }

  @Override
  public ByteBuffer getServiceProviderMetaData(String serviceName) {
    return AuxiliaryServiceHelper.getServiceDataFromEnv(
        serviceName, System.getenv());
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
}
