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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.TezTaskContext;

public abstract class TezTaskContextImpl implements TezTaskContext {

  private final Configuration tezConf;
  private final String vertexName;
  private final TezTaskAttemptID taskAttemptID;
  private final TezCounters counters;

  @Private
  public TezTaskContextImpl(Configuration tezConf,
      String vertexName, TezTaskAttemptID taskAttemptID,
      TezCounters counters) {
    this.tezConf = tezConf;
    this.vertexName = vertexName;
    this.taskAttemptID = taskAttemptID;
    this.counters = counters;
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
  public String getVertexName() {
    return vertexName;
  }

  @Override
  public TezCounters getCounters() {
    return counters;
  }

  // TODO Add a method to get working dir
}
