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

package org.apache.tez.dag.app.dag.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;

public class TezRootInputInitializerContextImpl implements
    TezRootInputInitializerContext {

  private RootInputLeafOutputDescriptor<InputDescriptor> input;
  private final Vertex vertex;
  private final AppContext appContext;

  // TODO Add support for counters - merged with the Vertex counters.

  public TezRootInputInitializerContextImpl(RootInputLeafOutputDescriptor<InputDescriptor> input, Vertex vertex,
                                            AppContext appContext) {
    checkNotNull(input, "input is null");
    checkNotNull(vertex, "vertex is null");
    checkNotNull(appContext, "appContext is null");
    this.input = input;
    this.vertex = vertex;
    this.appContext = appContext;
  }

  @Override
  public ApplicationId getApplicationId() {
    return vertex.getVertexId().getDAGId().getApplicationId();
  }

  @Override
  public String getDAGName() {
    return vertex.getDAG().getName();
  }

  @Override
  public String getInputName() {
    return this.input.getEntityName();
  }

  @Override
  public byte[] getUserPayload() {
    return this.input.getDescriptor().getUserPayload();
  }
  
  @Override 
  public int getNumTasks() {
    return vertex.getTotalTasks();
  }

  @Override
  public Resource getVertexTaskResource() {
    return vertex.getTaskResource();
  }

  @Override
  public Resource getTotalAvailableResource() {
    return appContext.getTaskScheduler().getTotalResources();
  }

  @Override
  public int getNumClusterNodes() {
    return appContext.getTaskScheduler().getNumClusterNodes();
  }

  @Override
  public int getDAGAttemptNumber() {
    return appContext.getApplicationAttemptId().getAttemptId();
  }

  @Override
  public int getVertexNumTasks(String vertexName) {
    return appContext.getCurrentDAG().getVertex(vertexName).getTotalTasks();
  }

}
