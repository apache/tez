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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;

public class TezRootInputInitializerContextImpl implements
    TezRootInputInitializerContext {

  private final TezVertexID vertexID;
  private final String dagName;
  private final String inputName;
  private final InputDescriptor inputDescriptor;
  private final int numTasks;
  private final Resource vertexTaskResource;
  private final Resource totalResource;
  private final int numClusterNodes;

  // TODO Add support for counters - merged with the Vertex counters.
  
  public TezRootInputInitializerContextImpl(TezVertexID vertexID,
      String dagName, String vertexName, String inputName,
      InputDescriptor inputDescriptor, int numTasks, int numClusterNodes,
      Resource vertexTaskResource, Resource totalResource) {
    this.vertexID = vertexID;
    this.dagName = dagName;
    this.inputName = inputName;
    this.inputDescriptor = inputDescriptor;
    this.numTasks = numTasks;
    this.vertexTaskResource = vertexTaskResource;
    this.totalResource = totalResource;
    this.numClusterNodes = numClusterNodes;
  }

  @Override
  public ApplicationId getApplicationId() {
    return vertexID.getDAGId().getApplicationId();
  }

  @Override
  public String getDAGName() {
    return this.dagName;
  }

  @Override
  public String getInputName() {
    return this.inputName;
  }

  @Override
  public byte[] getUserPayload() {
    return inputDescriptor.getUserPayload();
  }
  
  @Override 
  public int getNumTasks() {
    return numTasks;
  }

  @Override
  public Resource getVertexTaskResource() {
    return vertexTaskResource;
  }

  @Override
  public Resource getTotalAvailableResource() {
    return totalResource;
  }

  @Override
  public int getNumClusterNodes() {
    return numClusterNodes;
  }

}
