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
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.OutputCommitterContext;

public class OutputCommitterContextImpl implements OutputCommitterContext {

  private final ApplicationId applicationId;
  private final int dagAttemptNumber;
  private final String dagName;
  private final String vertexName;
  private final int vertexIdx;
  private final RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> output;

  public OutputCommitterContextImpl(ApplicationId applicationId,
      int dagAttemptNumber,
      String dagName,
      String vertexName,
      RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> output,
      int vertexIdx) {
    checkNotNull(applicationId, "applicationId is null");
    checkNotNull(dagName, "dagName is null");
    checkNotNull(vertexName, "vertexName is null");
    checkNotNull(output, "output is null");
    this.applicationId = applicationId;
    this.dagAttemptNumber = dagAttemptNumber;
    this.dagName = dagName;
    this.vertexName = vertexName;
    this.output = output;
    this.vertexIdx = vertexIdx;
  }

  @Override
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  @Override
  public int getDAGAttemptNumber() {
    return dagAttemptNumber;
  }

  @Override
  public String getDAGName() {
    return dagName;
  }

  @Override
  public String getVertexName() {
    return vertexName;
  }

  @Override
  public String getOutputName() {
    return output.getName();
  }

  @Override
  public UserPayload getOutputUserPayload() {
    return output.getIODescriptor().getUserPayload();
  }
  
  @Override
  public UserPayload getUserPayload() {
    return output.getControllerDescriptor().getUserPayload();
  }

  @Override
  public int getVertexIndex() {
    return vertexIdx;
  }

}
