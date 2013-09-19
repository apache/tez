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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;

public class InputSpec implements Writable {

  private String sourceVertexName;
  private InputDescriptor inputDescriptor;
  private int physicalEdgeCount;

  public InputSpec() {
  }

  public InputSpec(String sourceVertexName, InputDescriptor inputDescriptor,
      int physicalEdgeCount) {
    this.sourceVertexName = sourceVertexName;
    this.inputDescriptor = inputDescriptor;
    this.physicalEdgeCount = physicalEdgeCount;
  }

  public String getSourceVertexName() {
    return sourceVertexName;
  }

  public InputDescriptor getInputDescriptor() {
    return inputDescriptor;
  }

  public int getPhysicalEdgeCount() {
    return physicalEdgeCount;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODONEWTEZ convert to PB
    out.writeUTF(sourceVertexName);
    out.writeInt(physicalEdgeCount);
    byte[] inputDescBytes =
        DagTypeConverters.convertToDAGPlan(inputDescriptor).toByteArray();
    out.writeInt(inputDescBytes.length);
    out.write(inputDescBytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sourceVertexName = in.readUTF();
    physicalEdgeCount = in.readInt();
    int inputDescLen = in.readInt();
    byte[] inputDescBytes = new byte[inputDescLen];
    in.readFully(inputDescBytes);
    inputDescriptor =
        DagTypeConverters.convertInputDescriptorFromDAGPlan(
            TezEntityDescriptorProto.parseFrom(inputDescBytes));
  }

  public String toString() {
    return "{ sourceVertexName=" + sourceVertexName
        + ", physicalEdgeCount" + physicalEdgeCount
        + ", inputClassName=" + inputDescriptor.getClassName()
        + " }";
  }

}
