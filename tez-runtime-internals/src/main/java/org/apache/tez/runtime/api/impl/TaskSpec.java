/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.runtime.api.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class TaskSpec implements Writable {

  private TezTaskAttemptID taskAttemptId;
  private String vertexName;
  private String user;
  private ProcessorDescriptor processorDescriptor;
  private List<InputSpec> inputSpecList;
  private List<OutputSpec> outputSpecList;

  public TaskSpec() {
  }

  // TODO NEWTEZ Remove user
  public TaskSpec(TezTaskAttemptID taskAttemptID, String user,
      String vertexName, ProcessorDescriptor processorDescriptor,
      List<InputSpec> inputSpecList, List<OutputSpec> outputSpecList) {
    this.taskAttemptId = taskAttemptID;
    this.vertexName = vertexName;
    this.user = user;
    this.processorDescriptor = processorDescriptor;
    this.inputSpecList = inputSpecList;
    this.outputSpecList = outputSpecList;
  }

  public String getVertexName() {
    return vertexName;
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return taskAttemptId;
  }

  public String getUser() {
    return user;
  }

  public ProcessorDescriptor getProcessorDescriptor() {
    return processorDescriptor;
  }

  public List<InputSpec> getInputs() {
    return inputSpecList;
  }

  public List<OutputSpec> getOutputs() {
    return outputSpecList;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskAttemptId.write(out);
    out.writeUTF(vertexName);
    byte[] procDesc =
        DagTypeConverters.convertToDAGPlan(processorDescriptor).toByteArray();
    out.writeInt(procDesc.length);
    out.write(procDesc);
    out.writeInt(inputSpecList.size());
    for (InputSpec inputSpec : inputSpecList) {
      inputSpec.write(out);
    }
    out.writeInt(outputSpecList.size());
    for (OutputSpec outputSpec : outputSpecList) {
      outputSpec.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskAttemptId = new TezTaskAttemptID();
    taskAttemptId.readFields(in);
    vertexName = in.readUTF();
    int procDescLength = in.readInt();
    // TODO at least 3 buffer copies here. Need to convert this to full PB
    // TEZ-305
    byte[] procDescBytes = new byte[procDescLength];
    in.readFully(procDescBytes);
    processorDescriptor =
        DagTypeConverters.convertProcessorDescriptorFromDAGPlan(
            TezEntityDescriptorProto.parseFrom(procDescBytes));
    int numInputSpecs = in.readInt();
    inputSpecList = new ArrayList<InputSpec>(numInputSpecs);
    for (int i = 0; i < numInputSpecs; i++) {
      InputSpec inputSpec = new InputSpec();
      inputSpec.readFields(in);
      inputSpecList.add(inputSpec);
    }
    int numOutputSpecs = in.readInt();
    outputSpecList = new ArrayList<OutputSpec>(numOutputSpecs);
    for (int i = 0; i < numOutputSpecs; i++) {
      OutputSpec outputSpec = new OutputSpec();
      outputSpec.readFields(in);
      outputSpecList.add(outputSpec);
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("TaskAttemptID:" + taskAttemptId);
    sb.append("processorName=" + processorDescriptor.getClassName()
        + ", inputSpecListSize=" + inputSpecList.size()
        + ", outputSpecListSize=" + outputSpecList.size());
    sb.append(", inputSpecList=[");
    for (InputSpec i : inputSpecList) {
      sb.append("{" + i.toString() + "}, ");
    }
    sb.append("], outputSpecList=[");
    for (OutputSpec i : outputSpecList) {
      sb.append("{" + i.toString() + "}, ");
    }
    sb.append("]");
    return sb.toString();
  }

}
