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

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringInterner;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.records.TezTaskAttemptID;

import com.google.common.collect.Lists;

public class TaskSpec implements Writable {

  private TezTaskAttemptID taskAttemptId;
  private String dagName;
  private String vertexName;
  private ProcessorDescriptor processorDescriptor;
  private List<InputSpec> inputSpecList;
  private List<OutputSpec> outputSpecList;
  private List<GroupInputSpec> groupInputSpecList;

  public TaskSpec() {
  }

  public TaskSpec(TezTaskAttemptID taskAttemptID,
      String dagName, String vertexName,
      ProcessorDescriptor processorDescriptor,
      List<InputSpec> inputSpecList, List<OutputSpec> outputSpecList, 
      @Nullable List<GroupInputSpec> groupInputSpecList) {
    checkNotNull(taskAttemptID, "taskAttemptID is null");
    checkNotNull(dagName, "dagName is null");
    checkNotNull(vertexName, "vertexName is null");
    checkNotNull(processorDescriptor, "processorDescriptor is null");
    checkNotNull(inputSpecList, "inputSpecList is null");
    checkNotNull(outputSpecList, "outputSpecList is null");
    this.taskAttemptId = taskAttemptID;
    this.dagName = StringInterner.weakIntern(dagName);
    this.vertexName = StringInterner.weakIntern(vertexName);
    this.processorDescriptor = processorDescriptor;
    this.inputSpecList = inputSpecList;
    this.outputSpecList = outputSpecList;
    this.groupInputSpecList = groupInputSpecList;
  }

  public String getDAGName() {
    return dagName;
  }

  public String getVertexName() {
    return vertexName;
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return taskAttemptId;
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
  
  public List<GroupInputSpec> getGroupInputs() {
    return groupInputSpecList;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskAttemptId.write(out);
    out.writeUTF(dagName);
    out.writeUTF(vertexName);
    processorDescriptor.write(out);
    out.writeInt(inputSpecList.size());
    for (InputSpec inputSpec : inputSpecList) {
      inputSpec.write(out);
    }
    out.writeInt(outputSpecList.size());
    for (OutputSpec outputSpec : outputSpecList) {
      outputSpec.write(out);
    }
    if (groupInputSpecList != null && !groupInputSpecList.isEmpty()) {
      out.writeBoolean(true);
      out.writeInt(groupInputSpecList.size());
      for (GroupInputSpec group : groupInputSpecList) {
        group.write(out);
      }
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskAttemptId = TezTaskAttemptID.readTezTaskAttemptID(in);
    dagName = StringInterner.weakIntern(in.readUTF());
    vertexName = StringInterner.weakIntern(in.readUTF());
    // TODO TEZ-305 convert this to PB
    processorDescriptor = new ProcessorDescriptor();
    processorDescriptor.readFields(in);
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
    boolean hasGroupInputs = in.readBoolean();
    if (hasGroupInputs) {
      int numGroups = in.readInt();
      groupInputSpecList = Lists.newArrayListWithCapacity(numGroups);
      for (int i=0; i<numGroups; ++i) {
        GroupInputSpec group = new GroupInputSpec();
        group.readFields(in);
        groupInputSpecList.add(group);
      }
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("DAGName : " + dagName);
    sb.append(", VertexName: " + vertexName);
    sb.append(", TaskAttemptID:" + taskAttemptId);
    sb.append(", processorName=" + processorDescriptor.getClassName()
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
    if (groupInputSpecList != null && !groupInputSpecList.isEmpty()) {
      sb.append(" groupInputSpecList=[");
      for (GroupInputSpec group : groupInputSpecList) {
        sb.append("{" + group.toString() + "}, ");
      }
      sb.append("]");
    }
    return sb.toString();
  }

}
