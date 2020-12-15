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
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
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
  private int vertexParallelism = -1;
  private Configuration taskConf;

  public TaskSpec() {
  }
  
  public static TaskSpec createBaseTaskSpec(String dagName, String vertexName,
      int vertexParallelism, ProcessorDescriptor processorDescriptor,
      List<InputSpec> inputSpecList, List<OutputSpec> outputSpecList,
      @Nullable List<GroupInputSpec> groupInputSpecList, Configuration taskConf) {
    return new TaskSpec(dagName, vertexName, vertexParallelism, processorDescriptor, inputSpecList,
        outputSpecList, groupInputSpecList, taskConf);
  }

  public TaskSpec(
      String dagName, String vertexName,
      int vertexParallelism,
      ProcessorDescriptor processorDescriptor,
      List<InputSpec> inputSpecList, List<OutputSpec> outputSpecList,
      @Nullable List<GroupInputSpec> groupInputSpecList) {
    this(dagName, vertexName, vertexParallelism, processorDescriptor, inputSpecList,
        outputSpecList, groupInputSpecList, null);
  }

  public TaskSpec(
      String dagName, String vertexName,
      int vertexParallelism,
      ProcessorDescriptor processorDescriptor,
      List<InputSpec> inputSpecList, List<OutputSpec> outputSpecList,
      @Nullable List<GroupInputSpec> groupInputSpecList, Configuration taskConf) {
    Objects.requireNonNull(dagName, "dagName is null");
    Objects.requireNonNull(vertexName, "vertexName is null");
    Objects.requireNonNull(processorDescriptor, "processorDescriptor is null");
    Objects.requireNonNull(inputSpecList, "inputSpecList is null");
    Objects.requireNonNull(outputSpecList, "outputSpecList is null");
    this.taskAttemptId = null;
    this.dagName = StringInterner.weakIntern(dagName);
    this.vertexName = StringInterner.weakIntern(vertexName);
    this.processorDescriptor = processorDescriptor;
    this.inputSpecList = inputSpecList;
    this.outputSpecList = outputSpecList;
    this.groupInputSpecList = groupInputSpecList;
    this.vertexParallelism = vertexParallelism;
    this.taskConf = taskConf;
  }

  public TaskSpec(TezTaskAttemptID taskAttemptID,
                  String dagName, String vertexName,
                  int vertexParallelism,
                  ProcessorDescriptor processorDescriptor,
                  List<InputSpec> inputSpecList, List<OutputSpec> outputSpecList,
                  @Nullable List<GroupInputSpec> groupInputSpecList) {
    this(taskAttemptID, dagName, vertexName, vertexParallelism, processorDescriptor, inputSpecList,
        outputSpecList, groupInputSpecList, null);
  }

  public TaskSpec(TezTaskAttemptID taskAttemptID,
      String dagName, String vertexName,
      int vertexParallelism,
      ProcessorDescriptor processorDescriptor,
      List<InputSpec> inputSpecList, List<OutputSpec> outputSpecList,
      @Nullable List<GroupInputSpec> groupInputSpecList, Configuration taskConf) {
    Objects.requireNonNull(taskAttemptID, "taskAttemptID is null");
    Objects.requireNonNull(dagName, "dagName is null");
    Objects.requireNonNull(vertexName, "vertexName is null");
    Objects.requireNonNull(processorDescriptor, "processorDescriptor is null");
    Objects.requireNonNull(inputSpecList, "inputSpecList is null");
    Objects.requireNonNull(outputSpecList, "outputSpecList is null");
    this.taskAttemptId = taskAttemptID;
    this.dagName = StringInterner.weakIntern(dagName);
    this.vertexName = StringInterner.weakIntern(vertexName);
    this.processorDescriptor = processorDescriptor;
    this.inputSpecList = inputSpecList;
    this.outputSpecList = outputSpecList;
    this.groupInputSpecList = groupInputSpecList;
    this.vertexParallelism = vertexParallelism;
    this.taskConf = taskConf;
  }

  public String getDAGName() {
    return dagName;
  }

  public int getDagIdentifier() {
    return taskAttemptId.getTaskID().getVertexID().getDAGId().getId();
  }

  public int getVertexParallelism() {
    return vertexParallelism;
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

  public Configuration getTaskConf() {
    return taskConf;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskAttemptId.write(out);
    out.writeUTF(dagName);
    out.writeUTF(vertexName);
    out.writeInt(vertexParallelism);
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
    if (taskConf != null) {
      out.writeBoolean(true);
      taskConf.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskAttemptId = TezTaskAttemptID.readTezTaskAttemptID(in);
    dagName = StringInterner.weakIntern(in.readUTF());
    vertexName = StringInterner.weakIntern(in.readUTF());
    vertexParallelism = in.readInt();
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
    boolean hasVertexConf = in.readBoolean();
    if (hasVertexConf) {
      taskConf = new Configuration(false);
      taskConf.readFields(in);
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("DAGName : " + dagName);
    sb.append(", VertexName: " + vertexName);
    sb.append(", VertexParallelism: " + vertexParallelism);
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
    if (taskConf != null) {
      sb.append(", taskConfEntryCount=" + taskConf.size());
    }
    return sb.toString();
  }

}
