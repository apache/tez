/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.GroupInputSpecProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.IOSpecProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.TaskSpecProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.TaskSpecProto.Builder;

public class ProtoConverters {

  public static TaskSpec getTaskSpecfromProto(TaskSpecProto taskSpecProto) {
    TezTaskAttemptID taskAttemptID =
        TezTaskAttemptID.fromString(taskSpecProto.getTaskAttemptIdString());

    ProcessorDescriptor processorDescriptor = null;
    if (taskSpecProto.hasProcessorDescriptor()) {
      processorDescriptor = DagTypeConverters
          .convertProcessorDescriptorFromDAGPlan(taskSpecProto.getProcessorDescriptor());
    }

    List<InputSpec> inputSpecList = new ArrayList<InputSpec>(taskSpecProto.getInputSpecsCount());
    if (taskSpecProto.getInputSpecsCount() > 0) {
      for (IOSpecProto inputSpecProto : taskSpecProto.getInputSpecsList()) {
        inputSpecList.add(getInputSpecFromProto(inputSpecProto));
      }
    }

    List<OutputSpec> outputSpecList =
        new ArrayList<OutputSpec>(taskSpecProto.getOutputSpecsCount());
    if (taskSpecProto.getOutputSpecsCount() > 0) {
      for (IOSpecProto outputSpecProto : taskSpecProto.getOutputSpecsList()) {
        outputSpecList.add(getOutputSpecFromProto(outputSpecProto));
      }
    }

    List<GroupInputSpec> groupInputSpecs =
        new ArrayList<GroupInputSpec>(taskSpecProto.getGroupedInputSpecsCount());
    if (taskSpecProto.getGroupedInputSpecsCount() > 0) {
      for (GroupInputSpecProto groupInputSpecProto : taskSpecProto.getGroupedInputSpecsList()) {
        groupInputSpecs.add(getGroupInputSpecFromProto(groupInputSpecProto));
      }
    }

    Configuration taskConf = null;
    if (taskSpecProto.hasTaskConf()) {
      taskConf = new Configuration(false);
      Map<String, String> confMap =
          DagTypeConverters.convertConfFromProto(taskSpecProto.getTaskConf());
      for (Entry<String, String> e : confMap.entrySet()) {
        taskConf.set(e.getKey(), e.getValue());
      }
    }
    TaskSpec taskSpec =
        new TaskSpec(taskAttemptID, taskSpecProto.getDagName(), taskSpecProto.getVertexName(),
            taskSpecProto.getVertexParallelism(), processorDescriptor, inputSpecList,
            outputSpecList, groupInputSpecs, taskConf);
    return taskSpec;
  }

  public static TaskSpecProto convertTaskSpecToProto(TaskSpec taskSpec) {
    Builder builder = TaskSpecProto.newBuilder();
    builder.setTaskAttemptIdString(taskSpec.getTaskAttemptID().toString());
    builder.setDagName(taskSpec.getDAGName());
    builder.setVertexName(taskSpec.getVertexName());
    builder.setVertexParallelism(taskSpec.getVertexParallelism());

    if (taskSpec.getProcessorDescriptor() != null) {
      builder.setProcessorDescriptor(
          DagTypeConverters.convertToDAGPlan(taskSpec.getProcessorDescriptor()));
    }

    if (taskSpec.getInputs() != null && !taskSpec.getInputs().isEmpty()) {
      for (InputSpec inputSpec : taskSpec.getInputs()) {
        builder.addInputSpecs(convertInputSpecToProto(inputSpec));
      }
    }

    if (taskSpec.getOutputs() != null && !taskSpec.getOutputs().isEmpty()) {
      for (OutputSpec outputSpec : taskSpec.getOutputs()) {
        builder.addOutputSpecs(convertOutputSpecToProto(outputSpec));
      }
    }

    if (taskSpec.getGroupInputs() != null && !taskSpec.getGroupInputs().isEmpty()) {
      for (GroupInputSpec groupInputSpec : taskSpec.getGroupInputs()) {
        builder.addGroupedInputSpecs(convertGroupInputSpecToProto(groupInputSpec));

      }
    }
    if (taskSpec.getTaskConf() != null) {
      ConfigurationProto.Builder confBuilder = ConfigurationProto.newBuilder();
      Iterator<Entry<String, String>> iter = taskSpec.getTaskConf().iterator();
      while (iter.hasNext()) {
        Entry<String, String> entry = iter.next();
        confBuilder.addConfKeyValues(PlanKeyValuePair.newBuilder()
            .setKey(entry.getKey())
            .setValue(entry.getValue()).build());
      }
      builder.setTaskConf(confBuilder.build());
    }
    return builder.build();
  }


  public static InputSpec getInputSpecFromProto(IOSpecProto inputSpecProto) {
    InputDescriptor inputDescriptor = null;
    if (inputSpecProto.hasIoDescriptor()) {
      inputDescriptor =
          DagTypeConverters.convertInputDescriptorFromDAGPlan(inputSpecProto.getIoDescriptor());
    }
    InputSpec inputSpec = new InputSpec(inputSpecProto.getConnectedVertexName(), inputDescriptor,
        inputSpecProto.getPhysicalEdgeCount());
    return inputSpec;
  }

  public static IOSpecProto convertInputSpecToProto(InputSpec inputSpec) {
    IOSpecProto.Builder builder = IOSpecProto.newBuilder();
    if (inputSpec.getSourceVertexName() != null) {
      builder.setConnectedVertexName(inputSpec.getSourceVertexName());
    }
    if (inputSpec.getInputDescriptor() != null) {
      builder.setIoDescriptor(DagTypeConverters.convertToDAGPlan(inputSpec.getInputDescriptor()));
    }
    builder.setPhysicalEdgeCount(inputSpec.getPhysicalEdgeCount());
    return builder.build();
  }

  public static OutputSpec getOutputSpecFromProto(IOSpecProto outputSpecProto) {
    OutputDescriptor outputDescriptor = null;
    if (outputSpecProto.hasIoDescriptor()) {
      outputDescriptor =
          DagTypeConverters.convertOutputDescriptorFromDAGPlan(outputSpecProto.getIoDescriptor());
    }
    OutputSpec outputSpec =
        new OutputSpec(outputSpecProto.getConnectedVertexName(), outputDescriptor,
            outputSpecProto.getPhysicalEdgeCount());
    return outputSpec;
  }

  public static IOSpecProto convertOutputSpecToProto(OutputSpec outputSpec) {
    IOSpecProto.Builder builder = IOSpecProto.newBuilder();
    if (outputSpec.getDestinationVertexName() != null) {
      builder.setConnectedVertexName(outputSpec.getDestinationVertexName());
    }
    if (outputSpec.getOutputDescriptor() != null) {
      builder.setIoDescriptor(DagTypeConverters.convertToDAGPlan(outputSpec.getOutputDescriptor()));
    }
    builder.setPhysicalEdgeCount(outputSpec.getPhysicalEdgeCount());
    return builder.build();
  }

  public static GroupInputSpec getGroupInputSpecFromProto(GroupInputSpecProto groupInputSpecProto) {
    GroupInputSpec groupSpec = new GroupInputSpec(groupInputSpecProto.getGroupName(),
        groupInputSpecProto.getGroupVerticesList(), DagTypeConverters
        .convertInputDescriptorFromDAGPlan(groupInputSpecProto.getMergedInputDescriptor()));
    return groupSpec;
  }

  public static GroupInputSpecProto convertGroupInputSpecToProto(GroupInputSpec groupInputSpec) {
    GroupInputSpecProto.Builder builder = GroupInputSpecProto.newBuilder();
    builder.setGroupName(groupInputSpec.getGroupName());
    builder.addAllGroupVertices(groupInputSpec.getGroupVertices());
    builder.setMergedInputDescriptor(
        DagTypeConverters.convertToDAGPlan(groupInputSpec.getMergedInputDescriptor()));
    return builder.build();
  }

}
