/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.api.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestTaskSpec {

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSerDe() throws IOException {
    ByteBuffer payload = null;
    ProcessorDescriptor procDesc = ProcessorDescriptor.create("proc").setUserPayload(
        UserPayload.create(payload)).setHistoryText("historyText");

    List<InputSpec> inputSpecs = new ArrayList<>();
    InputSpec inputSpec = new InputSpec("src1", InputDescriptor.create("inputClass"),10);
    inputSpecs.add(inputSpec);
    List<OutputSpec> outputSpecs = new ArrayList<>();
    OutputSpec outputSpec = new OutputSpec("dest1", OutputDescriptor.create("outputClass"), 999);
    outputSpecs.add(outputSpec);
    List<GroupInputSpec> groupInputSpecs = null;

    Configuration taskConf = new Configuration(false);
    taskConf.set("foo", "bar");

    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1234", 1, 1), 1), 1), 1);
    TaskSpec taskSpec = new TaskSpec(taId, "dagName", "vName", -1, procDesc, inputSpecs, outputSpecs,
        groupInputSpecs, taskConf);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(bos);
    taskSpec.write(out);

    TaskSpec deSerTaskSpec = new TaskSpec();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInput in = new DataInputStream(bis);
    deSerTaskSpec.readFields(in);

    assertEquals(taskSpec.getDAGName(), deSerTaskSpec.getDAGName());
    assertEquals(taskSpec.getVertexName(), deSerTaskSpec.getVertexName());
    assertEquals(taskSpec.getVertexParallelism(), deSerTaskSpec.getVertexParallelism());
    assertEquals(taskSpec.getInputs().size(), deSerTaskSpec.getInputs().size());
    assertEquals(taskSpec.getOutputs().size(), deSerTaskSpec.getOutputs().size());
    assertNull(deSerTaskSpec.getGroupInputs());
    assertEquals(taskSpec.getInputs().get(0).getSourceVertexName(),
        deSerTaskSpec.getInputs().get(0).getSourceVertexName());
    assertEquals(taskSpec.getOutputs().get(0).getDestinationVertexName(),
        deSerTaskSpec.getOutputs().get(0).getDestinationVertexName());

    assertEquals(taskConf.get("foo"), deSerTaskSpec.getTaskConf().get("foo"));
  }

}
