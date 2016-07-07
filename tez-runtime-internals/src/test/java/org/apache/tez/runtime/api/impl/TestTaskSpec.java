/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.api.impl;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskSpec {

  @Test (timeout = 5000)
  public void testSerDe() throws IOException {
    ByteBuffer payload = null;
    ProcessorDescriptor procDesc = ProcessorDescriptor.create("proc").setUserPayload(
        UserPayload.create(payload)).setHistoryText("historyText");

    List<InputSpec> inputSpecs = new ArrayList<InputSpec>();
    InputSpec inputSpec = new InputSpec("src1", InputDescriptor.create("inputClass"),10);
    inputSpecs.add(inputSpec);
    List<OutputSpec> outputSpecs = new ArrayList<OutputSpec>();
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

    Assert.assertEquals(taskSpec.getDAGName(), deSerTaskSpec.getDAGName());
    Assert.assertEquals(taskSpec.getVertexName(), deSerTaskSpec.getVertexName());
    Assert.assertEquals(taskSpec.getVertexParallelism(), deSerTaskSpec.getVertexParallelism());
    Assert.assertEquals(taskSpec.getInputs().size(), deSerTaskSpec.getInputs().size());
    Assert.assertEquals(taskSpec.getOutputs().size(), deSerTaskSpec.getOutputs().size());
    Assert.assertNull(deSerTaskSpec.getGroupInputs());
    Assert.assertEquals(taskSpec.getInputs().get(0).getSourceVertexName(),
        deSerTaskSpec.getInputs().get(0).getSourceVertexName());
    Assert.assertEquals(taskSpec.getOutputs().get(0).getDestinationVertexName(),
        deSerTaskSpec.getOutputs().get(0).getDestinationVertexName());

    Assert.assertEquals(taskConf.get("foo"), deSerTaskSpec.getTaskConf().get("foo"));
  }

}
