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
package org.apache.tez.runtime.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;


public class TestTezTaskRunner2 {

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testTaskConfUsage() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("global", "global1");
    conf.set("global_override", "global1");
    String[] localDirs = null;
    Configuration taskConf = new Configuration(false);
    conf.set("global_override", "task1");
    conf.set("task", "task1");

    List<InputSpec> inputSpecList = new ArrayList<>();
    List<OutputSpec> outputSpecList = new ArrayList<>();
    TaskSpec taskSpec =
        new TaskSpec(
            TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(TezVertexID
                    .getInstance(TezDAGID.getInstance(ApplicationId.fromString("application_1_1"), 0), 0), 0), 0),
            "dagName", "vertexName", 1, mock(ProcessorDescriptor.class), inputSpecList,
            outputSpecList, null, taskConf);
    TezExecutors sharedExecutor = new TezSharedExecutor(conf);
    TezTaskRunner2 taskRunner2 = new TezTaskRunner2(conf, mock(UserGroupInformation.class),
        localDirs, taskSpec, 1, null, null, null, mock(TaskReporter.class), null, null, "pid",
        null, 1000, false, new DefaultHadoopShim(), sharedExecutor);

    assertEquals("global1", taskRunner2.task.getTaskConf().get("global"));
    assertEquals("task1", taskRunner2.task.getTaskConf().get("global_override"));
    assertEquals("task1", taskRunner2.task.getTaskConf().get("task"));
    sharedExecutor.shutdownNow();
  }


}
