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

package org.apache.tez.runtime.task;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.junit.Assert;
import org.junit.Test;

public class TestTezTaskRunner {

  @Test (timeout = 5000)
  public void testTaskConfUsage() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("global", "global1");
    conf.set("global_override", "global1");
    String[] localDirs = null;
    Configuration taskConf = new Configuration(false);
    conf.set("global_override", "task1");
    conf.set("task", "task1");

    List<InputSpec> inputSpecList = new ArrayList<InputSpec>();
    List<OutputSpec> outputSpecList = new ArrayList<OutputSpec>();
    TaskSpec taskSpec = new TaskSpec("dagName", "vertexName", 1, mock(ProcessorDescriptor.class),
        inputSpecList, outputSpecList, null, taskConf);
    TezTaskRunner taskRunner = new TezTaskRunner(conf, mock(UserGroupInformation.class),
        localDirs, taskSpec, 1, null, null, null, mock(TaskReporter.class), null, null, "pid",
        null, 1000);

    Assert.assertEquals("global1", taskRunner.task.getTaskConf().get("global"));
    Assert.assertEquals("task1", taskRunner.task.getTaskConf().get("global_override"));
    Assert.assertEquals("task1", taskRunner.task.getTaskConf().get("task"));
  }


}
