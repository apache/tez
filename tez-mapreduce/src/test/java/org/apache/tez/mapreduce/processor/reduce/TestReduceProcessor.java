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
package org.apache.tez.mapreduce.processor.reduce;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.tez.common.Constants;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.TezEngineTask;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.engine.api.Task;
import org.apache.tez.engine.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
import org.apache.tez.engine.runtime.TezEngineFactory;
import org.apache.tez.mapreduce.TestUmbilicalProtocol;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.MRTaskType;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.apache.tez.mapreduce.task.InitialTaskWithLocalSort;
import org.apache.tez.mapreduce.task.LocalFinalTask;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestReduceProcessor {
  
  private static final Log LOG = LogFactory.getLog(TestReduceProcessor.class);
  
  JobConf job;
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  private static Path workDir =
    new Path(new Path(System.getProperty("test.build.data", "/tmp")),
             "TestReduceProcessor").makeQualified(localFs);

  @Before
  public void setUp() {
    job = new JobConf(defaultConf);
    job.set(TezJobConfig.LOCAL_DIR, workDir.toString());
    job.setClass(
        Constants.TEZ_ENGINE_TASK_OUTPUT_MANAGER,
        TezLocalTaskOutputFiles.class, 
        TezTaskOutput.class);
    job.setNumReduceTasks(1);
  }
  
  @Test
  public void testReduceProcessor() throws Exception {
    localFs.delete(workDir, true);

    // Run a map
    MapUtils.runMapProcessor(
        localFs, workDir, job, 0, new Path(workDir, "map0"), 
        new InitialTaskWithLocalSort(), new TestUmbilicalProtocol()
        );

    LOG.info("Starting reduce...");
    FileOutputFormat.setOutputPath(job, new Path(workDir, "output"));
    
    // Now run a reduce
    TezEngineTask taskContext = new TezEngineTask(
        TezTestUtils.getMockTaskAttemptId(0, 0, 0, MRTaskType.REDUCE), "tez",
        "tez", "TODO_vertexName", LocalFinalTask.class.getName(),
        Collections.singletonList(new InputSpec("TODO_srcVertexName", 1)), null);
    job.set(JobContext.TASK_ATTEMPT_ID, taskContext.getTaskAttemptId().toString());
    Injector injector = Guice.createInjector(new LocalFinalTask());
    TezEngineFactory factory = injector.getInstance(TezEngineFactory.class);
    Task t = factory.createTask(taskContext);
    t.initialize(job, new TestUmbilicalProtocol());
    t.run();
    t.close();

  }

}
