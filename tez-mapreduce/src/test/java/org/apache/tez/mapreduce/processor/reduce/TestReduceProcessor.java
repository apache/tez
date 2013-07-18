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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.tez.common.Constants;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.engine.api.Task;
import org.apache.tez.engine.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
import org.apache.tez.engine.lib.input.LocalMergedInput;
import org.apache.tez.engine.lib.output.LocalOnFileSorterOutput;
import org.apache.tez.engine.runtime.RuntimeUtils;
import org.apache.tez.mapreduce.TestUmbilicalProtocol;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.IDConverter;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.output.SimpleOutput;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


@SuppressWarnings("deprecation")
public class TestReduceProcessor {
  
  private static final Log LOG = LogFactory.getLog(TestReduceProcessor.class);

  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null;
  private static Path workDir = null;
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      workDir =
          new Path(new Path(System.getProperty("test.build.data", "/tmp")),
                   "TestReduceProcessor").makeQualified(localFs);
      
      MapUtils.configureLocalDirs(defaultConf, workDir.toString());
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  public void setUpJobConf(JobConf job) {
    job.set(TezJobConfig.LOCAL_DIRS, workDir.toString());
    job.setClass(
        Constants.TEZ_ENGINE_TASK_OUTPUT_MANAGER,
        TezLocalTaskOutputFiles.class, 
        TezTaskOutput.class);
    job.setNumReduceTasks(1);
  }

  @Before
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test
  public void testReduceProcessor() throws Exception {

    String mapVertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    String reduceVertexName = MultiStageMRConfigUtil.getFinalReduceVertexName();
    JobConf jobConf = new JobConf(defaultConf);
    setUpJobConf(jobConf);
    TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles();
    mapOutputs.setConf(jobConf);
    
    Configuration conf = MultiStageMRConfToTezTranslator.convertMRToLinearTez(jobConf);
    conf.setInt(TezJobConfig.APPLICATION_ATTEMPT_ID, 0);
    Configuration mapStageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        mapVertexName);
    
    JobConf mapConf = new JobConf(mapStageConf);
    
    mapConf.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    
    
    // Run a map
    MapUtils.runMapProcessor(localFs, workDir, mapConf, 0,
        new Path(workDir, "map0"), new TestUmbilicalProtocol(), mapVertexName,
        Collections.singletonList(new InputSpec("NullVertex", 0,
            SimpleInput.class.getName())),
        Collections.singletonList(new OutputSpec("FakeVertex", 1,
            LocalOnFileSorterOutput.class.getName())));

    LOG.info("Starting reduce...");
    
    
    Configuration reduceStageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        reduceVertexName);
    JobConf reduceConf = new JobConf(reduceStageConf);
    reduceConf.setOutputFormat(SequenceFileOutputFormat.class);
    reduceConf.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    FileOutputFormat.setOutputPath(reduceConf, new Path(workDir, "output"));
    ProcessorDescriptor reduceProcessorDesc = new ProcessorDescriptor(
        ReduceProcessor.class.getName(), null);
    // Now run a reduce
    TezEngineTaskContext taskContext = new TezEngineTaskContext(
        TezTestUtils.getMockTaskAttemptId(0, 1, 0, 0), "testUser",
        "testJob", reduceVertexName, reduceProcessorDesc,
        Collections.singletonList(new InputSpec(mapVertexName, 1,
            LocalMergedInput.class.getName())),
        Collections.singletonList(new OutputSpec("", 1,
                SimpleOutput.class.getName())));
    
    Task t = RuntimeUtils.createRuntimeTask(taskContext);
    t.initialize(reduceConf, null, new TestUmbilicalProtocol());
    t.run();
    t.close();
    
    // Can this be done via some utility class ? MapOutputFile derivative, or
    // instantiating the OutputCommitter
    Path reduceOutputDir = new Path(new Path(workDir, "output"),
        "_temporary/0/" + IDConverter
            .toMRTaskId(taskContext.getTaskAttemptId().getTaskID()));
    Path reduceOutputFile = new Path(reduceOutputDir, "part-00000");

    SequenceFile.Reader reader = new SequenceFile.Reader(localFs,
        reduceOutputFile, reduceConf);

    LongWritable key = new LongWritable();
    Text value = new Text();
    long prev = Long.MIN_VALUE;
    while (reader.next(key, value)) {
      if (prev != Long.MIN_VALUE) {
        Assert.assertTrue(prev < key.get());
        prev = key.get();
      }
    }

    reader.close();
  }

}
