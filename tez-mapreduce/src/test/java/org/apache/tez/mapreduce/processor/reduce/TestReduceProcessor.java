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
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.engine.api.impl.InputSpec;
import org.apache.tez.engine.api.impl.OutputSpec;
import org.apache.tez.engine.api.impl.TaskSpec;
import org.apache.tez.engine.api.impl.TezUmbilical;
import org.apache.tez.engine.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
import org.apache.tez.engine.lib.input.LocalMergedInput;
import org.apache.tez.engine.lib.output.LocalOnFileSorterOutput;
import org.apache.tez.engine.newruntime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.IDConverter;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.SimpleInputLegacy;
import org.apache.tez.mapreduce.output.SimpleOutput;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
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
    TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles(jobConf, "TODONEWTEZ_uniqueId");
    
    Configuration conf = MultiStageMRConfToTezTranslator.convertMRToLinearTez(jobConf);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
    Configuration mapStageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        mapVertexName);
    
    JobConf mapConf = new JobConf(mapStageConf);
    
    mapConf.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    
    InputSpec mapInputSpec = new InputSpec("NullSrcVertex", new InputDescriptor(
        SimpleInputLegacy.class.getName()), 0);
    OutputSpec mapOutputSpec = new OutputSpec("NullDestVertex", new OutputDescriptor(
        LocalOnFileSorterOutput.class.getName()), 1);
    // Run a map
    // TODO NEWTEZ FIX Umbilical creation
    MapUtils.runMapProcessor(localFs, workDir, mapConf, 0,
        new Path(workDir, "map0"), (TezUmbilical) null, mapVertexName,
        Collections.singletonList(mapInputSpec),
        Collections.singletonList(mapOutputSpec));

    LOG.info("Starting reduce...");
    
    
    Configuration reduceStageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        reduceVertexName);
    JobConf reduceConf = new JobConf(reduceStageConf);
    reduceConf.setOutputFormat(SequenceFileOutputFormat.class);
    reduceConf.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    FileOutputFormat.setOutputPath(reduceConf, new Path(workDir, "output"));
    ProcessorDescriptor reduceProcessorDesc = new ProcessorDescriptor(
        ReduceProcessor.class.getName());
    
    InputSpec reduceInputSpec = new InputSpec(mapVertexName, new InputDescriptor(LocalMergedInput.class.getName()), 1);
    OutputSpec reduceOutputSpec = new OutputSpec("NullDestinationVertex", new OutputDescriptor(SimpleOutput.class.getName()), 1);
    
    // Now run a reduce
    TaskSpec taskSpec = new TaskSpec(
        TezTestUtils.getMockTaskAttemptId(0, 1, 0, 0),
        "testUser",
        reduceVertexName,
        reduceProcessorDesc,
        Collections.singletonList(reduceInputSpec),
        Collections.singletonList(reduceOutputSpec));
    
    // TODO NEWTEZ FIXME Umbilical and jobToken
    LogicalIOProcessorRuntimeTask task = new LogicalIOProcessorRuntimeTask(
        taskSpec,
        1,
        reduceConf,
        (TezUmbilical) null,
        null);
    
    task.initialize();
    task.run();
    
//    MRTask mrTask = (MRTask)t.getProcessor();
//    TODO NEWTEZ Verify the partitioner has been created
//    Assert.assertNull(mrTask.getPartitioner());
    task.close();
    
    // Can this be done via some utility class ? MapOutputFile derivative, or
    // instantiating the OutputCommitter
    

    // TODO NEWTEZ FIXME uniqueId generation and event generation (mockTaskId will not work here)
    Path reduceOutputDir = new Path(new Path(workDir, "output"),
        "_temporary/0/" + IDConverter
            .toMRTaskId(TezTestUtils.getMockTaskId(0, 1, 0)));
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
