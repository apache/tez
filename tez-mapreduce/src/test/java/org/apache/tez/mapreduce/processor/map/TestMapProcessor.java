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
package org.apache.tez.mapreduce.processor.map;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.tez.common.MRFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.TestUmbilical;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestMapProcessor {
  
  private static final Log LOG = LogFactory.getLog(TestMapProcessor.class);  
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  private static Path workDir = null;
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      workDir =
          new Path(new Path(System.getProperty("test.build.data", "/tmp")),
                   "TestMapProcessor").makeQualified(localFs);
      LOG.info("Using workDir: " + workDir);
      MapUtils.configureLocalDirs(defaultConf, workDir.toString());
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  


  

  public void setUpJobConf(JobConf job) {
    job.set(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workDir.toString());
    job.set(MRConfig.LOCAL_DIR, workDir.toString());
    job.setClass(
        Constants.TEZ_RUNTIME_TASK_OUTPUT_MANAGER,
        TezTaskOutputFiles.class,
        TezTaskOutput.class);
    job.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, MRPartitioner.class.getName());
    job.setNumReduceTasks(1);
  }

  @Before
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }
  
  @Test(timeout = 5000)
  public void testMapProcessor() throws Exception {
    String dagName = "mrdag0";
    String vertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    JobConf jobConf = new JobConf(defaultConf);
    setUpJobConf(jobConf);

    MRHelpers.translateMRConfToTez(jobConf);
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);

    jobConf.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, false);

    jobConf.set(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    
    Path mapInput = new Path(workDir, "map0");
    
    
    MapUtils.generateInputSplit(localFs, workDir, jobConf, mapInput);

    InputSpec mapInputSpec = new InputSpec("NullSrcVertex",
        InputDescriptor.create(MRInputLegacy.class.getName())
            .setUserPayload(UserPayload.create(ByteBuffer.wrap(
                MRRuntimeProtos.MRInputUserPayloadProto.newBuilder()
                    .setConfigurationBytes(TezUtils.createByteStringFromConf(jobConf)).build()
                    .toByteArray()))),
        1);
    OutputSpec mapOutputSpec = new OutputSpec("NullDestVertex", 
        OutputDescriptor.create(OrderedPartitionedKVOutput.class.getName())
            .setUserPayload(TezUtils.createUserPayloadFromConf(jobConf)), 1);

    LogicalIOProcessorRuntimeTask task = MapUtils.createLogicalTask(localFs, workDir, jobConf, 0,
        new Path(workDir, "map0"), new TestUmbilical(), dagName, vertexName,
        Collections.singletonList(mapInputSpec),
        Collections.singletonList(mapOutputSpec));
    
    task.initialize();
    task.run();
    task.close();
    
    OutputContext outputContext = task.getOutputContexts().iterator().next();
    TezTaskOutput mapOutputs = new TezTaskOutputFiles(jobConf, outputContext.getUniqueIdentifier());
    
    
    // TODO NEWTEZ FIXME OutputCommitter verification
//    MRTask mrTask = (MRTask)t.getProcessor();
//    Assert.assertEquals(TezNullOutputCommitter.class.getName(), mrTask
//        .getCommitter().getClass().getName());
//    t.close();

    Path mapOutputFile = mapOutputs.getOutputFile();
    LOG.info("mapOutputFile = " + mapOutputFile);
    IFile.Reader reader =
        new IFile.Reader(localFs, mapOutputFile, null, null, null, false, 0, -1);
    LongWritable key = new LongWritable();
    Text value = new Text();
    DataInputBuffer keyBuf = new DataInputBuffer();
    DataInputBuffer valueBuf = new DataInputBuffer();
    long prev = Long.MIN_VALUE;
    while (reader.nextRawKey(keyBuf)) {
      reader.nextRawValue(valueBuf);
      key.readFields(keyBuf);
      value.readFields(valueBuf);
      if (prev != Long.MIN_VALUE) {
        assert(prev <= key.get());
        prev = key.get();
      }
      LOG.info("key = " + key.get() + "; value = " + value);
    }
    reader.close();
  }
}
