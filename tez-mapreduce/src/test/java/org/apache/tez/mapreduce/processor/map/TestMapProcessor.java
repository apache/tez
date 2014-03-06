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
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.mapreduce.TestUmbilical;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.output.LocalOnFileSorterOutput;
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
    job.set(TezJobConfig.LOCAL_DIRS, workDir.toString());
    job.set(MRConfig.LOCAL_DIR, workDir.toString());
    job.setClass(
        Constants.TEZ_RUNTIME_TASK_OUTPUT_MANAGER,
        TezLocalTaskOutputFiles.class, 
        TezTaskOutput.class);
    job.set(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS, MRPartitioner.class.getName());
    job.setNumReduceTasks(1);
  }

  @Before
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }
  
  @Test
  public void testMapProcessor() throws Exception {
    String dagName = "mrdag0";
    String vertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    JobConf jobConf = new JobConf(defaultConf);
    setUpJobConf(jobConf);

    Configuration conf = MultiStageMRConfToTezTranslator.convertMRToLinearTez(jobConf);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);

    Configuration stageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        vertexName);
    
    JobConf job = new JobConf(stageConf);
    job.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, false);

    job.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    
    Path mapInput = new Path(workDir, "map0");
    
    
    MapUtils.generateInputSplit(localFs, workDir, job, mapInput);
    
    InputSpec mapInputSpec = new InputSpec("NullSrcVertex",
        new InputDescriptor(MRInputLegacy.class.getName())
            .setUserPayload(MRHelpers.createMRInputPayload(job, null)),
        0);
    OutputSpec mapOutputSpec = new OutputSpec("NullDestVertex", new OutputDescriptor(LocalOnFileSorterOutput.class.getName()), 1);

    LogicalIOProcessorRuntimeTask task = MapUtils.createLogicalTask(localFs, workDir, job, 0,
        new Path(workDir, "map0"), new TestUmbilical(), dagName, vertexName,
        Collections.singletonList(mapInputSpec),
        Collections.singletonList(mapOutputSpec));
    
    task.initialize();
    task.run();
    task.close();
    
    TezInputContext inputContext = task.getInputContexts().iterator().next();
    TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles(jobConf, inputContext.getUniqueIdentifier());
    
    
    // TODO NEWTEZ FIXME OutputCommitter verification
//    MRTask mrTask = (MRTask)t.getProcessor();
//    Assert.assertEquals(TezNullOutputCommitter.class.getName(), mrTask
//        .getCommitter().getClass().getName());
//    t.close();

    Path mapOutputFile = mapOutputs.getInputFile(new InputAttemptIdentifier(0, 0));
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

//  @Test
//  @Ignore
//  public void testMapProcessorWithInMemSort() throws Exception {
//    
//    String vertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
//    
//    final int partitions = 2;
//    JobConf jobConf = new JobConf(defaultConf);
//    jobConf.setNumReduceTasks(partitions);
//    setUpJobConf(jobConf);
//    TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles();
//    mapOutputs.setConf(jobConf);
//    
//    Configuration conf = MultiStageMRConfToTezTranslator.convertMRToLinearTez(jobConf);
//    Configuration stageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
//        vertexName);
//    
//    JobConf job = new JobConf(stageConf);
//
//    job.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
//        "localized-resources").toUri().toString());
//    localFs.delete(workDir, true);
//    Task t =
//        MapUtils.runMapProcessor(
//            localFs, workDir, job, 0, new Path(workDir, "map0"), 
//            new TestUmbilicalProtocol(true), vertexName, 
//            Collections.singletonList(new InputSpec("NullVertex", 0,
//                MRInput.class.getName())),
//            Collections.singletonList(new OutputSpec("FakeVertex", 1,
//                OldInMemorySortedOutput.class.getName()))
//            );
//    OldInMemorySortedOutput[] outputs = (OldInMemorySortedOutput[])t.getOutputs();
//    
//    verifyInMemSortedStream(outputs[0], 0, 4096);
//    int i = 0;
//    for (i = 2; i < 256; i <<= 1) {
//      verifyInMemSortedStream(outputs[0], 0, i);
//    }
//    verifyInMemSortedStream(outputs[0], 1, 4096);
//    for (i = 2; i < 256; i <<= 1) {
//      verifyInMemSortedStream(outputs[0], 1, i);
//    }
//
//    t.close();
//  }
//  
//  private void verifyInMemSortedStream(
//      OldInMemorySortedOutput output, int partition, int chunkSize) 
//          throws Exception {
//    ChunkedStream cs = 
//        new ChunkedStream(
//            output.getSorter().getSortedStream(partition), chunkSize);
//    int actualBytes = 0;
//    ChannelBuffer b = null;
//    while ((b = (ChannelBuffer)cs.nextChunk()) != null) {
//      LOG.info("b = " + b);
//      actualBytes += 
//          (b instanceof TruncatedChannelBuffer) ? 
//              ((TruncatedChannelBuffer)b).capacity() :
//              ((BigEndianHeapChannelBuffer)b).readableBytes();
//    }
//    
//    LOG.info("verifyInMemSortedStream" +
//    		" partition=" + partition + 
//    		" chunkSize=" + chunkSize +
//        " expected=" + 
//    		output.getSorter().getShuffleHeader(partition).getCompressedLength() + 
//        " actual=" + actualBytes);
//    Assert.assertEquals(
//        output.getSorter().getShuffleHeader(partition).getCompressedLength(), 
//        actualBytes);
//  }
}
