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
import org.apache.tez.common.Constants;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.engine.api.Task;
import org.apache.tez.engine.common.sort.impl.IFile;
import org.apache.tez.engine.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
import org.apache.tez.engine.lib.output.InMemorySortedOutput;
import org.apache.tez.engine.lib.output.LocalOnFileSorterOutput;
import org.apache.tez.mapreduce.TestUmbilicalProtocol;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.TruncatedChannelBuffer;
import org.jboss.netty.handler.stream.ChunkedStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestMapProcessor {
  
  private static final Log LOG = LogFactory.getLog(TestMapProcessor.class);  
  
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
  @SuppressWarnings("deprecation")
  private static Path workDir =
    new Path(new Path(System.getProperty("test.build.data", "/tmp")),
             "TestMapProcessor").makeQualified(localFs);

  TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles();

  public void setUpJobConf(JobConf job) {
    job.set(TezJobConfig.LOCAL_DIR, workDir.toString());
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
  public void testMapProcessor() throws Exception {
    String vertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    JobConf jobConf = new JobConf(defaultConf);
    setUpJobConf(jobConf);
    TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles();
    mapOutputs.setConf(jobConf);
    
    Configuration conf = MultiStageMRConfToTezTranslator.convertMRToLinearTez(jobConf);
    Configuration stageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        vertexName);
    
    JobConf job = new JobConf(stageConf);
    
    job.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());

    MapUtils.runMapProcessor(localFs, workDir, job, 0,
        new Path(workDir, "map0"), new TestUmbilicalProtocol(), vertexName,
        Collections.singletonList(new InputSpec("NullVertex", 0,
            SimpleInput.class.getName())),
        Collections.singletonList(new OutputSpec("FakeVertex", 1,
            LocalOnFileSorterOutput.class.getName()))).close();

    Path mapOutputFile = mapOutputs.getInputFile(0);
    LOG.info("mapOutputFile = " + mapOutputFile);
    IFile.Reader reader =
        new IFile.Reader(job, localFs, mapOutputFile, null, null);
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

  @Test
  @Ignore
  public void testMapProcessorWithInMemSort() throws Exception {
    
    String vertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    
    final int partitions = 2;
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setNumReduceTasks(partitions);
    setUpJobConf(jobConf);
    TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles();
    mapOutputs.setConf(jobConf);
    
    Configuration conf = MultiStageMRConfToTezTranslator.convertMRToLinearTez(jobConf);
    Configuration stageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        vertexName);
    
    JobConf job = new JobConf(stageConf);

    job.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    localFs.delete(workDir, true);
    Task t =
        MapUtils.runMapProcessor(
            localFs, workDir, job, 0, new Path(workDir, "map0"), 
            new TestUmbilicalProtocol(true), vertexName, 
            Collections.singletonList(new InputSpec("NullVertex", 0,
                SimpleInput.class.getName())),
            Collections.singletonList(new OutputSpec("FakeVertex", 1,
                InMemorySortedOutput.class.getName()))
            );
    InMemorySortedOutput[] outputs = (InMemorySortedOutput[])t.getOutputs();
    
    verifyInMemSortedStream(outputs[0], 0, 4096);
    int i = 0;
    for (i = 2; i < 256; i <<= 1) {
      verifyInMemSortedStream(outputs[0], 0, i);
    }
    verifyInMemSortedStream(outputs[0], 1, 4096);
    for (i = 2; i < 256; i <<= 1) {
      verifyInMemSortedStream(outputs[0], 1, i);
    }

    t.close();
  }
  
  private void verifyInMemSortedStream(
      InMemorySortedOutput output, int partition, int chunkSize) 
          throws Exception {
    ChunkedStream cs = 
        new ChunkedStream(
            output.getSorter().getSortedStream(partition), chunkSize);
    int actualBytes = 0;
    ChannelBuffer b = null;
    while ((b = (ChannelBuffer)cs.nextChunk()) != null) {
      LOG.info("b = " + b);
      actualBytes += 
          (b instanceof TruncatedChannelBuffer) ? 
              ((TruncatedChannelBuffer)b).capacity() :
              ((BigEndianHeapChannelBuffer)b).readableBytes();
    }
    
    LOG.info("verifyInMemSortedStream" +
    		" partition=" + partition + 
    		" chunkSize=" + chunkSize +
        " expected=" + 
    		output.getSorter().getShuffleHeader(partition).getCompressedLength() + 
        " actual=" + actualBytes);
    Assert.assertEquals(
        output.getSorter().getShuffleHeader(partition).getCompressedLength(), 
        actualBytes);
  }
}
