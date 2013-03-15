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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.api.Task;
import org.apache.tez.common.Constants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.engine.common.sort.impl.IFile;
import org.apache.tez.engine.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
import org.apache.tez.engine.lib.output.InMemorySortedOutput;
import org.apache.tez.mapreduce.TestUmbilicalProtocol;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.apache.tez.mapreduce.task.InitialTaskWithInMemSort;
import org.apache.tez.mapreduce.task.InitialTaskWithLocalSort;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.TruncatedChannelBuffer;
import org.jboss.netty.handler.stream.ChunkedStream;
import org.junit.Before;
import org.junit.Test;


public class TestMapProcessor {
  
  private static final Log LOG = LogFactory.getLog(TestMapProcessor.class);
  
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
             "TestMapProcessor").makeQualified(localFs);

  TezTaskOutput mapOutputs = new TezLocalTaskOutputFiles();
  
  @Before
  public void setUp() {
    job = new JobConf(defaultConf);
    job.set(TezJobConfig.LOCAL_DIR, workDir.toString());
    job.setClass(
        Constants.TEZ_ENGINE_TASK_OUTPUT_MANAGER,
        TezLocalTaskOutputFiles.class, 
        TezTaskOutput.class);
    job.setNumReduceTasks(1);
    mapOutputs.setConf(job);
  }
  
  @Test
  public void testMapProcessor() throws Exception {
    localFs.delete(workDir, true);
    MapUtils.runMapProcessor(
        localFs, workDir, job, 0, new Path(workDir, "map0"), 
        new InitialTaskWithLocalSort(), new TestUmbilicalProtocol()).close();

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

  //@Test (timeout=20000l)
  public void testMapProcessorWithInMemSort() throws Exception {
    final int partitions = 2;
    job.setNumReduceTasks(partitions);
    job.setInt(TezJobConfig.TEZ_ENGINE_TASK_OUTDEGREE, partitions); 

    localFs.delete(workDir, true);
    Task t =
        MapUtils.runMapProcessor(
            localFs, workDir, job, 0, new Path(workDir, "map0"), 
        new InitialTaskWithInMemSort(), new TestUmbilicalProtocol(true));
    InMemorySortedOutput output = (InMemorySortedOutput)t.getOutput();
    
    verifyInMemSortedStream(output, 0, 4096);
    int i = 0;
    for (i = 2; i < 256; i <<= 1) {
      verifyInMemSortedStream(output, 0, i);
    }
    verifyInMemSortedStream(output, 1, 4096);
    for (i = 2; i < 256; i <<= 1) {
      verifyInMemSortedStream(output, 1, i);
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
