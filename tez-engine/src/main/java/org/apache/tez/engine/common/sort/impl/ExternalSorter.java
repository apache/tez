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

package org.apache.tez.engine.common.sort.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.Constants;
import org.apache.tez.common.RunningTaskContext;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Partitioner;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.common.ConfigUtils;
import org.apache.tez.engine.common.combine.CombineInput;
import org.apache.tez.engine.common.combine.CombineOutput;
import org.apache.tez.engine.common.shuffle.impl.ShuffleHeader;
import org.apache.tez.engine.common.sort.impl.IFile.Writer;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
import org.apache.tez.engine.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.engine.records.OutputContext;
import org.apache.tez.engine.records.TezTaskAttemptID;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class ExternalSorter {

  private static final Log LOG = LogFactory.getLog(ExternalSorter.class);

  public abstract void close() throws IOException, InterruptedException;

  public abstract void flush() throws IOException, InterruptedException;

  public abstract void write(Object key, Object value) throws IOException,
      InterruptedException;

  protected Processor combineProcessor;
  protected Partitioner partitioner;
  protected TezEngineTaskContext task;
  protected RunningTaskContext runningTaskContext;
  protected Configuration job;
  protected FileSystem rfs;
  protected TezTaskOutput mapOutputFile;
  protected int partitions;
  protected Class keyClass;
  protected Class valClass;
  protected RawComparator comparator;
  protected SerializationFactory serializationFactory;
  protected Serializer keySerializer;
  protected Serializer valSerializer;
  
  protected IndexedSorter sorter;

  // Compression for map-outputs
  protected CompressionCodec codec;
  
  // Counters
  protected TezCounter mapOutputByteCounter;
  protected TezCounter mapOutputRecordCounter;
  protected TezCounter fileOutputByteCounter;
  protected TezCounter spilledRecordsCounter;
  protected Progress sortPhase;

  public void initialize(Configuration conf, Master master)
      throws IOException, InterruptedException {
    
    this.job = conf;
    LOG.info("TEZ_ENGINE_TASK_ATTEMPT_ID: " + 
        job.get(Constants.TEZ_ENGINE_TASK_ATTEMPT_ID));

    partitions = task.getOutputSpecList().get(0).getNumOutputs();
//    partitions = 
//        job.getInt(
//            TezJobConfig.TEZ_ENGINE_TASK_OUTDEGREE, 
//            TezJobConfig.DEFAULT_TEZ_ENGINE_TASK_OUTDEGREE);
    rfs = ((LocalFileSystem)FileSystem.getLocal(job)).getRaw();
    
    // sorter
    sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
          QuickSort.class, IndexedSorter.class), job);
    
    comparator = ConfigUtils.getOutputKeyComparator(job);
    
    // k/v serialization
    keyClass = ConfigUtils.getMapOutputKeyClass(job);
    valClass = ConfigUtils.getMapOutputValueClass(job);
    serializationFactory = new SerializationFactory(job);
    keySerializer = serializationFactory.getSerializer(keyClass);
    valSerializer = serializationFactory.getSerializer(valClass);
    
    //    counters
    mapOutputByteCounter = 
        runningTaskContext.getTaskReporter().getCounter(TaskCounter.MAP_OUTPUT_BYTES);
    mapOutputRecordCounter =
      runningTaskContext.getTaskReporter().getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    fileOutputByteCounter = 
        runningTaskContext.getTaskReporter().
            getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);
    spilledRecordsCounter = 
        runningTaskContext.getTaskReporter().getCounter(TaskCounter.SPILLED_RECORDS);
    // compression
    if (ConfigUtils.getCompressMapOutput(job)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getMapOutputCompressorClass(job, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    } else {
      codec = null;
    }

    // Task outputs 
    mapOutputFile =
        (TezTaskOutput) ReflectionUtils.newInstance(
            conf.getClass(
                Constants.TEZ_ENGINE_TASK_OUTPUT_MANAGER, 
                TezTaskOutputFiles.class), conf);
//    LOG.info("XXX mapOutputFile: " + mapOutputFile.getClass());
    
    // sortPhase
    sortPhase  = runningTaskContext.getProgress().addPhase("sort", 0.333f);
  }

  /**
   * Exception indicating that the allocated sort buffer is insufficient to hold
   * the current record.
   */
  @SuppressWarnings("serial")
  public static class MapBufferTooSmallException extends IOException {
    public MapBufferTooSmallException(String s) {
      super(s);
    }
  }

  public void setTask(RunningTaskContext task) {
    this.runningTaskContext = task;
    this.combineProcessor = task.getCombineProcessor();
    this.partitioner = task.getPartitioner();
  }

  public TezTaskAttemptID getTaskAttemptId() {
    return task.getTaskAttemptId();
  }

  @Private
  public TezTaskOutput getMapOutput() {
    return mapOutputFile;
  }

  protected void runCombineProcessor(TezRawKeyValueIterator kvIter,
      Writer writer) throws IOException, InterruptedException {

    CombineInput combineIn = new CombineInput(kvIter);
    combineIn.initialize(job, runningTaskContext.getTaskReporter());

    CombineOutput combineOut = new CombineOutput(writer);
    combineOut.initialize(job, runningTaskContext.getTaskReporter());

    combineProcessor.process(new Input[] {combineIn},
        new Output[] {combineOut});

    combineIn.close();
    combineOut.close();

  }

  /**
   * Rename srcPath to dstPath on the same volume. This is the same as
   * RawLocalFileSystem's rename method, except that it will not fall back to a
   * copy, and it will create the target directory if it doesn't exist.
   */
  protected void sameVolRename(Path srcPath, Path dstPath) throws IOException {
    RawLocalFileSystem rfs = (RawLocalFileSystem) this.rfs;
    File src = rfs.pathToFile(srcPath);
    File dst = rfs.pathToFile(dstPath);
    if (!dst.getParentFile().exists()) {
      if (!dst.getParentFile().mkdirs()) {
        throw new IOException("Unable to rename " + src + " to " + dst
            + ": couldn't create parent directory");
      }
    }

    if (!src.renameTo(dst)) {
      throw new IOException("Unable to rename " + src + " to " + dst);
    }
//    LOG.info("XXX sameVolRename src=" + src + ", dst=" + dst);
  }

//  public ExternalSorter() {
//    super();
//  }
  
  public ExternalSorter(TezEngineTaskContext tezEngineTask) {
    this.task = tezEngineTask;
  }

  public InputStream getSortedStream(int partition) {
    throw new UnsupportedOperationException("getSortedStream isn't supported!");
  }

  public ShuffleHeader getShuffleHeader(int reduce) {
    throw new UnsupportedOperationException("getShuffleHeader isn't supported!");
  }

  public OutputContext getOutputContext() {
    return null;
  }
}
