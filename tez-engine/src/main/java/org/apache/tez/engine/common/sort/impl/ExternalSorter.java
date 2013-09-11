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
import java.lang.reflect.Constructor;

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
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.Constants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.engine.api.Partitioner;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.common.ConfigUtils;
import org.apache.tez.engine.common.shuffle.impl.ShuffleHeader;
import org.apache.tez.engine.common.task.local.newoutput.TezTaskOutput;
import org.apache.tez.engine.common.task.local.newoutput.TezTaskOutputFiles;
import org.apache.tez.engine.hadoop.compat.NullProgressable;
import org.apache.tez.engine.newapi.TezOutputContext;
import org.apache.tez.engine.records.OutputContext;
import org.apache.tez.engine.common.sort.impl.IFile.Writer;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class ExternalSorter {

  private static final Log LOG = LogFactory.getLog(ExternalSorter.class);

  public abstract void close() throws IOException;

  public abstract void flush() throws IOException;

  public abstract void write(Object key, Object value) throws IOException;

  protected Progressable nullProgressable = new NullProgressable();
  protected TezOutputContext outputContext;
  protected Processor combineProcessor;
  protected Partitioner partitioner;
  protected Configuration conf;
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

  // TODO NEWTEZ Setup CombineProcessor
  // TODO NEWTEZ Setup Partitioner in SimpleOutput

  // Counters
  // TODO TEZ Rename all counter variables [Mapping of counter to MR for compatibility in the MR layer]
  protected TezCounter mapOutputByteCounter;
  protected TezCounter mapOutputRecordCounter;
  protected TezCounter fileOutputByteCounter;
  protected TezCounter spilledRecordsCounter;

  public void initialize(TezOutputContext outputContext, Configuration conf, int numOutputs) throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    this.partitions = numOutputs;

    rfs = ((LocalFileSystem)FileSystem.getLocal(this.conf)).getRaw();

    // sorter
    sorter = ReflectionUtils.newInstance(this.conf.getClass(
        TezJobConfig.TEZ_ENGINE_INTERNAL_SORTER_CLASS, QuickSort.class,
        IndexedSorter.class), this.conf);

    comparator = ConfigUtils.getIntermediateOutputKeyComparator(this.conf);

    // k/v serialization
    keyClass = ConfigUtils.getIntermediateOutputKeyClass(this.conf);
    valClass = ConfigUtils.getIntermediateOutputValueClass(this.conf);
    serializationFactory = new SerializationFactory(this.conf);
    keySerializer = serializationFactory.getSerializer(keyClass);
    valSerializer = serializationFactory.getSerializer(valClass);

    //    counters
    mapOutputByteCounter =
        outputContext.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES);
    mapOutputRecordCounter =
        outputContext.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    fileOutputByteCounter =
        outputContext.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);
    spilledRecordsCounter =
        outputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    // compression
    if (ConfigUtils.shouldCompressIntermediateOutput(this.conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateOutputCompressorClass(this.conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, this.conf);
    } else {
      codec = null;
    }

    // Task outputs
    mapOutputFile = instantiateTaskOutputManager(this.conf, outputContext);
  }

  // TODO NEWTEZ Add an interface (! Processor) for CombineProcessor, which MR tasks can initialize and set.
  // Alternately add a config key with a classname, which is easy to initialize.
  public void setCombiner(Processor combineProcessor) {
    this.combineProcessor = combineProcessor;
  }
  
  // TODO NEWTEZ Setup a config value for the Partitioner class, from where it can be initialized.
  public void setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
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

  @Private
  public TezTaskOutput getMapOutput() {
    return mapOutputFile;
  }

  protected void runCombineProcessor(TezRawKeyValueIterator kvIter,
      Writer writer) throws IOException {

    // TODO NEWTEZ Fix Combiner.
//    CombineInput combineIn = new CombineInput(kvIter);
//    combineIn.initialize(job, runningTaskContext.getTaskReporter());
//
//    CombineOutput combineOut = new CombineOutput(writer);
//    combineOut.initialize(job, runningTaskContext.getTaskReporter());
//
//    try {
//      combineProcessor.process(new Input[] {combineIn},
//          new Output[] {combineOut});
//    } catch (IOException ioe) {
//      try {
//        combineProcessor.close();
//      } catch (IOException ignored) {}
//
//      // Do not close output here as the sorter should close the combine output
//
//      throw ioe;
//    }

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
  
  

  private TezTaskOutput instantiateTaskOutputManager(Configuration conf, TezOutputContext outputContext) {
    Class<?> clazz = conf.getClass(Constants.TEZ_ENGINE_TASK_OUTPUT_MANAGER,
        TezTaskOutputFiles.class);
    try {
      Constructor<?> ctor = clazz.getConstructor(Configuration.class, String.class);
      ctor.setAccessible(true);
      TezTaskOutput instance = (TezTaskOutput) ctor.newInstance(conf, outputContext.getUniqueIdentifier());
      return instance;
    } catch (Exception e) {
      throw new TezUncheckedException(
          "Unable to instantiate configured TezOutputFileManager: "
              + conf.get(Constants.TEZ_ENGINE_TASK_OUTPUT_MANAGER,
                  TezTaskOutputFiles.class.getName()), e);
    }
  }
  
}
