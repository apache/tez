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

package org.apache.tez.runtime.library.common.sort.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.hadoop.compat.NullProgressable;

import com.google.common.base.Preconditions;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class ExternalSorter {

  private static final Log LOG = LogFactory.getLog(ExternalSorter.class);

  public abstract void close() throws IOException;

  public abstract void flush() throws IOException;

  public abstract void write(Object key, Object value) throws IOException;

  public void write(Object key, Iterable<Object> values) throws IOException {
    //TODO: Sorter classes should override this method later.
    Iterator<Object> it = values.iterator();
    while(it.hasNext()) {
      write(key, it.next());
    }
  }

  protected final Progressable nullProgressable = new NullProgressable();
  protected final OutputContext outputContext;
  protected final Combiner combiner;
  protected final Partitioner partitioner;
  protected final Configuration conf;
  protected final FileSystem rfs;
  protected final TezTaskOutput mapOutputFile;
  protected final int partitions;
  protected final Class keyClass;
  protected final Class valClass;
  protected final RawComparator comparator;
  protected final SerializationFactory serializationFactory;
  protected final Serializer keySerializer;
  protected final Serializer valSerializer;
  
  protected final boolean ifileReadAhead;
  protected final int ifileReadAheadLength;
  protected final int ifileBufferSize;

  protected final int availableMemoryMb;

  protected final IndexedSorter sorter;

  // Compression for map-outputs
  protected final CompressionCodec codec;

  // Counters
  // MR compatilbity layer needs to rename counters back to what MR requries.

  // Represents final deserialized size of output (spills are not counted)
  protected final TezCounter mapOutputByteCounter;
  // Represents final number of records written (spills are not counted)
  protected final TezCounter mapOutputRecordCounter;
  // Represents the size of the final output - with any overheads introduced by
  // the storage/serialization mechanism. This is an uncompressed data size.
  protected final TezCounter outputBytesWithOverheadCounter;
  // Represents the size of the final output - which will be transmitted over
  // the wire (spills are not counted). Factors in compression if it is enabled.
  protected final TezCounter fileOutputByteCounter;
  // Represents total number of records written to disk (includes spills. Min
  // value for this is equal to number of output records)
  protected final TezCounter spilledRecordsCounter;
  // Bytes written as a result of additional spills. The single spill for the
  // final output data is not considered. (This will be 0 if there's no
  // additional spills. Compressed size - so may not represent the size in the
  // sort buffer)
  protected final TezCounter additionalSpillBytesWritten;
  
  protected final TezCounter additionalSpillBytesRead;
  // Number of additional spills. (This will be 0 if there's no additional
  // spills)
  protected final TezCounter numAdditionalSpills;

  public ExternalSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    this.partitions = numOutputs;

    rfs = ((LocalFileSystem)FileSystem.getLocal(this.conf)).getRaw();

    int assignedMb = (int) (initialMemoryAvailable >> 20);
    if (assignedMb <= 0) {
      if (initialMemoryAvailable > 0) { // Rounded down to 0MB - may be > 0 && < 1MB
        this.availableMemoryMb = 1;
        LOG.warn("initialAvailableMemory: " + initialMemoryAvailable
            + " is too low. Rounding to 1 MB");
      } else {
        throw new RuntimeException("InitialMemoryAssigned is <= 0: " + initialMemoryAvailable);
      }
    } else {
      this.availableMemoryMb = assignedMb;
    }

    // sorter
    sorter = ReflectionUtils.newInstance(this.conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS, QuickSort.class,
        IndexedSorter.class), this.conf);

    comparator = ConfigUtils.getIntermediateOutputKeyComparator(this.conf);

    // k/v serialization
    keyClass = ConfigUtils.getIntermediateOutputKeyClass(this.conf);
    valClass = ConfigUtils.getIntermediateOutputValueClass(this.conf);
    serializationFactory = new SerializationFactory(this.conf);
    keySerializer = serializationFactory.getSerializer(keyClass);
    valSerializer = serializationFactory.getSerializer(valClass);
    LOG.info("keySerializer=" + keySerializer + "; valueSerializer=" + valSerializer
        + "; comparator=" + (RawComparator) ConfigUtils.getIntermediateOutputKeyComparator(conf)
        + "; conf=" + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));

    //    counters    
    mapOutputByteCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    mapOutputRecordCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    outputBytesWithOverheadCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    fileOutputByteCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    spilledRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    additionalSpillBytesWritten = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    additionalSpillBytesRead = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);
    numAdditionalSpills = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);

    // compression
    if (ConfigUtils.shouldCompressIntermediateOutput(this.conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateOutputCompressorClass(this.conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, this.conf);

      if (codec != null) {
        Class<? extends Compressor> compressorType = null;
        Throwable cause = null;
        try {
          compressorType = codec.getCompressorType();
        } catch (RuntimeException e) {
          cause = e;
        }
        if (compressorType == null) {
          String errMsg =
              String.format("Unable to get CompressorType for codec (%s). This is most" +
                      " likely due to missing native libraries for the codec.",
                  conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC));
          throw new IOException(errMsg, cause);
        }
      }
    } else {
      codec = null;
    }

    this.ifileReadAhead = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }
    this.ifileBufferSize = conf.getInt("io.file.buffer.size",
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);

    
    // Task outputs
    mapOutputFile = TezRuntimeUtils.instantiateTaskOutputManager(conf, outputContext);
    
    LOG.info("Instantiating Partitioner: [" + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS) + "]");
    this.conf.setInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, this.partitions);
    this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    this.combiner = TezRuntimeUtils.instantiateCombiner(this.conf, outputContext);
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
    try {
      combiner.combine(kvIter, writer);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
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

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    int initialMemRequestMb = 
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 
            TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB_DEFAULT);
    Preconditions.checkArgument(initialMemRequestMb != 0, "io.sort.mb should be larger than 0");
    long reqBytes = initialMemRequestMb << 20;
    LOG.info("Requested SortBufferSize (io.sort.mb): " + initialMemRequestMb);
    return reqBytes;
  }
}
