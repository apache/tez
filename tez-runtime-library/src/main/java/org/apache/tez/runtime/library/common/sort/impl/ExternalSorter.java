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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;

import com.google.common.base.Preconditions;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class ExternalSorter {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalSorter.class);

  public List<Event> close() throws IOException {
    spillFileIndexPaths.clear();
    spillFilePaths.clear();
    reportStatistics();
    outputContext.notifyProgress();
    return Collections.emptyList();
  }

  public abstract void flush() throws IOException;

  public abstract void write(Object key, Object value) throws IOException;

  public void write(Object key, Iterable<Object> values) throws IOException {
    //TODO: Sorter classes should override this method later.
    Iterator<Object> it = values.iterator();
    while(it.hasNext()) {
      write(key, it.next());
    }
  }

  protected final Progressable progressable = new Progressable() {
    @Override
    public void progress() {
      outputContext.notifyProgress();
    }
  };

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

  protected final long availableMemoryMb;

  protected final IndexedSorter sorter;

  // Compression for map-outputs
  protected final CompressionCodec codec;

  protected final Map<Integer, Path> spillFilePaths = Maps.newHashMap();
  protected final Map<Integer, Path> spillFileIndexPaths = Maps.newHashMap();

  protected Path finalOutputFile;
  protected Path finalIndexFile;
  protected int numSpills;

  protected final boolean cleanup;

  protected OutputStatisticsReporter statsReporter;
  protected final long[] partitionStats;
  protected final boolean finalMergeEnabled;
  protected final boolean sendEmptyPartitionDetails;

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
  // Number of spills written & consumed by the same task to generate the final file
  protected final TezCounter numAdditionalSpills;
  // Number of files offered via shuffle-handler to consumers.
  protected final TezCounter numShuffleChunks;
  // How partition stats should be reported.
  final ReportPartitionStats reportPartitionStats;

  public ExternalSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    this.partitions = numOutputs;
    reportPartitionStats = ReportPartitionStats.fromString(
        conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));
    partitionStats = reportPartitionStats.isEnabled() ?
        (new long[partitions]) : null;

    cleanup = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT,
        TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT_DEFAULT);

    rfs = ((LocalFileSystem)FileSystem.getLocal(this.conf)).getRaw();

    if (LOG.isDebugEnabled()) {
      LOG.debug(outputContext.getDestinationVertexName() + ": Initial Mem bytes : " +
          initialMemoryAvailable + ", in MB=" + ((initialMemoryAvailable >> 20)));
    }
    int assignedMb = (int) (initialMemoryAvailable >> 20);
    //Let the overflow checks happen in appropriate sorter impls
    this.availableMemoryMb = assignedMb;

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
    LOG.info(outputContext.getDestinationVertexName() + " using: "
        + "memoryMb=" + assignedMb
        + ", keySerializerClass=" + keyClass
        + ", valueSerializerClass=" + valSerializer
        + ", comparator=" + (RawComparator) ConfigUtils.getIntermediateOutputKeyComparator(conf)
        + ", partitioner=" + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS)
        + ", serialization=" + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY)
        + ", reportPartitionStats=" + reportPartitionStats);

    //    counters    
    mapOutputByteCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    mapOutputRecordCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    outputBytesWithOverheadCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    fileOutputByteCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    spilledRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    additionalSpillBytesWritten = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    additionalSpillBytesRead = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);
    numAdditionalSpills = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
    numShuffleChunks = outputContext.getCounters().findCounter(TaskCounter.SHUFFLE_CHUNK_COUNT);

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

    this.conf.setInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, this.partitions);
    this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    this.combiner = TezRuntimeUtils.instantiateCombiner(this.conf, outputContext);

    this.statsReporter = outputContext.getStatisticsReporter();
    this.finalMergeEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT_DEFAULT);
    this.sendEmptyPartitionDetails = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);
  }

  @VisibleForTesting
  public boolean isFinalMergeEnabled() {
    return finalMergeEnabled;
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

  @Private
  public Path getFinalIndexFile() {
    return finalIndexFile;
  }

  public Path getFinalOutputFile() {
    return finalOutputFile;
  }

  protected void runCombineProcessor(TezRawKeyValueIterator kvIter,
      Writer writer) throws IOException {
    try {
      outputContext.notifyProgress();
      combiner.combine(kvIter, writer);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOInterruptedException("Combiner interrupted", e);
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
    long reqBytes = ((long) initialMemRequestMb) << 20;
    //Higher bound checks are done in individual sorter implementations
    Preconditions.checkArgument(initialMemRequestMb > 0 && reqBytes < maxAvailableTaskMemory,
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + " " + initialMemRequestMb + " should be "
            + "larger than 0 and should be less than the available task memory (MB):" +
            (maxAvailableTaskMemory >> 20));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested SortBufferSize ("
          + TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + "): "
          + initialMemRequestMb);
    }
    return reqBytes;
  }

  public int getNumSpills() {
    return numSpills;
  }

  protected synchronized void cleanup() throws IOException {
    if (!cleanup) {
      return;
    }
    cleanup(spillFilePaths);
    cleanup(spillFileIndexPaths);
    //TODO: What if when same volume rename happens (have to rely on job completion cleanup)
    cleanup(finalOutputFile);
    cleanup(finalIndexFile);
  }

  protected synchronized void cleanup(Path path) {
    if (path == null || !cleanup) {
      return;
    }
    try {
      LOG.info("Deleting " + path);
      rfs.delete(path, true);
    } catch(IOException ioe) {
      LOG.warn("Error in deleting "  + path);
    }
  }

  protected synchronized void cleanup(Map<Integer, Path> spillMap) {
    if (!cleanup) {
      return;
    }
    for(Map.Entry<Integer, Path> entry : spillMap.entrySet()) {
      cleanup(entry.getValue());
    }
  }

  public long[] getPartitionStats() {
    return partitionStats;
  }

  protected boolean reportPartitionStats() {
    return (partitionStats != null);
  }

  protected synchronized void reportStatistics() {
    // This works for non-started outputs since new counters will be created with an initial value of 0
    long outputSize = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES).getValue();
    statsReporter.reportDataSize(outputSize);
    long outputRecords = outputContext.getCounters()
        .findCounter(TaskCounter.OUTPUT_RECORDS).getValue();
    statsReporter.reportItemsProcessed(outputRecords);
  }

  public boolean reportDetailedPartitionStats() {
    return reportPartitionStats.isPrecise();
  }
}
