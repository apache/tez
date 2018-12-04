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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.library.common.comparator.ProxyComparator;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.DiskSegment;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;
import org.apache.tez.runtime.library.utils.LocalProgress;
import org.apache.tez.util.StopWatch;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord.SPILL_FILE_PERMS;

@SuppressWarnings({"unchecked", "rawtypes"})
public class PipelinedSorter extends ExternalSorter {
  
  private static final Logger LOG = LoggerFactory.getLogger(PipelinedSorter.class);
  
  /**
   * The size of each record in the index file for the map-outputs.
   */
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

  private final static int APPROX_HEADER_LENGTH = 150;

  private final int partitionBits;
  
  private static final int PARTITION = 0;        // partition offset in acct
  private static final int KEYSTART = 1;         // key offset in acct
  private static final int VALSTART = 2;         // val offset in acct
  private static final int VALLEN = 3;           // val len in acct
  private static final int NMETA = 4;            // num meta ints
  private static final int METASIZE = NMETA * 4; // size in bytes

  private final int minSpillsForCombine;
  private final ProxyComparator hasher;
  // SortSpans  
  private SortSpan span;

  //total memory capacity allocated to sorter
  private final long capacity;

  //track buffer overflow recursively in all buffers
  private int bufferOverflowRecursion;

  // Merger
  private final SpanMerger merger; 
  private final ExecutorService sortmaster;

  private final ArrayList<TezSpillRecord> indexCacheList =
    new ArrayList<TezSpillRecord>();

  private final boolean pipelinedShuffle;

  private long currentAllocatableMemory;
  //Maintain a list of ByteBuffers
  @VisibleForTesting
  final List<ByteBuffer> buffers;
  final int maxNumberOfBlocks;
  private int bufferIndex = -1;
  private final int MIN_BLOCK_SIZE;
  private final boolean lazyAllocateMem;
  private final Deflater deflater;
  private final String auxiliaryService;

  /**
   * Store the events to be send in close.
   */
  private final List<Event> finalEvents;

  // TODO Set additional countesr - total bytes written, spills etc.

  public PipelinedSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);

    lazyAllocateMem = this.conf.getBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY_DEFAULT);

    if (lazyAllocateMem) {
      /**
       * When lazy-allocation is enabled, framework takes care of auto
       * allocating memory on need basis. Desirable block size is set to 256MB
       */
      //256 MB - 64 bytes. See comment for the 32MB allocation.
      MIN_BLOCK_SIZE = ((256 << 20) - 64);
    } else {
      int minBlockSize = conf.getInt(TezRuntimeConfiguration
              .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB,
          TezRuntimeConfiguration
              .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB_DEFAULT);
      Preconditions.checkArgument(
          (minBlockSize > 0 && minBlockSize < 2047),
          TezRuntimeConfiguration
              .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB
              + "=" + minBlockSize + " should be a positive value between 0 and 2047");
      MIN_BLOCK_SIZE = minBlockSize << 20;
    }

    StringBuilder initialSetupLogLine = new StringBuilder("Setting up PipelinedSorter for ")
        .append(outputContext.getDestinationVertexName()).append(": ");
    partitionBits = bitcount(partitions)+1;

    boolean confPipelinedShuffle = this.conf.getBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);

    pipelinedShuffle = !isFinalMergeEnabled() && confPipelinedShuffle;
    auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    //sanity checks
    final long sortmb = this.availableMemoryMb;

    // buffers and accounting
    long maxMemLimit = sortmb << 20;

    initialSetupLogLine.append(", UsingHashComparator=");
    // k/v serialization
    if(comparator instanceof ProxyComparator) {
      hasher = (ProxyComparator)comparator;
      initialSetupLogLine.append(true);
    } else {
      hasher = null;
      initialSetupLogLine.append(false);
    }

    LOG.info(initialSetupLogLine.toString());

    long totalCapacityWithoutMeta = 0;
    long availableMem = maxMemLimit;
    int numBlocks = 0;
    while(availableMem > 0) {
      long size = Math.min(availableMem, computeBlockSize(availableMem, maxMemLimit));
      int sizeWithoutMeta = (int) ((size) - (size % METASIZE));
      totalCapacityWithoutMeta += sizeWithoutMeta;
      availableMem -= size;
      numBlocks++;
    }
    currentAllocatableMemory = maxMemLimit;
    maxNumberOfBlocks = numBlocks;
    capacity = totalCapacityWithoutMeta;

    buffers = Lists.newArrayListWithCapacity(maxNumberOfBlocks);
    allocateSpace(); //Allocate the first block
    if (!lazyAllocateMem) {
      LOG.info("Pre allocating rest of memory buffers upfront");
      while(allocateSpace() != null);
    }

    initialSetupLogLine.append("#blocks=").append(maxNumberOfBlocks);
    initialSetupLogLine.append(", maxMemUsage=").append(maxMemLimit);
    initialSetupLogLine.append(", lazyAllocateMem=").append(
        lazyAllocateMem);
    initialSetupLogLine.append(", minBlockSize=").append(MIN_BLOCK_SIZE);
    initialSetupLogLine.append(", initial BLOCK_SIZE=").append(buffers.get(0).capacity());
    initialSetupLogLine.append(", finalMergeEnabled=").append(isFinalMergeEnabled());
    initialSetupLogLine.append(", pipelinedShuffle=").append(pipelinedShuffle);
    initialSetupLogLine.append(", sendEmptyPartitions=").append(sendEmptyPartitionDetails);
    initialSetupLogLine.append(", ").append(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB).append(
        "=").append(
        sortmb);

    Preconditions.checkState(buffers.size() > 0, "Atleast one buffer needs to be present");
    LOG.info(initialSetupLogLine.toString());

    span = new SortSpan(buffers.get(bufferIndex), 1024 * 1024, 16, this.comparator);
    merger = new SpanMerger(); // SpanIterators are comparable
    final int sortThreads = 
            this.conf.getInt(
                TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS,
                TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS_DEFAULT);
    sortmaster = Executors.newFixedThreadPool(sortThreads,
        new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Sorter {" + TezUtilsInternal
            .cleanVertexName(outputContext.getDestinationVertexName()) + "} #%d")
        .build());


    valSerializer.open(span.out);
    keySerializer.open(span.out);
    minSpillsForCombine = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS, 3);
    deflater = TezCommonUtils.newBestCompressionDeflater();
    finalEvents = Lists.newLinkedList();
  }

  ByteBuffer allocateSpace() {
    if (currentAllocatableMemory <= 0) {
      //No space available.
      return null;
    }

    int size = computeBlockSize(currentAllocatableMemory, availableMemoryMb << 20);
    currentAllocatableMemory -= size;
    int sizeWithoutMeta = (size) - (size % METASIZE);
    ByteBuffer space = ByteBuffer.allocate(sizeWithoutMeta);

    buffers.add(space);
    bufferIndex++;

    Preconditions.checkState(buffers.size() <= maxNumberOfBlocks,
        "Number of blocks " + buffers.size()
            + " is exceeding  " + maxNumberOfBlocks);

    LOG.info("Newly allocated block size=" + size
        + ", index=" + bufferIndex
        + ", Number of buffers=" + buffers.size()
        + ", currentAllocatableMemory=" + currentAllocatableMemory
        + ", currentBufferSize=" + space.capacity()
        + ", total=" + (availableMemoryMb << 20));
    return space;
  }


  @VisibleForTesting
  int computeBlockSize(long availableMem, long maxAllocatedMemory) {
    int maxBlockSize = 0;
    /**
     * When lazy-allocation is enabled, framework takes care of auto allocating
     * memory on need basis. In such cases, first buffer starts with 32 MB.
     */
    if (lazyAllocateMem) {
      if (buffers == null || buffers.isEmpty()) {
        //32 MB - 64 bytes
        // These buffers end up occupying 33554456 (32M + 24) bytes.
        // On large JVMs (64G+), with G1Gc - the region size maxes out at
        // 32M. Without the -64, this structure would end up using 2 regions.
        return ((32 << 20) - 64);
      }
    }

    //Honor MIN_BLOCK_SIZE
    maxBlockSize = Math.max(MIN_BLOCK_SIZE, maxBlockSize);

    if (availableMem < maxBlockSize) {
      maxBlockSize = (int) availableMem;
    }

    int maxMem = (maxAllocatedMemory > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) maxAllocatedMemory;
    if (maxBlockSize > maxMem) {
      maxBlockSize = maxMem;
    }

    availableMem -= maxBlockSize;
    if (availableMem < MIN_BLOCK_SIZE) {
      if ((maxBlockSize + availableMem) < Integer.MAX_VALUE) {
        //Merge remaining with last block
        maxBlockSize += availableMem;
      }
    }
    return maxBlockSize;
  }

  private int bitcount(int n) {
    int bit = 0;
    while(n!=0) {
      bit++;
      n >>= 1;
    }
    return bit;
  }
  
  public void sort() throws IOException {
    SortSpan newSpan = span.next();

    if(newSpan == null) {
      //avoid sort/spill of empty span
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      // sort in the same thread, do not wait for the thread pool
      merger.add(span.sort(sorter));
      boolean ret = spill(true);
      stopWatch.stop();
      if (LOG.isDebugEnabled()) {
        LOG.debug(outputContext.getDestinationVertexName() + ": Time taken for spill " + (stopWatch.now(TimeUnit.MILLISECONDS)) + " ms");
      }
      if (pipelinedShuffle && ret) {
        sendPipelinedShuffleEvents();
      }
      //safe to reset bufferIndex to 0;
      bufferIndex = 0;
      int items = 1024*1024;
      int perItem = 16;
      if(span.length() != 0) {
        items = span.length();
        perItem = span.kvbuffer.limit()/items;
        items = (int) ((span.capacity)/(METASIZE+perItem));
        if(items > 1024*1024) {
            // our goal is to have 1M splits and sort early
            items = 1024*1024;
        }
      }
      Preconditions.checkArgument(buffers.get(bufferIndex) != null, "block should not be empty");
      //TODO: fix per item being passed.
      span = new SortSpan((ByteBuffer)buffers.get(bufferIndex).clear(), (1024*1024),
          perItem, ConfigUtils.getIntermediateOutputKeyComparator(this.conf));
    } else {
      // queue up the sort
      SortTask task = new SortTask(span, sorter);
      LOG.debug("Submitting span={} for sort", span.toString());
      Future<SpanIterator> future = sortmaster.submit(task);
      merger.add(future);
      span = newSpan;
    }
    valSerializer.open(span.out);
    keySerializer.open(span.out);
  }

  // if pipelined shuffle is enabled, this method is called to send events for every spill
  private void sendPipelinedShuffleEvents() throws IOException{
    List<Event> events = Lists.newLinkedList();
    String pathComponent = (outputContext.getUniqueIdentifier() + "_" + (numSpills-1));
    ShuffleUtils.generateEventOnSpill(events, isFinalMergeEnabled(), false,
        outputContext, (numSpills - 1), indexCacheList.get(numSpills - 1),
        partitions, sendEmptyPartitionDetails, pathComponent, partitionStats,
        reportDetailedPartitionStats(), auxiliaryService, deflater);
    outputContext.sendEvents(events);
    LOG.info(outputContext.getDestinationVertexName() +
        ": Added spill event for spill (final update=false), spillId=" + (numSpills - 1));
  }

  @Override
  public void write(Object key, Object value)
      throws IOException {
    collect(
        key, value, partitioner.getPartition(key, value, partitions));
  }

  /**
   * Serialize the key, value to intermediate storage.
   * When this method returns, kvindex must refer to sufficient unused
   * storage to store one METADATA.
   */
  synchronized void collect(Object key, Object value, final int partition
                                   ) throws IOException {
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
                            + keyClass.getName() + ", received "
                            + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
                            + valClass.getName() + ", received "
                            + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")");
    }
    // TBD:FIX in TEZ-2574
    if (span.kvmeta.remaining() < METASIZE) {
      this.sort();
      if (span.length() == 0) {
        spillSingleRecord(key, value, partition);
        return;
      }
    }
    int keystart = span.kvbuffer.position();
    int valstart = -1;
    int valend = -1;
    try {
      keySerializer.serialize(key);
      valstart = span.kvbuffer.position();      
      valSerializer.serialize(value);
      valend = span.kvbuffer.position();
    } catch(BufferOverflowException overflow) {
      // restore limit
      span.kvbuffer.position(keystart);
      this.sort();
      if (span.length() == 0 || bufferOverflowRecursion > buffers.size()) {
        // spill the current key value pair
        spillSingleRecord(key, value, partition);
        bufferOverflowRecursion = 0;
        return;
      }
      bufferOverflowRecursion++;
      // try again
      this.collect(key, value, partition);
      return;
    }

    if (bufferOverflowRecursion > 0) {
      bufferOverflowRecursion--;
    }

    int prefix = 0;

    if(hasher != null) {
      prefix = hasher.getProxy(key);
    }

    prefix = (partition << (32 - partitionBits)) | (prefix >>> partitionBits);

    /* maintain order as in PARTITION, KEYSTART, VALSTART, VALLEN */
    span.kvmeta.put(prefix);
    span.kvmeta.put(keystart);
    span.kvmeta.put(valstart);
    span.kvmeta.put(valend - valstart);
    mapOutputRecordCounter.increment(1);
    outputContext.notifyProgress();
    mapOutputByteCounter.increment(valend - keystart);
  }

  private void adjustSpillCounters(long rawLength, long compLength) {
    if (!isFinalMergeEnabled()) {
      outputBytesWithOverheadCounter.increment(rawLength);
    } else {
      if (numSpills > 0) {
        additionalSpillBytesWritten.increment(compLength);
        // Reset the value will be set during the final merge.
        outputBytesWithOverheadCounter.setValue(0);
      } else {
        // Set this up for the first write only. Subsequent ones will be handled in the final merge.
        outputBytesWithOverheadCounter.increment(rawLength);
      }
    }
  }

  // it is guaranteed that when spillSingleRecord is called, there is
  // no merger spans queued in executor.
  private void spillSingleRecord(final Object key, final Object value,
          int partition) throws IOException {
    final TezSpillRecord spillRec = new TezSpillRecord(partitions);
    // getSpillFileForWrite with size -1 as the serialized size of KV pair is still unknown
    final Path filename = mapOutputFile.getSpillFileForWrite(numSpills, -1);
    Path indexFilename =
        mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
            * MAP_OUTPUT_INDEX_RECORD_LENGTH);
    spillFilePaths.put(numSpills, filename);
    FSDataOutputStream out = rfs.create(filename, true, 4096);
    if (!SPILL_FILE_PERMS.equals(SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(conf)))) {
      rfs.setPermission(filename, SPILL_FILE_PERMS);
    }

    try {
      LOG.info(outputContext.getDestinationVertexName() + ": Spilling to " + filename.toString() +
          ", indexFilename=" + indexFilename);
      for (int i = 0; i < partitions; ++i) {
        if (isThreadInterrupted()) {
          return;
        }
        Writer writer = null;
        try {
          long segmentStart = out.getPos();
          if (!sendEmptyPartitionDetails || (i == partition)) {
            writer = new Writer(conf, out, keyClass, valClass, codec,
                spilledRecordsCounter, null, false);
          }
          // we need not check for combiner since its a single record
          if (i == partition) {
            final long recordStart = out.getPos();
            writer.append(key, value);
            mapOutputRecordCounter.increment(1);
            mapOutputByteCounter.increment(out.getPos() - recordStart);
          }
          long rawLength = 0;
          long partLength = 0;
          if (writer != null) {
            writer.close();
            rawLength = writer.getRawLength();
            partLength = writer.getCompressedLength();
          }
          adjustSpillCounters(rawLength, partLength);
          // record offsets
          final TezIndexRecord rec =
              new TezIndexRecord(
                  segmentStart, rawLength, partLength);
          spillRec.putIndex(rec, i);
          writer = null;
        } finally {
          if (null != writer) {
            writer.close();
          }
        }
      }

      spillFileIndexPaths.put(numSpills, indexFilename);
      spillRec.writeToFile(indexFilename, conf);
      //TODO: honor cache limits
      indexCacheList.add(spillRec);
      ++numSpills;
      if (!isFinalMergeEnabled()) {
          fileOutputByteCounter.increment(rfs.getFileStatus(filename).getLen());
          //No final merge. Set the number of files offered via shuffle-handler
          numShuffleChunks.setValue(numSpills);
      }
      if (pipelinedShuffle) {
        sendPipelinedShuffleEvents();
      }
    } finally {
        out.close();
    }
  }

  public boolean spill(boolean ignoreEmptySpills) throws IOException {
    FSDataOutputStream out = null;
    try {
      try {
        boolean ret = merger.ready();
        // if merger returned false and ignore merge is true,
        // then return directly without spilling
        if (!ret && ignoreEmptySpills){
          return false;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info(outputContext.getDestinationVertexName() + ": Interrupted while waiting for mergers to complete");
        throw new IOInterruptedException(outputContext.getDestinationVertexName() + ": Interrupted while waiting for mergers to complete", e);
      }

      // create spill file
      final long size = capacity +
          + (partitions * APPROX_HEADER_LENGTH);
      final TezSpillRecord spillRec = new TezSpillRecord(partitions);
      final Path filename =
        mapOutputFile.getSpillFileForWrite(numSpills, size);
      spillFilePaths.put(numSpills, filename);
      out = rfs.create(filename, true, 4096);
      if (!SPILL_FILE_PERMS.equals(SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(conf)))) {
        rfs.setPermission(filename, SPILL_FILE_PERMS);
      }
      LOG.info(outputContext.getDestinationVertexName() + ": Spilling to " + filename.toString());
      for (int i = 0; i < partitions; ++i) {
        if (isThreadInterrupted()) {
          return false;
        }
        outputContext.notifyProgress();
        TezRawKeyValueIterator kvIter = merger.filter(i);
        //write merged output to disk
        long segmentStart = out.getPos();
        Writer writer = null;
        boolean hasNext = kvIter.hasNext();
        if (hasNext || !sendEmptyPartitionDetails) {
          writer = new Writer(conf, out, keyClass, valClass, codec,
              spilledRecordsCounter, null, merger.needsRLE());
        }
        if (combiner == null) {
          while (kvIter.next()) {
            writer.append(kvIter.getKey(), kvIter.getValue());
          }
        } else {          
          if (hasNext) {
            runCombineProcessor(kvIter, writer);
          }
        }
        long rawLength = 0;
        long partLength = 0;
        //close
        if (writer != null) {
          writer.close();
          rawLength = writer.getRawLength();
          partLength = writer.getCompressedLength();
        }
        adjustSpillCounters(rawLength, partLength);
        // record offsets
        final TezIndexRecord rec =
            new TezIndexRecord(segmentStart, rawLength, partLength);
        spillRec.putIndex(rec, i);
        if (!isFinalMergeEnabled() && reportPartitionStats()) {
          partitionStats[i] += partLength;
        }
      }

      Path indexFilename =
        mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
            * MAP_OUTPUT_INDEX_RECORD_LENGTH);
      spillFileIndexPaths.put(numSpills, indexFilename);
      spillRec.writeToFile(indexFilename, conf);
      //TODO: honor cache limits
      indexCacheList.add(spillRec);
      ++numSpills;
      if (!isFinalMergeEnabled()) {
        fileOutputByteCounter.increment(rfs.getFileStatus(filename).getLen());
        //No final merge. Set the number of files offered via shuffle-handler
        numShuffleChunks.setValue(numSpills);
      }
      return true;
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  private boolean isThreadInterrupted() throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      if (cleanup) {
        cleanup();
      }
      sortmaster.shutdownNow();
      LOG.info(outputContext.getDestinationVertexName() + ": Thread interrupted, cleaned up stale data, sorter threads shutdown=" + sortmaster
          .isShutdown() + ", terminated=" + sortmaster.isTerminated());
      return true;
    }
    return false;
  }

  @Override
  public void flush() throws IOException {
    final String uniqueIdentifier = outputContext.getUniqueIdentifier();

    outputContext.notifyProgress();
    /**
     * Possible that the thread got interrupted when flush was happening or when the flush was
     * never invoked. As a part of cleanup activity in TezTaskRunner, it would invoke close()
     * on all I/O. At that time, this is safe to cleanup
     */
    if (isThreadInterrupted()) {
      return;
    }

    try {
      LOG.info(outputContext.getDestinationVertexName() + ": Starting flush of map output");
      span.end();
      merger.add(span.sort(sorter));
      // force a spill in flush()
      // case 1: we want to force because of following scenarios:
      // we have no keys written, and flush got called
      // we want atleast one spill(be it empty)
      // case 2: in pipeline shuffle case, we have no way of
      // knowing the last key being written until flush is called
      // so for flush()->spill() we want to force spill so that
      // we can send pipeline shuffle event with last event true.
      spill(false);
      sortmaster.shutdown();

      //safe to clean up
      buffers.clear();


      if(indexCacheList.isEmpty()) {
        /*
         * If we do not have this check, and if the task gets killed in the middle, it can throw
         * NPE leading to distraction when debugging.
         */
        if (LOG.isDebugEnabled()) {
          LOG.debug(outputContext.getDestinationVertexName()
              + ": Index list is empty... returning");
        }
        return;
      }

      if (!isFinalMergeEnabled()) {

        //For pipelined shuffle, previous events are already sent. Just generate the last event alone
        int startIndex = (pipelinedShuffle) ? (numSpills - 1) : 0;
        int endIndex = numSpills;

        for (int i = startIndex; i < endIndex; i++) {
          boolean isLastEvent = (i == numSpills - 1);
          String pathComponent = (outputContext.getUniqueIdentifier() + "_" + i);
          ShuffleUtils.generateEventOnSpill(finalEvents, isFinalMergeEnabled(), isLastEvent,
              outputContext, i, indexCacheList.get(i), partitions,
              sendEmptyPartitionDetails, pathComponent, partitionStats,
              reportDetailedPartitionStats(), auxiliaryService, deflater);
          LOG.info(outputContext.getDestinationVertexName() + ": Adding spill event for spill (final update=" + isLastEvent + "), spillId=" + i);
        }
        return;
      }

      numAdditionalSpills.increment(numSpills - 1);

      //In case final merge is required, the following code path is executed.
      if (numSpills == 1) {
        // someday be able to pass this directly to shuffle
        // without writing to disk
        final Path filename = spillFilePaths.get(0);
        final Path indexFilename = spillFileIndexPaths.get(0);
        finalOutputFile = mapOutputFile.getOutputFileForWriteInVolume(filename);
        finalIndexFile = mapOutputFile.getOutputIndexFileForWriteInVolume(indexFilename);

        sameVolRename(filename, finalOutputFile);
        sameVolRename(indexFilename, finalIndexFile);
        if (LOG.isDebugEnabled()) {
          LOG.debug(outputContext.getDestinationVertexName() + ": numSpills=" + numSpills +
              ", finalOutputFile=" + finalOutputFile + ", "
              + "finalIndexFile=" + finalIndexFile + ", filename=" + filename + ", indexFilename=" +
              indexFilename);
        }
        TezSpillRecord spillRecord = new TezSpillRecord(finalIndexFile, conf);
        if (reportPartitionStats()) {
          for (int i = 0; i < spillRecord.size(); i++) {
            partitionStats[i] += spillRecord.getIndex(i).getPartLength();
          }
        }
        numShuffleChunks.setValue(numSpills);
        fileOutputByteCounter.increment(rfs.getFileStatus(finalOutputFile).getLen());
        // ??? why are events not being sent here?
        return;
      }

      finalOutputFile =
          mapOutputFile.getOutputFileForWrite(0); //TODO
      finalIndexFile =
          mapOutputFile.getOutputIndexFileForWrite(0); //TODO

      if (LOG.isDebugEnabled()) {
        LOG.debug(outputContext.getDestinationVertexName() + ": " +
            "numSpills: " + numSpills + ", finalOutputFile:" + finalOutputFile + ", finalIndexFile:"
                + finalIndexFile);
      }
      //The output stream for the final single output file
      FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);
      if (!SPILL_FILE_PERMS.equals(SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(conf)))) {
        rfs.setPermission(finalOutputFile, SPILL_FILE_PERMS);
      }

      final TezSpillRecord spillRec = new TezSpillRecord(partitions);

      for (int parts = 0; parts < partitions; parts++) {
        boolean shouldWrite = false;
        //create the segments to be merged
        List<Segment> segmentList =
            new ArrayList<Segment>(numSpills);
        for (int i = 0; i < numSpills; i++) {
          Path spillFilename = spillFilePaths.get(i);
          TezIndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);
          if (indexRecord.hasData() || !sendEmptyPartitionDetails) {
            shouldWrite = true;
            DiskSegment s =
                new DiskSegment(rfs, spillFilename, indexRecord.getStartOffset(),
                    indexRecord.getPartLength(), codec, ifileReadAhead,
                    ifileReadAheadLength, ifileBufferSize, true);
            segmentList.add(s);
          }
        }

        int mergeFactor =
            this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR,
                TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT);
        // sort the segments only if there are intermediate merges
        boolean sortSegments = segmentList.size() > mergeFactor;
        //merge
        TezRawKeyValueIterator kvIter = TezMerger.merge(conf, rfs,
            keyClass, valClass, codec,
            segmentList, mergeFactor,
            new Path(uniqueIdentifier),
            (RawComparator) ConfigUtils.getIntermediateOutputKeyComparator(conf),
            progressable, sortSegments, true,
            null, spilledRecordsCounter, additionalSpillBytesRead,
            null, merger.needsRLE()); // Not using any Progress in TezMerger. Should just work.
        //write merged output to disk
        long segmentStart = finalOut.getPos();
        long rawLength = 0;
        long partLength = 0;
        if (shouldWrite) {
          Writer writer =
              new Writer(conf, finalOut, keyClass, valClass, codec,
                  spilledRecordsCounter, null, merger.needsRLE());
          if (combiner == null || numSpills < minSpillsForCombine) {
            TezMerger.writeFile(kvIter, writer, progressable,
                TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
          } else {
            runCombineProcessor(kvIter, writer);
          }

          //close
          writer.close();
          rawLength = writer.getRawLength();
          partLength = writer.getCompressedLength();
        }
        outputBytesWithOverheadCounter.increment(rawLength);

        // record offsets
        final TezIndexRecord rec =
            new TezIndexRecord(segmentStart, rawLength, partLength);
        spillRec.putIndex(rec, parts);
        if (reportPartitionStats()) {
          partitionStats[parts] += partLength;
        }
      }

      numShuffleChunks.setValue(1); //final merge has happened.
      fileOutputByteCounter.increment(rfs.getFileStatus(finalOutputFile).getLen());

      spillRec.writeToFile(finalIndexFile, conf);
      finalOut.close();
      for (int i = 0; i < numSpills; i++) {
        Path indexFilename = spillFileIndexPaths.get(i);
        Path spillFilename = spillFilePaths.get(i);
        rfs.delete(indexFilename, true);
        rfs.delete(spillFilename, true);
      }

      spillFileIndexPaths.clear();
      spillFilePaths.clear();
    } catch(InterruptedException ie) {
      if (cleanup) {
        cleanup();
      }
      Thread.currentThread().interrupt();
      throw new IOInterruptedException("Interrupted while closing Output", ie);
    }
  }

  /**
   * Close and send events.
   * @return events to be returned by the edge.
   * @throws IOException parent can throw this.
   */
  public final List<Event> close() throws IOException {
    super.close();
    return finalEvents;
  }


  private interface PartitionedRawKeyValueIterator extends TezRawKeyValueIterator {
    int getPartition();
    Integer peekPartition();
  }

  private static class BufferStreamWrapper extends OutputStream
  {
    private final ByteBuffer out;
    public BufferStreamWrapper(ByteBuffer out) {
      this.out = out;
    }
    
    @Override
    public void write(int b) throws IOException { out.put((byte)b); }
    @Override
    public void write(byte[] b) throws IOException { out.put(b); }
    @Override
    public void write(byte[] b, int off, int len) throws IOException { out.put(b, off, len); }
  }

  private static final class InputByteBuffer extends DataInputBuffer {
    private byte[] buffer = new byte[256]; 
    private ByteBuffer wrapped = ByteBuffer.wrap(buffer);
    private void resize(int length) {
      if(length > buffer.length || (buffer.length > 10 * (1+length))) {
        // scale down as well as scale up across values
        buffer = new byte[length];
        wrapped = ByteBuffer.wrap(buffer);
      }
      wrapped.limit(length);
    }

    // shallow copy
    public void reset(DataInputBuffer clone) {
      byte[] data = clone.getData();
      int start = clone.getPosition();
      int length = clone.getLength() - start;
      super.reset(data, start, length);
    }

    // deep copy
    @SuppressWarnings("unused")
    public void copy(DataInputBuffer clone) {
      byte[] data = clone.getData();
      int start = clone.getPosition();
      int length = clone.getLength() - start;
      resize(length);
      System.arraycopy(data, start, buffer, 0, length);
      super.reset(buffer, 0, length);
    }
  }

  private final class SortSpan implements IndexedSortable {
    final IntBuffer kvmeta;
    final byte[] rawkvmeta;
    final int kvmetabase;
    final ByteBuffer kvbuffer;
    final NonSyncDataOutputStream out;
    final RawComparator comparator;
    final byte[] imeta = new byte[METASIZE];

    private int index = 0;
    private long eq = 0;
    private boolean reinit = false;
    private int capacity;


    public SortSpan(ByteBuffer source, int maxItems, int perItem, RawComparator comparator) {
      capacity = source.remaining();
      int metasize = METASIZE*maxItems;
      int dataSize = maxItems * perItem;
      if(capacity < (metasize+dataSize)) {
        // try to allocate less meta space, because we have sample data
        metasize = METASIZE*(capacity/(perItem+METASIZE));
      }
      ByteBuffer reserved = source.duplicate();
      reserved.mark();
      LOG.info(outputContext.getDestinationVertexName() + ": " + "reserved.remaining()=" +
          reserved.remaining() + ", reserved.metasize=" + metasize);
      reserved.position(metasize);
      kvbuffer = reserved.slice();
      reserved.flip();
      reserved.limit(metasize);
      ByteBuffer kvmetabuffer = reserved.slice();
      rawkvmeta = kvmetabuffer.array();
      kvmetabase = kvmetabuffer.arrayOffset();
      kvmeta = kvmetabuffer
                .order(ByteOrder.nativeOrder())
               .asIntBuffer();
      out = new NonSyncDataOutputStream(
              new BufferStreamWrapper(kvbuffer));
      this.comparator = comparator;
    }

    public SpanIterator sort(IndexedSorter sorter) {
      long start = System.currentTimeMillis();
      if(length() > 1) {
        sorter.sort(this, 0, length(), progressable);
      }
      LOG.info(outputContext.getDestinationVertexName() + ": " + "done sorting span=" + index + ", length=" + length() + ", "
          + "time=" + (System.currentTimeMillis() - start));
      return new SpanIterator((SortSpan)this);
    }

    int offsetFor(int i) {
      return (i * NMETA);
    }

    public void swap(final int mi, final int mj) {
      final int kvi = offsetFor(mi);
      final int kvj = offsetFor(mj);

      final int kvioff = kvmetabase + (kvi << 2);
      final int kvjoff = kvmetabase + (kvj << 2);
      System.arraycopy(rawkvmeta, kvioff, imeta, 0, METASIZE);
      System.arraycopy(rawkvmeta, kvjoff, rawkvmeta, kvioff, METASIZE);
      System.arraycopy(imeta, 0, rawkvmeta, kvjoff, METASIZE);
    }

    protected int compareKeys(final int kvi, final int kvj) {
      final int istart = kvmeta.get(kvi + KEYSTART);
      final int jstart = kvmeta.get(kvj + KEYSTART);
      final int ilen   = kvmeta.get(kvi + VALSTART) - istart;
      final int jlen   = kvmeta.get(kvj + VALSTART) - jstart;

      if (ilen == 0 || jlen == 0) {
        if (ilen == jlen) {
          eq++;
        }
        return ilen - jlen;
      }

      final byte[] buf = kvbuffer.array();
      final int off = kvbuffer.arrayOffset();

      // sort by key
      final int cmp = comparator.compare(buf, off + istart, ilen, buf, off + jstart, jlen);
      if(cmp == 0) eq++;
      return cmp;
    }


    public int compare(final int mi, final int mj) {
      final int kvi = offsetFor(mi);
      final int kvj = offsetFor(mj);
      final int kvip = kvmeta.get(kvi + PARTITION);
      final int kvjp = kvmeta.get(kvj + PARTITION);
      // sort by partition      
      if (kvip != kvjp) {
        return kvip - kvjp;
      }
      return compareKeys(kvi, kvj);
    }

    public SortSpan next() {
      ByteBuffer remaining = end();
      if(remaining != null) {
        SortSpan newSpan = null;
        int items = length();
        int perItem = kvbuffer.position()/items;
        if (reinit) { //next mem block
          //quite possible that the previous span had a length of 1. It is better to reinit here for new span.
          items = 1024*1024;
          perItem = 16;
        }
        final RawComparator newComparator = ConfigUtils.getIntermediateOutputKeyComparator(conf);
        if (this.comparator == newComparator) {
          LOG.warn("Same comparator used. comparator={}, newComparator={},"
                  + " hashCode: comparator={}, newComparator={}",
              this.comparator, newComparator,
              System.identityHashCode(this.comparator),
              System.identityHashCode(newComparator));
        }
        newSpan = new SortSpan(remaining, items, perItem, newComparator);
        newSpan.index = index+1;
        LOG.info(String.format(outputContext.getDestinationVertexName() + ": " + "New Span%d.length = %d, perItem = %d", newSpan.index, newSpan
            .length(), perItem) + ", counter:" + mapOutputRecordCounter.getValue());
        return newSpan;
      }
      return null;
    }

    public int length() {
      return kvmeta.limit()/NMETA;
    }

    public ByteBuffer end() {
      ByteBuffer remaining = kvbuffer.duplicate();
      remaining.position(kvbuffer.position());
      remaining = remaining.slice();
      kvbuffer.limit(kvbuffer.position());
      kvmeta.limit(kvmeta.position());
      int items = length();
      if(items == 0) {
        return null;
      }
      int perItem = kvbuffer.position()/items;
      LOG.info(outputContext.getDestinationVertexName() + ": " + String.format("Span%d.length = %d, perItem = %d", index, length(), perItem));
      if(remaining.remaining() < METASIZE+perItem) {
        //Check if we can get the next Buffer from the main buffer list
        ByteBuffer space = allocateSpace();
        if (space != null) {
          LOG.info(outputContext.getDestinationVertexName() + ": " + "Getting memory from next block in the list, recordsWritten=" +
              mapOutputRecordCounter.getValue());
          reinit = true;
          return space;
        }
        return null;
      }
      return remaining;
    }

    public int compareInternal(final DataInputBuffer needle, final int needlePart, final int index) {
      int cmp = 0;
      final int keystart;
      final int valstart;
      final int partition;
      partition = kvmeta.get(this.offsetFor(index) + PARTITION);
      if(partition != needlePart) {
          cmp = (partition-needlePart);
      } else {
        keystart = kvmeta.get(this.offsetFor(index) + KEYSTART);
        valstart = kvmeta.get(this.offsetFor(index) + VALSTART);
        final byte[] buf = kvbuffer.array();
        final int off = kvbuffer.arrayOffset();
        cmp = comparator.compare(buf,
            keystart + off , (valstart - keystart),
            needle.getData(),
            needle.getPosition(), (needle.getLength() - needle.getPosition()));
      }
      return cmp;
    }
    
    public long getEq() {
      return eq;
    }
    
    @Override
    public String toString() {
        return String.format("Span[%d,%d]", NMETA*kvmeta.capacity(), kvbuffer.limit());
    }
  }

  private static class SpanIterator implements PartitionedRawKeyValueIterator, Comparable<SpanIterator> {
    private int kvindex = -1;
    private final int maxindex;
    private final IntBuffer kvmeta;
    private final ByteBuffer kvbuffer;
    private final SortSpan span;
    private final InputByteBuffer key = new InputByteBuffer();
    private final InputByteBuffer value = new InputByteBuffer();
    private final Progress progress = new LocalProgress();

    private static final int minrun = (1 << 4);

    public SpanIterator(SortSpan span) {
      this.kvmeta = span.kvmeta;
      this.kvbuffer = span.kvbuffer;
      this.span = span;
      this.maxindex = (kvmeta.limit()/NMETA) - 1;
    }

    public DataInputBuffer getKey()  {
      final int keystart = kvmeta.get(span.offsetFor(kvindex) + KEYSTART);
      final int valstart = kvmeta.get(span.offsetFor(kvindex) + VALSTART);
      final byte[] buf = kvbuffer.array();
      final int off = kvbuffer.arrayOffset();
      key.reset(buf, off + keystart, valstart - keystart);
      return key;
    }

    public DataInputBuffer getValue() {
      final int valstart = kvmeta.get(span.offsetFor(kvindex) + VALSTART);
      final int vallen = kvmeta.get(span.offsetFor(kvindex) + VALLEN);
      final byte[] buf = kvbuffer.array();
      final int off = kvbuffer.arrayOffset();
      value.reset(buf, off + valstart, vallen);
      return value;
    }

    public boolean next() {
      // caveat: since we use this as a comparable in the merger 
      if(kvindex == maxindex) return false;
      kvindex += 1;
      if(kvindex % 100 == 0) {
        progress.set(1 - ((maxindex - kvindex) / (float) maxindex));
      }
      return true;
    }

    @Override
    public boolean hasNext() {
      return (kvindex == maxindex);
    }

    public void close() {
    }

    public Progress getProgress() { 
      return progress;
    }

    @Override
    public boolean isSameKey() throws IOException {
      return false;
    }

    public int getPartition() {
      final int partition = kvmeta.get(span.offsetFor(kvindex) + PARTITION);
      return partition;
    }

    public Integer peekPartition() {
      if (!hasNext()) {
        return null;
      } else {
          return kvmeta.get(span.offsetFor(kvindex + 1) + PARTITION);
      }
    }

    @SuppressWarnings("unused")
    public int size() {
      return (maxindex - kvindex);
    }

    public int compareTo(SpanIterator other) {
      return span.compareInternal(other.getKey(), other.getPartition(), kvindex);
    }
    
    @Override
    public String toString() {
      return String.format("SpanIterator<%d:%d> (span=%s)", kvindex, maxindex, span.toString());
    }

    /**
     * bisect returns the next insertion point for a given raw key, skipping keys
     * which are <= needle using a binary search instead of a linear comparison.
     * This is massively efficient when long strings of identical keys occur.
     * @param needle 
     * @param needlePart
     * @return
     */
    int bisect(DataInputBuffer needle, int needlePart) {
      int start = kvindex;
      int end = maxindex-1;
      int mid = start;
      int cmp = 0;

      if(end - start < minrun) {
        return 0;
      }

      if(span.compareInternal(needle, needlePart, start) > 0) {
        return kvindex;
      }
      
      // bail out early if we haven't got a min run 
      if(span.compareInternal(needle, needlePart, start+minrun) > 0) {
        return 0;
      }

      if(span.compareInternal(needle, needlePart, end) < 0) {
        return end - kvindex;
      }
      
      boolean found = false;
      
      // we sort 100k items, the max it can do is 20 loops, but break early
      for(int i = 0; start < end && i < 16; i++) {
        mid = start + (end - start)/2;
        cmp = span.compareInternal(needle, needlePart, mid);
        if(cmp == 0) {
          start = mid;
          found = true;
        } else if(cmp < 0) {
          start = mid; 
          found = true;
        }
        if(cmp > 0) {
          end = mid;
        }
      }

      if(found) {
        return start - kvindex;
      }
      return 0;
    }
  }

  private static class SortTask extends CallableWithNdc<SpanIterator> {
    private final SortSpan sortable;
    private final IndexedSorter sorter;

    public SortTask(SortSpan sortable, IndexedSorter sorter) {
        this.sortable = sortable;
        this.sorter = sorter;
    }

    @Override
    protected SpanIterator callInternal() {
      return sortable.sort(sorter);
    }
  }

  private class PartitionFilter implements TezRawKeyValueIterator {
    private final PartitionedRawKeyValueIterator iter;
    private int partition;
    private boolean dirty = false;
    public PartitionFilter(PartitionedRawKeyValueIterator iter) {
      this.iter = iter;
    }
    public DataInputBuffer getKey() throws IOException { return iter.getKey(); }
    public DataInputBuffer getValue() throws IOException { return iter.getValue(); }
    public void close() throws IOException { }
    public Progress getProgress() {
      return new Progress();
    }

    @Override
    public boolean isSameKey() throws IOException {
      return iter.isSameKey();
    }

    public boolean next() throws IOException {
      if(dirty || iter.next()) { 
        int prefix = iter.getPartition();

        if((prefix >>> (32 - partitionBits)) == partition) {
          dirty = false; // we found what we were looking for, good
          return true;
        } else if(!dirty) {
          dirty = true; // we did a lookahead and failed to find partition
        }
      }
      return false;
    }

    @Override
    public boolean hasNext() throws IOException {
      if (dirty || iter.hasNext()) {
        Integer part;
        if (dirty) {
          part = iter.getPartition();
        } else {
          part = iter.peekPartition();
        }

        if (part != null) {
          return (part >>> (32 - partitionBits)) == partition;
        }
      }
      return false;
    }

    public void reset(int partition) {
      this.partition = partition;
    }

    @SuppressWarnings("unused")
    public int getPartition() {
      return this.partition;
    }
  }

  private static class SpanHeap extends java.util.PriorityQueue<SpanIterator> {
    private static final long serialVersionUID = 1L;

    public SpanHeap() {
      super(256);
    }
    /**
     * {@link PriorityQueue}.poll() by a different name 
     * @return
     */
    public SpanIterator pop() {
      return this.poll();
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public boolean needsRLE() {
    return merger.needsRLE();
  }

  private final class SpanMerger implements PartitionedRawKeyValueIterator {
    InputByteBuffer key = new InputByteBuffer();
    InputByteBuffer value = new InputByteBuffer();
    int partition;

    private ArrayList< Future<SpanIterator>> futures = new ArrayList< Future<SpanIterator>>();

    private SpanHeap heap = new SpanHeap();
    private PartitionFilter partIter;

    private int gallop = 0;
    private SpanIterator horse;
    private long total = 0;
    private long eq = 0;
    
    public SpanMerger() {
      // SpanIterators are comparable
      partIter = new PartitionFilter(this);
    }

    public final void add(SpanIterator iter) {
      if(iter.next()) {
        heap.add(iter);
      }
    }

    public final void add(Future<SpanIterator> iter) {
      this.futures.add(iter);
    }

    public final boolean ready() throws IOException, InterruptedException {
      int numSpanItr = futures.size();
      try {
        SpanIterator iter = null;
        while(this.futures.size() > 0) {
          Future<SpanIterator> futureIter = this.futures.remove(0);
          iter = futureIter.get();
          this.add(iter);
        }

        StringBuilder sb = new StringBuilder();
        if (heap.size() == 0) {
          return false;
        }
        for(SpanIterator sp: heap) {
            sb.append(sp.toString());
            sb.append(",");
            total += sp.span.length();
            eq += sp.span.getEq();
        }
        LOG.info(outputContext.getDestinationVertexName() + ": " + "Heap = " + sb.toString());
        return true;
      } catch(ExecutionException e) {
        LOG.error("Heap size={}, total={}, eq={}, partition={}, gallop={}, totalItr={},"
                + " futures.size={}, destVertexName={}",
            heap.size(), total, eq, partition, gallop, numSpanItr, futures.size(),
            outputContext.getDestinationVertexName(), e);
        throw new IOException(e);
      }
    }

    private SpanIterator pop() {
      if(gallop > 0) {
        gallop--;
        return horse;
      }
      SpanIterator current = heap.pop();
      SpanIterator next = heap.peek();
      if(next != null && current != null &&
        ((Object)horse) == ((Object)current)) {
        // TODO: a better threshold check than 1 key repeating
        gallop = current.bisect(next.getKey(), next.getPartition())-1;
      }
      horse = current;
      return current;
    }
    
    public boolean needsRLE() {
      return (eq > 0.1 * total);
    }

    @SuppressWarnings("unused")
    private SpanIterator peek() {
      if (gallop > 0) {
        return horse;
      }
      return heap.peek();
    }

    public final boolean next() {
      SpanIterator current = pop();

      if(current != null) {
        partition = current.getPartition();
        key.reset(current.getKey());
        value.reset(current.getValue());
        if(gallop <= 0) {
          // since all keys and values are references to the kvbuffer, no more deep copies
          this.add(current);
        } else {
          // galloping, no deep copies required anyway
          current.next();
        }
        return true;
      }
      return false;
    }

    @Override
    public boolean hasNext() {
      return peek() != null;
    }

    public Integer peekPartition() {
      if (!hasNext()) {
        return null;
      } else {
        SpanIterator peek = peek();
        return peek.getPartition();
      }
    }

    public DataInputBuffer getKey() { return key; }
    public DataInputBuffer getValue() { return value; }
    public int getPartition() { return partition; }

    public void close() throws IOException {
    }

    public Progress getProgress() {
      // TODO
      return new Progress();
    }

    @Override
    public boolean isSameKey() throws IOException {
      return false;
    }

    public TezRawKeyValueIterator filter(int partition) {
      partIter.reset(partition);
      return partIter;
    }

  }
}
