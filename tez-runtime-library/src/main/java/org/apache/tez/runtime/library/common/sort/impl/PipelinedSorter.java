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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.library.common.comparator.ProxyComparator;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
  //Maintain a bunch of ByteBuffers (each of them can hold approximately 2 GB data)
  @VisibleForTesting
  protected final LinkedList<ByteBuffer> bufferList = new LinkedList<ByteBuffer>();
  private ListIterator<ByteBuffer> listIterator;

  //total memory capacity allocated to sorter
  private final long capacity;

  //track buffer overflow recursively in all buffers
  private int bufferOverflowRecursion;

  private final int blockSize;


  // Merger
  private final SpanMerger merger; 
  private final ExecutorService sortmaster;

  private final ArrayList<TezSpillRecord> indexCacheList =
    new ArrayList<TezSpillRecord>();
  private int totalIndexCacheMemory;
  private int indexCacheMemoryLimit;

  private final boolean pipelinedShuffle;
  private final boolean finalMergeEnabled;
  private final boolean sendEmptyPartitionDetails;

  // TODO Set additional countesr - total bytes written, spills etc.

  public PipelinedSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    this(outputContext,conf,numOutputs, initialMemoryAvailable, 0);
  }

  PipelinedSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable, int blkSize) throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);
    
    partitionBits = bitcount(partitions)+1;

    finalMergeEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT_DEFAULT);

    boolean confPipelinedShuffle = this.conf.getBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);

    sendEmptyPartitionDetails = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);


    pipelinedShuffle = !finalMergeEnabled && confPipelinedShuffle;

    //sanity checks
    final long sortmb = this.availableMemoryMb;
    indexCacheMemoryLimit = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES,
                                       TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES_DEFAULT);

    // buffers and accounting
    long maxMemUsage = sortmb << 20;

    this.blockSize = computeBlockSize(blkSize, maxMemUsage);

    long usage = sortmb << 20;
    //Divide total memory into different blocks.
    int numberOfBlocks = Math.max(1, (int) Math.ceil(1.0 * usage / blockSize));
    LOG.info("Number of Blocks : " + numberOfBlocks
        + ", maxMemUsage=" + maxMemUsage + ", BLOCK_SIZE=" + blockSize + ", finalMergeEnabled="
        + finalMergeEnabled + ", pipelinedShuffle=" + pipelinedShuffle + ", "
        + "sendEmptyPartitionDetails=" + sendEmptyPartitionDetails);
    long totalCapacityWithoutMeta = 0;
    for (int i = 0; i < numberOfBlocks; i++) {
      Preconditions.checkArgument(usage > 0, "usage can't be less than zero " + usage);
      long size = Math.min(usage, blockSize);
      int sizeWithoutMeta = (int) ((size) - (size % METASIZE));
      bufferList.add(ByteBuffer.allocate(sizeWithoutMeta));
      totalCapacityWithoutMeta += sizeWithoutMeta;
      usage -= size;
    }
    capacity = totalCapacityWithoutMeta;
    listIterator = bufferList.listIterator();


    LOG.info(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + " = " + sortmb);
    Preconditions.checkArgument(listIterator.hasNext(), "Buffer list seems to be empty " + bufferList.size());
    span = new SortSpan(listIterator.next(), 1024*1024, 16, this.comparator);
    merger = new SpanMerger(); // SpanIterators are comparable
    final int sortThreads = 
            this.conf.getInt(
                TezRuntimeConfiguration.TEZ_RUNTIME_SORT_THREADS, 
                TezRuntimeConfiguration.TEZ_RUNTIME_SORT_THREADS_DEFAULT);
    sortmaster = Executors.newFixedThreadPool(sortThreads,
        new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Sorter [" + TezUtilsInternal
            .cleanVertexName(outputContext.getDestinationVertexName()) + "] #%d")
        .build());

    // k/v serialization    
    if(comparator instanceof ProxyComparator) {
      hasher = (ProxyComparator)comparator;
      LOG.info("Using the HashComparator");
    } else {
      hasher = null;
    }    
    valSerializer.open(span.out);
    keySerializer.open(span.out);
    minSpillsForCombine = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS, 3);
  }

  @VisibleForTesting
  static int computeBlockSize(int blkSize, long maxMemUsage) {
    if (blkSize == 0) {
      return (int) Math.min(maxMemUsage, Integer.MAX_VALUE);
    } else {
      Preconditions.checkArgument(blkSize > 0, "blkSize should be between 1 and Integer.MAX_VALUE");
      if (blkSize >= maxMemUsage) {
        return (maxMemUsage > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) maxMemUsage;
      } else {
        return blkSize;
      }
    }
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
      Stopwatch stopWatch = new Stopwatch();
      stopWatch.start();
      // sort in the same thread, do not wait for the thread pool
      merger.add(span.sort(sorter));
      spill();
      stopWatch.stop();
      LOG.info("Time taken for spill " + (stopWatch.elapsedMillis()) + " ms");
      if (pipelinedShuffle) {
        List<Event> events = Lists.newLinkedList();
        String pathComponent = (outputContext.getUniqueIdentifier() + "_" + (numSpills-1));
        ShuffleUtils.generateEventOnSpill(events, finalMergeEnabled, false, outputContext,
            (numSpills - 1), indexCacheList.get(numSpills - 1), partitions, sendEmptyPartitionDetails,
            pathComponent);
        outputContext.sendEvents(events);
        LOG.info("Adding spill event for spill (final update=false), spillId=" + (numSpills - 1));
      }
      //safe to reset the iterator
      listIterator = bufferList.listIterator();
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
      Preconditions.checkArgument(listIterator.hasNext(), "block iterator should not be empty");
      span = new SortSpan((ByteBuffer)listIterator.next().clear(), (1024*1024), perItem, this.comparator);
    } else {
      // queue up the sort
      SortTask task = new SortTask(span, sorter);
      Future<SpanIterator> future = sortmaster.submit(task);
      merger.add(future);
      span = newSpan;
    }
    valSerializer.open(span.out);
    keySerializer.open(span.out);
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
    if(span.kvmeta.remaining() < METASIZE) {
      this.sort();
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

      bufferOverflowRecursion++;
      if (bufferOverflowRecursion > bufferList.size()) {
        throw new MapBufferTooSmallException("Record too large for in-memory buffer. Exceeded "
            + "buffer overflow limit, bufferOverflowRecursion=" + bufferOverflowRecursion + ", bufferList"
            + ".size=" + bufferList.size() + ", blockSize=" + blockSize);
      }
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
    mapOutputByteCounter.increment(valend - keystart);
  }

  public void spill() throws IOException { 
    // create spill file
    final long size = capacity +
        + (partitions * APPROX_HEADER_LENGTH);
    final TezSpillRecord spillRec = new TezSpillRecord(partitions);
    final Path filename =
      mapOutputFile.getSpillFileForWrite(numSpills, size);
    spillFilePaths.put(numSpills, filename);
    FSDataOutputStream out = rfs.create(filename, true, 4096);

    try {
      merger.ready(); // wait for all the future results from sort threads
      LOG.info("Spilling to " + filename.toString());
      for (int i = 0; i < partitions; ++i) {
        TezRawKeyValueIterator kvIter = merger.filter(i);
        //write merged output to disk
        long segmentStart = out.getPos();
        Writer writer =
          new Writer(conf, out, keyClass, valClass, codec,
              spilledRecordsCounter, null, merger.needsRLE());
        if (combiner == null) {
          while(kvIter.next()) {
            writer.append(kvIter.getKey(), kvIter.getValue());
          }
        } else {          
          runCombineProcessor(kvIter, writer);
        }
        //close
        writer.close();

        // record offsets
        final TezIndexRecord rec = 
            new TezIndexRecord(
                segmentStart,
                writer.getRawLength(),
                writer.getCompressedLength());
        spillRec.putIndex(rec, i);
      }

      Path indexFilename =
        mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
            * MAP_OUTPUT_INDEX_RECORD_LENGTH);
      spillFileIndexPaths.put(numSpills, indexFilename);
      spillRec.writeToFile(indexFilename, conf);
      //TODO: honor cache limits
      indexCacheList.add(spillRec);
      ++numSpills;
    } catch(InterruptedException ie) {
      // TODO:the combiner has been interrupted
    } finally {
      out.close();
    }
  }

  @Override
  public void flush() throws IOException {
    final String uniqueIdentifier = outputContext.getUniqueIdentifier();

    LOG.info("Starting flush of map output");
    span.end();
    merger.add(span.sort(sorter));
    spill();
    sortmaster.shutdown();

    //safe to clean up
    bufferList.clear();

    numAdditionalSpills.increment(numSpills - 1);

    if (!finalMergeEnabled) {
      //Generate events for all spills
      List<Event> events = Lists.newLinkedList();

      //For pipelined shuffle, previous events are already sent. Just generate the last event alone
      int startIndex = (pipelinedShuffle) ? (numSpills - 1) : 0;
      int endIndex = numSpills;

      for (int i = startIndex; i < endIndex; i++) {
        boolean isLastEvent = (i == numSpills - 1);

        String pathComponent = (outputContext.getUniqueIdentifier() + "_" + i);
        ShuffleUtils.generateEventOnSpill(events, finalMergeEnabled, isLastEvent,
            outputContext, i, indexCacheList.get(i), partitions,
            sendEmptyPartitionDetails, pathComponent);
        LOG.info("Adding spill event for spill (final update=" + isLastEvent + "), spillId=" + i);
      }
      outputContext.sendEvents(events);
      //No need to generate final merge
      return;
    }

    //In case final merge is required, the following code path is executed.
    if(numSpills == 1) {
      // someday be able to pass this directly to shuffle
      // without writing to disk
      final Path filename = spillFilePaths.get(0);
      final Path indexFilename = spillFileIndexPaths.get(0);
      finalOutputFile = mapOutputFile.getOutputFileForWriteInVolume(filename);
      finalIndexFile = mapOutputFile.getOutputIndexFileForWriteInVolume(indexFilename);

      sameVolRename(filename, finalOutputFile);
      sameVolRename(indexFilename, finalIndexFile);
      if (LOG.isInfoEnabled()) {
        LOG.info("numSpills=" + numSpills + ", finalOutputFile=" + finalOutputFile + ", "
            + "finalIndexFile=" + finalIndexFile + ", filename=" + filename + ", indexFilename=" +
            indexFilename);
      }
      return;
    }

    finalOutputFile =
        mapOutputFile.getOutputFileForWrite(0); //TODO
    finalIndexFile =
        mapOutputFile.getOutputIndexFileForWrite(0); //TODO

    if (LOG.isDebugEnabled()) {
      LOG.debug("numSpills: " + numSpills + ", finalOutputFile:" + finalOutputFile + ", finalIndexFile:"
              + finalIndexFile);
    }

    //The output stream for the final single output file
    FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

    final TezSpillRecord spillRec = new TezSpillRecord(partitions);


    for (int parts = 0; parts < partitions; parts++) {
      //create the segments to be merged
      List<Segment> segmentList =
          new ArrayList<Segment>(numSpills);
      for(int i = 0; i < numSpills; i++) {
        Path spillFilename = spillFilePaths.get(i);
        TezIndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

        Segment s =
            new Segment(rfs, spillFilename, indexRecord.getStartOffset(),
                             indexRecord.getPartLength(), codec, ifileReadAhead,
                             ifileReadAheadLength, ifileBufferSize, true);
        segmentList.add(i, s);
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
                     (RawComparator)ConfigUtils.getIntermediateOutputKeyComparator(conf), 
                     nullProgressable, sortSegments, true,
                     null, spilledRecordsCounter, null,
                     null); // Not using any Progress in TezMerger. Should just work.

      //write merged output to disk
      long segmentStart = finalOut.getPos();
      Writer writer =
          new Writer(conf, finalOut, keyClass, valClass, codec,
                           spilledRecordsCounter, null, merger.needsRLE());
      if (combiner == null || numSpills < minSpillsForCombine) {
        TezMerger.writeFile(kvIter, writer, nullProgressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
      } else {
        runCombineProcessor(kvIter, writer);
      }

      //close
      writer.close();

      // record offsets
      final TezIndexRecord rec = 
          new TezIndexRecord(
              segmentStart, 
              writer.getRawLength(), 
              writer.getCompressedLength());
      spillRec.putIndex(rec, parts);
    }

    spillRec.writeToFile(finalIndexFile, conf);
    finalOut.close();
    for(int i = 0; i < numSpills; i++) {
      Path indexFilename = spillFileIndexPaths.get(i);
      Path spillFilename = spillFilePaths.get(i);
      rfs.delete(indexFilename,true);
      rfs.delete(spillFilename,true);
    }

    spillFileIndexPaths.clear();
    spillFilePaths.clear();
  }


  private interface PartitionedRawKeyValueIterator extends TezRawKeyValueIterator {
    int getPartition();
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

  private final class SortSpan  implements IndexedSortable {
    final IntBuffer kvmeta;
    final ByteBuffer kvbuffer;
    final DataOutputStream out;
    final RawComparator comparator;
    final int imeta[] = new int[NMETA];
    final int jmeta[] = new int[NMETA];

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
      LOG.info("reserved.remaining() = " + reserved.remaining());
      LOG.info("reserved.size = "+ metasize);
      reserved.position(metasize);
      kvbuffer = reserved.slice();
      reserved.flip();
      reserved.limit(metasize);
      kvmeta = reserved
                .slice()
                .order(ByteOrder.nativeOrder())
               .asIntBuffer();
      out = new DataOutputStream(
              new BufferStreamWrapper(kvbuffer));
      this.comparator = comparator;
    }

    public SpanIterator sort(IndexedSorter sorter) {
      long start = System.currentTimeMillis();
      if(length() > 1) {
        sorter.sort(this, 0, length(), nullProgressable);
      }
      LOG.info("done sorting span=" + index + ", length=" + length() + ", "
          + "time=" + (System.currentTimeMillis() - start));
      return new SpanIterator(this);
    }

    int offsetFor(int i) {
      return (i * NMETA);
    }

    public void swap(final int mi, final int mj) {
      final int kvi = offsetFor(mi);
      final int kvj = offsetFor(mj);

      kvmeta.position(kvi); kvmeta.get(imeta);
      kvmeta.position(kvj); kvmeta.get(jmeta);
      kvmeta.position(kvj); kvmeta.put(imeta);
      kvmeta.position(kvi); kvmeta.put(jmeta);
    }

    private int compareKeys(final int kvi, final int kvj) {
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
        newSpan = new SortSpan(remaining, items, perItem, this.comparator);
        newSpan.index = index+1;
        LOG.info(String.format("New Span%d.length = %d, perItem = %d", newSpan.index, newSpan
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
      LOG.info(String.format("Span%d.length = %d, perItem = %d", index, length(), perItem));
      if(remaining.remaining() < METASIZE+perItem) {
        //Check if we can get the next Buffer from the main buffer list
        if (listIterator.hasNext()) {
          LOG.info("Getting memory from next block in the list, recordsWritten=" +
              mapOutputRecordCounter.getValue());
          reinit = true;
          return listIterator.next();
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
            needle.getPosition(), needle.getLength());
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
    private final Progress progress = new Progress();

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
      if(kvindex % 100 == 0) {
          progress.set((kvindex-maxindex) / (float)maxindex);
      }
      kvindex += 1;
      return true;
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
      try {
        SpanIterator iter = null;
        while(this.futures.size() > 0) {
          Future<SpanIterator> futureIter = this.futures.remove(0);
          iter = futureIter.get();
          this.add(iter);
        }
        
        StringBuilder sb = new StringBuilder();
        for(SpanIterator sp: heap) {
            sb.append(sp.toString());
            sb.append(",");
            total += sp.span.length();
            eq += sp.span.getEq();
        }
        LOG.info("Heap = " + sb.toString());
        return true;
      } catch(Exception e) {
        LOG.info(e.toString());
        return false;
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
