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
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.library.common.comparator.ProxyComparator;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings({"unchecked", "rawtypes"})
public class PipelinedSorter extends ExternalSorter {
  
  private static final Log LOG = LogFactory.getLog(PipelinedSorter.class);
  
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

  // spill accounting
  volatile Throwable sortSpillException = null;

  int numSpills = 0;
  private final int minSpillsForCombine;
  private final ProxyComparator hasher;
  // SortSpans  
  private SortSpan span;
  private ByteBuffer largeBuffer;
  // Merger
  private final SpanMerger merger; 
  private final ExecutorService sortmaster;

  private final ArrayList<TezSpillRecord> indexCacheList =
    new ArrayList<TezSpillRecord>();
  private int totalIndexCacheMemory;
  private int indexCacheMemoryLimit;

  // TODO Set additional countesr - total bytes written, spills etc.

  public PipelinedSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);
    
    partitionBits = bitcount(partitions)+1;
   
    //sanity checks
    final int sortmb = this.availableMemoryMb;
    indexCacheMemoryLimit = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES,
                                       TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES_DEFAULT);

    // buffers and accounting
    int maxMemUsage = sortmb << 20;
    maxMemUsage -= maxMemUsage % METASIZE;
    largeBuffer = ByteBuffer.allocate(maxMemUsage);
    LOG.info(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + " = " + sortmb);
    // TODO: configurable setting?
    span = new SortSpan(largeBuffer, 1024*1024, 16);
    merger = new SpanMerger(comparator);
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
      // sort in the same thread, do not wait for the thread pool
      merger.add(span.sort(sorter, comparator));
      spill();
      int items = 1024*1024;
      int perItem = 16;
      if(span.length() != 0) {
        items = span.length();
        perItem = span.kvbuffer.limit()/items;
        items = (largeBuffer.capacity())/(METASIZE+perItem);
        if(items > 1024*1024) {
            // our goal is to have 1M splits and sort early
            items = 1024*1024;
        }
      }      
      span = new SortSpan(largeBuffer, items, perItem);
    } else {
      // queue up the sort
      SortTask task = new SortTask(span, sorter, comparator);
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
      // try again
      this.collect(key, value, partition);
      return;
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
    if((valstart - keystart) > span.keymax) {
      span.keymax = (valstart - keystart);
    }
    if((valend - valstart) > span.valmax) {
      span.valmax = (valend - valstart);
    }
    mapOutputRecordCounter.increment(1);
    mapOutputByteCounter.increment(valend - keystart);
  }

  public void spill() throws IOException { 
    // create spill file
    final long size = largeBuffer.capacity() + 
      (partitions * APPROX_HEADER_LENGTH);
    final TezSpillRecord spillRec = new TezSpillRecord(partitions);
    final Path filename =
      mapOutputFile.getSpillFileForWrite(numSpills, size);    
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
      // TODO: cache
      spillRec.writeToFile(indexFilename, conf);
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
    Path finalOutputFile =
        mapOutputFile.getOutputFileForWrite(0); //TODO
    Path finalIndexFile =
        mapOutputFile.getOutputIndexFileForWrite(0); //TODO

    LOG.info("Starting flush of map output");
    span.end();
    merger.add(span.sort(sorter, comparator));
    spill();
    sortmaster.shutdown();

    largeBuffer = null;

    if(numSpills == 1) {
      // someday be able to pass this directly to shuffle
      // without writing to disk
      final Path filename =
          mapOutputFile.getSpillFile(0);
      Path indexFilename =
              mapOutputFile.getSpillIndexFile(0);
      sameVolRename(filename, mapOutputFile.getOutputFileForWriteInVolume(filename));
      sameVolRename(indexFilename, mapOutputFile.getOutputIndexFileForWriteInVolume(indexFilename));
      return;
    }
    
    //The output stream for the final single output file
    FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

    final TezSpillRecord spillRec = new TezSpillRecord(partitions);
    final ArrayList<TezSpillRecord> indexCacheList = new ArrayList<TezSpillRecord>();

    for(int i = 0; i < numSpills; i++) {
      // TODO: build this cache before
      Path indexFilename = mapOutputFile.getSpillIndexFile(i);
      TezSpillRecord spillIndex = new TezSpillRecord(indexFilename, conf);
      indexCacheList.add(spillIndex);
    }
    
    for (int parts = 0; parts < partitions; parts++) {
      //create the segments to be merged
      List<Segment> segmentList =
          new ArrayList<Segment>(numSpills);
      for(int i = 0; i < numSpills; i++) {
        Path spillFilename = mapOutputFile.getSpillFile(i);
        TezIndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

        Segment s =
            new Segment(conf, rfs, spillFilename, indexRecord.getStartOffset(),
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
      Path indexFilename = mapOutputFile.getSpillIndexFile(i);
      Path spillFilename = mapOutputFile.getSpillFile(i);
      rfs.delete(indexFilename,true);
      rfs.delete(spillFilename,true);
    }
  }

  public void close() { }

  private interface PartitionedRawKeyValueIterator extends TezRawKeyValueIterator {
    int getPartition();
  }

  private class BufferStreamWrapper extends OutputStream 
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

  protected class InputByteBuffer extends DataInputBuffer {
    private byte[] buffer = new byte[256]; 
    private ByteBuffer wrapped = ByteBuffer.wrap(buffer);
    private void resize(int length) {
      if(length > buffer.length) {
        buffer = new byte[length];
        wrapped = ByteBuffer.wrap(buffer);
      }
      wrapped.limit(length);
    }
    public void reset(ByteBuffer b, int start, int length) {
      resize(length);
      b.position(start);
      b.get(buffer, 0, length);
      super.reset(buffer, 0, length);
    }
    // clone-ish function
    public void reset(DataInputBuffer clone) {
      byte[] data = clone.getData();
      int start = clone.getPosition();
      int length = clone.getLength();
      resize(length);
      System.arraycopy(data, start, buffer, 0, length);
      super.reset(buffer, 0, length);
    }
  }

  private class SortSpan  implements IndexedSortable {
    final IntBuffer kvmeta;
    final ByteBuffer kvbuffer;
    final DataOutputStream out;    
    private RawComparator comparator; 
    final int imeta[] = new int[NMETA];
    final int jmeta[] = new int[NMETA];
    int keymax = 1;
    int valmax = 1;
    private int i,j;
    private byte[] ki;
    private byte[] kj;
    private int index = 0;
    private InputByteBuffer hay = new InputByteBuffer();
    private long eq = 0;

    public SortSpan(ByteBuffer source, int maxItems, int perItem) {
      int capacity = source.remaining(); 
      int metasize = METASIZE*maxItems;
      int dataSize = maxItems * perItem;
      if(capacity < (metasize+dataSize)) {
        // try to allocate less meta space, because we have sample data
        metasize = METASIZE*(capacity/(perItem+METASIZE));
      }
      ByteBuffer reserved = source.duplicate();
      reserved.mark();
      LOG.info("reserved.remaining() = "+reserved.remaining());
      LOG.info("reserved.size = "+metasize);
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
    }

    public SpanIterator sort(IndexedSorter sorter, RawComparator comparator) {
    	this.comparator = comparator;
      ki = new byte[keymax];
      kj = new byte[keymax];
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

      if(i == mi || j == mj) i = -1;
      if(i == mi || j == mj) j = -1;
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
      
      final int istart = kvmeta.get(kvi + KEYSTART);
      final int jstart = kvmeta.get(kvj + KEYSTART);
      final int ilen   = kvmeta.get(kvi + VALSTART) - istart;
      final int jlen   = kvmeta.get(kvj + VALSTART) - jstart;

      kvbuffer.position(istart);
      kvbuffer.get(ki, 0, ilen);
      kvbuffer.position(jstart);
      kvbuffer.get(kj, 0, jlen);
      // sort by key
      final int cmp = comparator.compare(ki, 0, ilen, kj, 0, jlen);
      if(cmp == 0) eq++;
      return cmp;
    }

    public SortSpan next() {
      ByteBuffer remaining = end();
      if(remaining != null) {
        int items = length();
        int perItem = kvbuffer.position()/items;
        SortSpan newSpan = new SortSpan(remaining, items, perItem);
        newSpan.index = index+1;
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
        return null;
      }
      return remaining;
    }

    private int compareInternal(DataInputBuffer needle, int needlePart, int index) {
      int cmp = 0;
      int keystart;
      int valstart;
      int partition;
      partition = kvmeta.get(span.offsetFor(index) + PARTITION);
      if(partition != needlePart) {
          cmp = (partition-needlePart);
      } else {
        keystart = kvmeta.get(span.offsetFor(index) + KEYSTART);
        valstart = kvmeta.get(span.offsetFor(index) + VALSTART);
        // hay is allocated ahead of time
        hay.reset(kvbuffer, keystart, valstart - keystart);
        cmp = comparator.compare(hay.getData(), 
            hay.getPosition(), hay.getLength(),
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

  private class SpanIterator implements PartitionedRawKeyValueIterator, Comparable<SpanIterator> {
    private int kvindex = -1;
    private int maxindex;
    private IntBuffer kvmeta;
    private ByteBuffer kvbuffer;
    private SortSpan span;
    private InputByteBuffer key = new InputByteBuffer();
    private InputByteBuffer value = new InputByteBuffer();
    private Progress progress = new Progress();

    private final int minrun = (1 << 4);

    public SpanIterator(SortSpan span) {
      this.kvmeta = span.kvmeta;
      this.kvbuffer = span.kvbuffer;
      this.span = span;
      this.maxindex = (kvmeta.limit()/NMETA) - 1;
    }

    public DataInputBuffer getKey() throws IOException {
      final int keystart = kvmeta.get(span.offsetFor(kvindex) + KEYSTART);
      final int valstart = kvmeta.get(span.offsetFor(kvindex) + VALSTART);
      key.reset(kvbuffer, keystart, valstart - keystart);
      return key;
    }

    public DataInputBuffer getValue() throws IOException {
      final int valstart = kvmeta.get(span.offsetFor(kvindex) + VALSTART);
      final int vallen = kvmeta.get(span.offsetFor(kvindex) + VALLEN);
      value.reset(kvbuffer, valstart, vallen);
      return value;
    }

    public boolean next() throws IOException {
      // caveat: since we use this as a comparable in the merger 
      if(kvindex == maxindex) return false;
      if(kvindex % 100 == 0) {
          progress.set((kvindex-maxindex) / maxindex);
      }
      kvindex += 1;
      return true;
    }

    public void close() throws IOException {
    }

    public Progress getProgress() { 
      return progress;
    }

    public int getPartition() {
      final int partition = kvmeta.get(span.offsetFor(kvindex) + PARTITION);
      return partition;
    }

    public int size() {
      return (maxindex - kvindex);
    }

    public int compareTo(SpanIterator other) {
      try {
        return span.compareInternal(other.getKey(), other.getPartition(), kvindex);
      } catch(IOException ie) {
        // since we're not reading off disk, how could getKey() throw exceptions?
      }
      return -1;
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

  private class SortTask implements Callable<SpanIterator> {
    private final SortSpan sortable;
    private final IndexedSorter sorter;
    private final RawComparator comparator;
    
    public SortTask(SortSpan sortable, 
              IndexedSorter sorter, RawComparator comparator) {
        this.sortable = sortable;
        this.sorter = sorter;
        this.comparator = comparator;
    }

    public SpanIterator call() {
      return sortable.sort(sorter, comparator);
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

    public int getPartition() {
      return this.partition;
    }
  }

  private class SpanHeap extends java.util.PriorityQueue<SpanIterator> {
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

  private class SpanMerger implements PartitionedRawKeyValueIterator {
    private final RawComparator comparator;
    InputByteBuffer key = new InputByteBuffer();
    InputByteBuffer value = new InputByteBuffer();
    int partition;

    private ArrayList< Future<SpanIterator>> futures = new ArrayList< Future<SpanIterator>>();

    private SpanHeap heap = new SpanHeap();
    private PartitionFilter partIter;

    private int gallop = 0;
    private SpanIterator horse;
    private long total = 0;
    private long count = 0;
    private long eq = 0;
    
    public SpanMerger(RawComparator comparator) {
      this.comparator = comparator;
      partIter = new PartitionFilter(this);
    }

    public void add(SpanIterator iter) throws IOException{
      if(iter.next()) {
        heap.add(iter);
      }
    }

    public void add(Future<SpanIterator> iter) throws IOException{
      this.futures.add(iter);
    }

    public boolean ready() throws IOException, InterruptedException {
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

    private SpanIterator pop() throws IOException {
      if(gallop > 0) {
        gallop--;
        return horse;
      }
      SpanIterator current = heap.pop();
      SpanIterator next = heap.peek();
      if(next != null && current != null &&
        ((Object)horse) == ((Object)current)) {
        // TODO: a better threshold check
        gallop = current.bisect(next.getKey(), next.getPartition())-1;
      }
      horse = current;
      return current;
    }
    
    public boolean needsRLE() {
      return (eq > 0.1 * total);
    }
    
    private SpanIterator peek() throws IOException {
    	if(gallop > 0) {
            return horse;
        }
    	return heap.peek();
    }

    public boolean next() throws IOException {
      SpanIterator current = pop();

      if(current != null) {
        // keep local copies, since add() will move it all out
        key.reset(current.getKey());
        value.reset(current.getValue());
        partition = current.getPartition();
        if(gallop <= 0) {
          this.add(current);
        } else {
          // galloping
          current.next();
        }
        return true;
      }
      return false;
    }

    public DataInputBuffer getKey() throws IOException { return key; }
    public DataInputBuffer getValue() throws IOException { return value; }
    public int getPartition() { return partition; }

    public void close() throws IOException {
    }

    public Progress getProgress() {
      // TODO
      return new Progress();
    }

    public TezRawKeyValueIterator filter(int partition) {
      partIter.reset(partition);
      return partIter;
    }

  }
}
