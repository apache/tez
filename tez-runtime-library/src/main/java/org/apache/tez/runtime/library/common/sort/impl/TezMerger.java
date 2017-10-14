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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader.KeyState;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.utils.BufferUtils;
import org.apache.tez.runtime.library.utils.LocalProgress;

/**
 * Merger is an utility class used by the Map and Reduce tasks for merging
 * both their memory and disk segments
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings({"unchecked", "rawtypes"})
public class TezMerger {
  private static final Logger LOG = LoggerFactory.getLogger(TezMerger.class);

  
  // Local directories
  private static LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

  public static
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class keyClass, Class valueClass, 
                            CompressionCodec codec, boolean ifileReadAhead,
                            int ifileReadAheadLength, int ifileBufferSize,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator comparator, Progressable reporter,
                            TezCounter readsCounter,
                            TezCounter writesCounter,
                            TezCounter bytesReadCounter,
                            Progress mergePhase)
      throws IOException, InterruptedException {
    return 
      new MergeQueue(conf, fs, inputs, deleteInputs, codec, ifileReadAhead,
                           ifileReadAheadLength, ifileBufferSize, false, comparator, 
                           reporter, null).merge(keyClass, valueClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter,
                                           bytesReadCounter,
                                           mergePhase);
  }

  // Used by the in-memory merger.
  public static
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs, 
                            Class keyClass, Class valueClass, 
                            List<Segment> segments, 
                            int mergeFactor, Path tmpDir,
                            RawComparator comparator, Progressable reporter,
                            TezCounter readsCounter,
                            TezCounter writesCounter,
                            TezCounter bytesReadCounter,
                            Progress mergePhase)
      throws IOException, InterruptedException {
    // Get rid of this ?
    return merge(conf, fs, keyClass, valueClass, segments, mergeFactor, tmpDir,
                 comparator, reporter, false, readsCounter, writesCounter, bytesReadCounter,
                 mergePhase);
  }

  public static <K extends Object, V extends Object>
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class keyClass, Class valueClass,
                            List<Segment> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator comparator, Progressable reporter,
                            boolean sortSegments,
                            TezCounter readsCounter,
                            TezCounter writesCounter,
                            TezCounter bytesReadCounter,
                            Progress mergePhase)
      throws IOException, InterruptedException {
    return new MergeQueue(conf, fs, segments, comparator, reporter,
                           sortSegments, false).merge(keyClass, valueClass,
                                               mergeFactor, tmpDir,
                                               readsCounter, writesCounter,
                                               bytesReadCounter, mergePhase);
  }

  public static <K extends Object, V extends Object>
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
      Class keyClass, Class valueClass,
      CompressionCodec codec,
      List<Segment> segments,
      int mergeFactor, Path tmpDir,
      RawComparator comparator, Progressable reporter,
      boolean sortSegments,
      boolean considerFinalMergeForProgress,
      TezCounter readsCounter,
      TezCounter writesCounter,
      TezCounter bytesReadCounter,
      Progress mergePhase, boolean checkForSameKeys)
      throws IOException, InterruptedException {
    return new MergeQueue(conf, fs, segments, comparator, reporter,
        sortSegments, codec, considerFinalMergeForProgress, checkForSameKeys).
        merge(keyClass, valueClass,
            mergeFactor, tmpDir,
            readsCounter, writesCounter,
            bytesReadCounter,
            mergePhase);
  }

  public static <K extends Object, V extends Object>
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class keyClass, Class valueClass,
                            CompressionCodec codec,
                            List<Segment> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator comparator, Progressable reporter,
                            boolean sortSegments,
                            boolean considerFinalMergeForProgress,
                            TezCounter readsCounter,
                            TezCounter writesCounter,
                            TezCounter bytesReadCounter,
                            Progress mergePhase)
      throws IOException, InterruptedException {
    return new MergeQueue(conf, fs, segments, comparator, reporter,
                           sortSegments, codec, considerFinalMergeForProgress).
                                         merge(keyClass, valueClass,
                                             mergeFactor, tmpDir,
                                             readsCounter, writesCounter,
                                             bytesReadCounter,
                                             mergePhase);
  }

  public static <K extends Object, V extends Object>
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
                          Class keyClass, Class valueClass,
                          CompressionCodec codec,
                          List<Segment> segments,
                          int mergeFactor, int inMemSegments, Path tmpDir,
                          RawComparator comparator, Progressable reporter,
                          boolean sortSegments,
                          TezCounter readsCounter,
                          TezCounter writesCounter,
                          TezCounter bytesReadCounter,
                          Progress mergePhase)
      throws IOException, InterruptedException {
  return new MergeQueue(conf, fs, segments, comparator, reporter,
                         sortSegments, codec, false).merge(keyClass, valueClass,
                                             mergeFactor, inMemSegments,
                                             tmpDir,
                                             readsCounter, writesCounter,
                                             bytesReadCounter,
                                             mergePhase);
}

  public static <K extends Object, V extends Object>
  void writeFile(TezRawKeyValueIterator records, Writer writer,
      Progressable progressable, long recordsBeforeProgress)
      throws IOException, InterruptedException {
    long recordCtr = 0;
    long count = 0;
    while(records.next()) {
      if (records.isSameKey()) {
        writer.append(IFile.REPEAT_KEY, records.getValue());
        count++;
      } else {
        writer.append(records.getKey(), records.getValue());
      }
      
      if (((recordCtr++) % recordsBeforeProgress) == 0) {
        progressable.progress();
        if (Thread.currentThread().isInterrupted()) {
          /**
           * Takes care DefaultSorter.mergeParts, MergeManager's merger threads,
           * PipelinedSorter's flush(). This is not expensive check as it is carried out every
           * 10000 records or so.
           */
          throw new InterruptedException("Current thread=" + Thread.currentThread().getName() + " got "
              + "interrupted");
        }
      }
    }
    if ((count > 0) && LOG.isTraceEnabled()) {
      LOG.trace("writeFile SAME_KEY count=" + count);
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static class KeyValueBuffer {
    private byte[] buf;
    private int position;
    private int length;

    public KeyValueBuffer(byte buf[], int position, int length) {
      reset(buf, position, length);
    }

    public void reset(byte[] input, int position, int length) {
      this.buf = input;
      this.position = position;
      this.length = length;
    }

    public byte[] getData() {
      return buf;
    }

    public int getPosition() {
      return position;
    }

    public int getLength() {
      return length;
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Segment {
    static final byte[] EMPTY_BYTES = new byte[0];
    Reader reader = null;
    final KeyValueBuffer key = new KeyValueBuffer(EMPTY_BYTES, 0, 0);
    TezCounter mapOutputsCounter = null;

    public Segment(Reader reader, TezCounter mapOutputsCounter) {
      this.reader = reader;
      this.mapOutputsCounter = mapOutputsCounter;
    }

    void init(TezCounter readsCounter, TezCounter bytesReadCounter) throws IOException {
      if (mapOutputsCounter != null) {
        mapOutputsCounter.increment(1);
      }
    }

    boolean inMemory() {
      return true;
    }

    KeyValueBuffer getKey() { return key; }

    DataInputBuffer getValue(DataInputBuffer value) throws IOException {
      nextRawValue(value);
      return value;
    }

    public long getLength() {
      return reader.getLength();
    }

    KeyState readRawKey(DataInputBuffer nextKey) throws IOException {
      KeyState keyState = reader.readRawKey(nextKey);
      key.reset(nextKey.getData(), nextKey.getPosition(), nextKey.getLength() - nextKey.getPosition());
      return keyState;
    }

    boolean nextRawKey(DataInputBuffer nextKey) throws IOException {
      boolean hasNext = reader.nextRawKey(nextKey);
      key.reset(nextKey.getData(), nextKey.getPosition(), nextKey.getLength() - nextKey.getPosition());
      return hasNext;
    }

    void nextRawValue(DataInputBuffer value) throws IOException {
      reader.nextRawValue(value);
    }

    void closeReader() throws IOException {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }

    void close() throws IOException {
      closeReader();
    }

    public long getPosition() throws IOException {
      return reader.getPosition();
    }

    // This method is used by BackupStore to extract the
    // absolute position after a reset
    long getActualPosition() throws IOException {
      return reader.getPosition();
    }

    Reader getReader() {
      return reader;
    }

    // This method is used by BackupStore to reinitialize the
    // reader to start reading from a different segment offset
    void reinitReader(int offset) throws IOException {
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class DiskSegment extends Segment {

    FileSystem fs = null;
    Path file = null;
    boolean preserve = false; // Signifies whether the segment should be kept after a merge is complete. Checked in the close method.
    CompressionCodec codec = null;
    long segmentOffset = 0;
    long segmentLength = -1;
    boolean ifileReadAhead;
    int ifileReadAheadLength;
    int bufferSize = -1;

    public DiskSegment(FileSystem fs, Path file,
        CompressionCodec codec, boolean ifileReadAhead,
        int ifileReadAheadLength, int bufferSize, boolean preserve)
    throws IOException {
      this(fs, file, codec, ifileReadAhead, ifileReadAheadLength,
          bufferSize, preserve, null);
    }

    public DiskSegment(FileSystem fs, Path file,
                   CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLenth,
                   int bufferSize, boolean preserve, TezCounter mergedMapOutputsCounter)
  throws IOException {
      this(fs, file, 0, fs.getFileStatus(file).getLen(), codec,
          ifileReadAhead, ifileReadAheadLenth, bufferSize, preserve,
          mergedMapOutputsCounter);
    }

    public DiskSegment(FileSystem fs, Path file,
                   long segmentOffset, long segmentLength,
                   CompressionCodec codec, boolean ifileReadAhead,
                   int ifileReadAheadLength,  int bufferSize, 
                   boolean preserve) throws IOException {
      this(fs, file, segmentOffset, segmentLength, codec, ifileReadAhead,
          ifileReadAheadLength, bufferSize, preserve, null);
    }

    public DiskSegment(FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean ifileReadAhead, int ifileReadAheadLength, int bufferSize,
        boolean preserve, TezCounter mergedMapOutputsCounter)
    throws IOException {
      super(null, mergedMapOutputsCounter);
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;
      this.ifileReadAhead = ifileReadAhead;
      this.ifileReadAheadLength =ifileReadAheadLength;
      this.bufferSize = bufferSize;

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;
    }

    @Override
    void init(TezCounter readsCounter, TezCounter bytesReadCounter) throws IOException {
      super.init(readsCounter, bytesReadCounter);
      FSDataInputStream in = fs.open(file);
      in.seek(segmentOffset);
      reader = new Reader(in, segmentLength, codec, readsCounter, bytesReadCounter, ifileReadAhead,
          ifileReadAheadLength, bufferSize);
    }

    @Override
    boolean inMemory() {
      return false;
    }

    @Override
    public long getLength() {
      return (reader == null) ?
        segmentLength : reader.getLength();
    }

    @Override
    void close() throws IOException {
      super.close();
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }
    // This method is used by BackupStore to extract the
    // absolute position after a reset
    @Override
    long getActualPosition() throws IOException {
      return segmentOffset + reader.getPosition();
    }

    // This method is used by BackupStore to reinitialize the
    // reader to start reading from a different segment offset
    @Override
    void reinitReader(int offset) throws IOException {
      if (!inMemory()) {
        closeReader();
        segmentOffset = offset;
        segmentLength = fs.getFileStatus(file).getLen() - segmentOffset;
        init(null, null);
      }
    }
  }

  @VisibleForTesting
  static class MergeQueue<K extends Object, V extends Object>
  extends PriorityQueue<Segment> implements TezRawKeyValueIterator {
    final Configuration conf;
    final FileSystem fs;
    final CompressionCodec codec;
    final boolean checkForSameKeys;
    static final boolean ifileReadAhead = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT;
    static final int ifileReadAheadLength = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT;
    static final int ifileBufferSize = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT;
    static final long recordsBeforeProgress = TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT;
    
    List<Segment> segments = new ArrayList<Segment>();
    
    final RawComparator comparator;

    private long totalBytesProcessed;
    private float progPerByte;
    private Progress mergeProgress = new LocalProgress();
    // Boolean variable for including/considering final merge as part of sort
    // phase or not. This is true in map task, false in reduce task. It is
    // used in calculating mergeProgress.
    private final boolean considerFinalMergeForProgress;

    final Progressable reporter;
    
    final DataInputBuffer key = new DataInputBuffer();
    final DataInputBuffer value = new DataInputBuffer();
    final DataInputBuffer nextKey = new DataInputBuffer();
    final DataInputBuffer diskIFileValue = new DataInputBuffer();
    
    Segment minSegment;
    Comparator<Segment> segmentComparator =   
      new Comparator<Segment>() {
      public int compare(Segment o1, Segment o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }

        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    };

    KeyState hasNext;
    DataOutputBuffer prevKey = new DataOutputBuffer();

    public MergeQueue(Configuration conf, FileSystem fs, 
                      Path[] inputs, boolean deleteInputs,
                      CompressionCodec codec, boolean ifileReadAhead,
                      int ifileReadAheadLength, int ifileBufferSize,
                      boolean considerFinalMergeForProgress,
                      RawComparator comparator, Progressable reporter, 
                      TezCounter mergedMapOutputsCounter) 
    throws IOException {
      this.conf = conf;
      this.checkForSameKeys = true;
      // this.recordsBeforeProgress =
      // conf.getLong(TezJobConfig.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS,
      // TezJobConfig.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
      this.fs = fs;
      this.codec = codec;
      this.comparator = comparator;
      this.reporter = reporter;
      this.considerFinalMergeForProgress = considerFinalMergeForProgress;
      
      for (Path file : inputs) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("MergeQ: adding: " + file);
        }
        segments.add(new DiskSegment(fs, file, codec, ifileReadAhead,
                                      ifileReadAheadLength, ifileBufferSize,
                                      !deleteInputs, 
                                       (file.toString().endsWith(
                                           Constants.MERGED_OUTPUT_PREFIX) ? 
                                        null : mergedMapOutputsCounter)));
      }
      
      // Sort segments on file-lengths
      Collections.sort(segments, segmentComparator); 
    }
    
    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment> segments, RawComparator comparator,
        Progressable reporter, boolean sortSegments, boolean considerFinalMergeForProgress) {
      this(conf, fs, segments, comparator, reporter, sortSegments, null,
          considerFinalMergeForProgress);
    }

    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment> segments, RawComparator comparator,
        Progressable reporter, boolean sortSegments, CompressionCodec codec,
        boolean considerFinalMergeForProgress) {
      this(conf, fs, segments, comparator, reporter, sortSegments, null,
          considerFinalMergeForProgress, true);
    }

    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment> segments, RawComparator comparator,
        Progressable reporter, boolean sortSegments, CompressionCodec codec,
        boolean considerFinalMergeForProgress, boolean checkForSameKeys) {
      this.conf = conf;
      this.fs = fs;
      this.comparator = comparator;
      this.segments = segments;
      this.reporter = reporter;
      this.considerFinalMergeForProgress = considerFinalMergeForProgress;
      if (sortSegments) {
        Collections.sort(segments, segmentComparator);
      }
      this.checkForSameKeys = checkForSameKeys;
      this.codec = codec;
    }

    public void close() throws IOException {
      Segment segment;
      while((segment = pop()) != null) {
        segment.close();
      }
    }

    public DataInputBuffer getKey() throws IOException {
      return key;
    }

    public DataInputBuffer getValue() throws IOException {
      return value;
    }

    private void populatePreviousKey() throws IOException {
      key.reset();
      BufferUtils.copy(key, prevKey);
    }

    private void adjustPriorityQueue(Segment reader) throws IOException{
      long startPos = reader.getPosition();
      if (checkForSameKeys) {
        if (hasNext == null) {
          /**
           * hasNext can be null during first iteration & prevKey is initialized here.
           * In cases of NO_KEY/NEW_KEY, we readjust the queue later. If new segment/file is found
           * during this process, we need to compare keys for RLE across segment boundaries.
           * prevKey can't be empty at that time (e.g custom comparators)
           */
          populatePreviousKey();
        } else {
          //indicates a key has been read already
          if (hasNext != KeyState.SAME_KEY) {
            /**
             * Store previous key before reading next for later key comparisons.
             * If all keys in a segment are unique, it would always hit this code path and key copies
             * are wasteful in such condition, as these comparisons are mainly done for RLE.
             * TODO: When better stats are available, this condition can be avoided.
             */
            populatePreviousKey();
          }
        }
      }
      hasNext = reader.readRawKey(nextKey);
      long endPos = reader.getPosition();
      totalBytesProcessed += endPos - startPos;
      mergeProgress.set(totalBytesProcessed * progPerByte);
      if (hasNext == KeyState.NEW_KEY) {
        adjustTop();
        compareKeyWithNextTopKey(reader);
      } else if(hasNext == KeyState.NO_KEY) {
        pop();
        reader.close();
        compareKeyWithNextTopKey(null);
      } else if(hasNext == KeyState.SAME_KEY) {
        // do not rebalance the priority queue
      }
    }

    /**
     * Check if the previous key is same as the next top segment's key.
     * This would be useful to compute whether same key is spread across multiple segments.
     *
     * @param current
     * @throws IOException
     */
    void compareKeyWithNextTopKey(Segment current) throws IOException {
      Segment nextTop = top();
      if (checkForSameKeys && nextTop != current) {
        //we have a different file. Compare it with previous key
        KeyValueBuffer nextKey = nextTop.getKey();
        int compare = compare(nextKey, prevKey);
        if (compare == 0) {
          //Same key is available in the next segment.
          hasNext = KeyState.SAME_KEY;
        }
      }
    }

    public boolean next() throws IOException {
      if (!hasNext()) {
        return false;
      }

      minSegment = top();
      long startPos = minSegment.getPosition();
      KeyValueBuffer nextKey = minSegment.getKey();
      key.reset(nextKey.getData(), nextKey.getPosition(), nextKey.getLength());
      if (!minSegment.inMemory()) {
        //When we load the value from an inmemory segment, we reset
        //the "value" DIB in this class to the inmem segment's byte[].
        //When we load the value bytes from disk, we shouldn't use
        //the same byte[] since it would corrupt the data in the inmem
        //segment. So we maintain an explicit DIB for value bytes
        //obtained from disk, and if the current segment is a disk
        //segment, we reset the "value" DIB to the byte[] in that (so 
        //we reuse the disk segment DIB whenever we consider
        //a disk segment).
        minSegment.getValue(diskIFileValue);
        value.reset(diskIFileValue.getData(), diskIFileValue.getLength());
      } else {
        minSegment.getValue(value);
      }
      long endPos = minSegment.getPosition();
      totalBytesProcessed += endPos - startPos;
      mergeProgress.set(totalBytesProcessed * progPerByte);

      return true;
    }

    int compare(KeyValueBuffer nextKey, DataOutputBuffer buf2) {
      byte[] b1 = nextKey.getData();
      byte[] b2 = buf2.getData();
      int s1 = nextKey.getPosition();
      int s2 = 0;
      int l1 = nextKey.getLength();
      int l2 = buf2.getLength();
      return comparator.compare(b1, s1, l1, b2, s2, l2);
    }

    protected boolean lessThan(Object a, Object b) {
      KeyValueBuffer key1 = ((Segment)a).getKey();
      KeyValueBuffer key2 = ((Segment)b).getKey();
      int s1 = key1.getPosition();
      int l1 = key1.getLength();
      int s2 = key2.getPosition();
      int l2 = key2.getLength();;

      return comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2) < 0;
    }
    
    public TezRawKeyValueIterator merge(Class keyClass, Class valueClass,
                                     int factor, Path tmpDir,
                                     TezCounter readsCounter,
                                     TezCounter writesCounter,
                                     TezCounter bytesReadCounter,
                                     Progress mergePhase)
        throws IOException, InterruptedException {
      return merge(keyClass, valueClass, factor, 0, tmpDir,
                   readsCounter, writesCounter, bytesReadCounter, mergePhase);
    }

    TezRawKeyValueIterator merge(Class keyClass, Class valueClass,
                                     int factor, int inMem, Path tmpDir,
                                     TezCounter readsCounter,
                                     TezCounter writesCounter,
                                     TezCounter bytesReadCounter,
                                     Progress mergePhase)
        throws IOException, InterruptedException {
      if (segments.size() == 0) {
        LOG.info("Nothing to merge. Returning an empty iterator");
        return new EmptyIterator();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Merging " + segments.size() + " sorted segments");
      }

      /*
       * If there are inMemory segments, then they come first in the segments
       * list and then the sorted disk segments. Otherwise(if there are only
       * disk segments), then they are sorted segments if there are more than
       * factor segments in the segments list.
       */
      int numSegments = segments.size();
      int origFactor = factor;
      int passNo = 1;
      if (mergePhase != null) {
        mergeProgress = mergePhase;
      }

      long totalBytes = computeBytesInMerges(segments, factor, inMem, considerFinalMergeForProgress);
      if (totalBytes != 0) {
        progPerByte = 1.0f / (float)totalBytes;
      }
      
      //create the MergeStreams from the sorted map created in the constructor
      //and dump the final output to a file
      do {
        //get the factor for this pass of merge. We assume in-memory segments
        //are the first entries in the segment list and that the pass factor
        //doesn't apply to them
        factor = getPassFactor(factor, passNo, numSegments - inMem);
        if (1 == passNo) {
          factor += inMem;
        }
        List<Segment> segmentsToMerge =
          new ArrayList<Segment>();
        int segmentsConsidered = 0;
        int numSegmentsToConsider = factor;
        long startBytes = 0; // starting bytes of segments of this merge
        while (true) {
          //extract the smallest 'factor' number of segments  
          //Call cleanup on the empty segments (no key/value data)
          List<Segment> mStream = 
            getSegmentDescriptors(numSegmentsToConsider);
          for (Segment segment : mStream) {
            // Initialize the segment at the last possible moment;
            // this helps in ensuring we don't use buffers until we need them

            segment.init(readsCounter, bytesReadCounter);
            long startPos = segment.getPosition();
            boolean hasNext = segment.nextRawKey(nextKey);
            long endPos = segment.getPosition();
            
            if (hasNext) {
              startBytes += endPos - startPos;
              segmentsToMerge.add(segment);
              segmentsConsidered++;
            }
            else { // Empty segments. Can this be avoided altogether ?
              segment.close();
              numSegments--; //we ignore this segment for the merge
            }
          }
          //if we have the desired number of segments
          //or looked at all available segments, we break
          if (segmentsConsidered == factor || 
              segments.size() == 0) {
            break;
          }

          // Get the correct # of segments in case some of them were empty.
          numSegmentsToConsider = factor - segmentsConsidered;
        }
        
        //feed the streams to the priority queue
        initialize(segmentsToMerge.size());
        clear();
        for (Segment segment : segmentsToMerge) {
          put(segment);
        }
        
        //if we have lesser number of segments remaining, then just return the
        //iterator, else do another single level merge
        if (numSegments <= factor) { // Will always kick in if only in-mem segments are provided.
          if (!considerFinalMergeForProgress) { // for reduce task

            // Reset totalBytesProcessed and recalculate totalBytes from the
            // remaining segments to track the progress of the final merge.
            // Final merge is considered as the progress of the reducePhase,
            // the 3rd phase of reduce task.
            totalBytesProcessed = 0;
            totalBytes = 0;
            for (int i = 0; i < segmentsToMerge.size(); i++) {
              totalBytes += segmentsToMerge.get(i).getLength();
            }
          }
          if (totalBytes != 0) //being paranoid
            progPerByte = 1.0f / (float)totalBytes;
          
          totalBytesProcessed += startBytes;
          if (totalBytes != 0)
            mergeProgress.set(totalBytesProcessed * progPerByte);
          else
            mergeProgress.set(1.0f); // Last pass and no segments left - we're done

          if (LOG.isDebugEnabled()) {
            LOG.debug("Down to the last merge-pass, with " + numSegments +
                " segments left of total size: " +
                (totalBytes - totalBytesProcessed) + " bytes");
          }
          // At this point, Factor Segments have not been physically
          // materialized. The merge will be done dynamically. Some of them may
          // be in-memory segments, other on-disk semgnets. Decision to be made
          // by a finalMerge is that is required.
          return this;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Merging " + segmentsToMerge.size() +
                " intermediate segments out of a total of " +
                (segments.size() + segmentsToMerge.size()));
          }
          
          long bytesProcessedInPrevMerges = totalBytesProcessed;
          totalBytesProcessed += startBytes;

          //we want to spread the creation of temp files on multiple disks if 
          //available under the space constraints
          long approxOutputSize = 0; 
          for (Segment s : segmentsToMerge) {
            approxOutputSize += s.getLength() + 
                                ChecksumFileSystem.getApproxChkSumLength(
                                s.getLength());
          }
          Path tmpFilename = 
            new Path(tmpDir, "intermediate").suffix("." + passNo);

          Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                              tmpFilename.toString(),
                                              approxOutputSize, conf);

          // TODO Would it ever make sense to make this an in-memory writer ?
          // Merging because of too many disk segments - might fit in memory.
          Writer writer = 
            new Writer(conf, fs, outputFile, keyClass, valueClass, codec,
                             writesCounter, null);

          writeFile(this, writer, reporter, recordsBeforeProgress);
          writer.close();
          
          //we finished one single level merge; now clean up the priority 
          //queue
          this.close();

          // Add the newly create segment to the list of segments to be merged
          Segment tempSegment = 
            new DiskSegment(fs, outputFile, codec, ifileReadAhead,
                ifileReadAheadLength, ifileBufferSize, false);

          // Insert new merged segment into the sorted list
          int pos = Collections.binarySearch(segments, tempSegment,
                                             segmentComparator);
          if (pos < 0) {
            // binary search failed. So position to be inserted at is -pos-1
            pos = -pos-1;
          }
          segments.add(pos, tempSegment);
          numSegments = segments.size();
          
          // Subtract the difference between expected size of new segment and 
          // actual size of new segment(Expected size of new segment is
          // inputBytesOfThisMerge) from totalBytes. Expected size and actual
          // size will match(almost) if combiner is not called in merge.
          long inputBytesOfThisMerge = totalBytesProcessed -
                                       bytesProcessedInPrevMerges;
          totalBytes -= inputBytesOfThisMerge - tempSegment.getLength();
          if (totalBytes != 0) {
            progPerByte = 1.0f / (float)totalBytes;
          }
          
          passNo++;
        }
        //we are worried about only the first pass merge factor. So reset the 
        //factor to what it originally was
        factor = origFactor;
      } while(true);
    }
    
    /**
     * Determine the number of segments to merge in a given pass. Assuming more
     * than factor segments, the first pass should attempt to bring the total
     * number of segments - 1 to be divisible by the factor - 1 (each pass
     * takes X segments and produces 1) to minimize the number of merges.
     */
    private static int getPassFactor(int factor, int passNo, int numSegments) {
      // passNo > 1 in the OR list - is that correct ?
      if (passNo > 1 || numSegments <= factor || factor == 1) 
        return factor;
      int mod = (numSegments - 1) % (factor - 1);
      if (mod == 0)
        return factor;
      return mod + 1;
    }
    
    /** Return (& remove) the requested number of segment descriptors from the
     * sorted map.
     */
    private List<Segment> getSegmentDescriptors(int numDescriptors) {
      if (numDescriptors > segments.size()) {
        List<Segment> subList = new ArrayList<Segment>(segments);
        segments.clear();
        return subList;
      }

      // Efficiently bulk remove segments
      List<Segment> subList = segments.subList(0, numDescriptors);
      List<Segment> subListCopy = new ArrayList<>(subList);
      subList.clear();
      return subListCopy;
    }
    
    /**
     * Compute expected size of input bytes to merges, will be used in
     * calculating mergeProgress. This simulates the above merge() method and
     * tries to obtain the number of bytes that are going to be merged in all
     * merges(assuming that there is no combiner called while merging).
     * @param segments segments to compute merge bytes
     * @param factor mapreduce.task.io.sort.factor
     * @param inMem  number of segments in memory to be merged
     * @param considerFinalMergeForProgress whether to consider for final merge
     */
    static long computeBytesInMerges(List<Segment> segments, int factor, int inMem, boolean considerFinalMergeForProgress) {
      int numSegments = segments.size();
      long[] segmentSizes = new long[numSegments];
      long totalBytes = 0;
      int n = numSegments - inMem;
      // factor for 1st pass
      int f = getPassFactor(factor, 1, n) + inMem;
      n = numSegments;
 
      for (int i = 0; i < numSegments; i++) {
        // Not handling empty segments here assuming that it would not affect
        // much in calculation of mergeProgress.
        segmentSizes[i] = segments.get(i).getLength();
      }
      
      // If includeFinalMerge is true, allow the following while loop iterate
      // for 1 more iteration. This is to include final merge as part of the
      // computation of expected input bytes of merges
      boolean considerFinalMerge = considerFinalMergeForProgress;

      int offset = 0;
      while (n > f || considerFinalMerge) {
        if (n <= f) {
          considerFinalMerge = false;
        }
        long mergedSize = 0;
        f = Math.min(f, n);
        for (int j = 0; j < f; j++) {
          mergedSize += segmentSizes[offset + j];
        }
        totalBytes += mergedSize;
        
        // insert new size into the sorted list
        int pos = Arrays.binarySearch(segmentSizes, offset, offset + n, mergedSize);
        if (pos < 0) {
          pos = -pos-1;
        }
        if (pos < offset + f) {
          // Insert at the beginning
          offset += f - 1;
          segmentSizes[offset] = mergedSize;
        } else if (pos < offset + n) {
          // Insert in the middle
          if (offset + n < segmentSizes.length) {
            // Shift right after insertion point into unused capacity
            System.arraycopy(segmentSizes, pos, segmentSizes, pos + 1, offset + n - pos);
            // Insert into insertion point
            segmentSizes[pos] = mergedSize;
            offset += f;
          } else {
            // Full left shift before insertion point
            System.arraycopy(segmentSizes, offset + f, segmentSizes, 0, pos - (offset + f));
            // Insert in the middle
            segmentSizes[pos - (offset + f)] = mergedSize;
            // Full left shift after insertion point
            System.arraycopy(segmentSizes, pos, segmentSizes, pos - (offset + f) + 1, offset + n - pos);
            offset = 0;
          }
        } else {
          // Insert at the end
          if (pos < segmentSizes.length) {
            // Append into unused capacity
            segmentSizes[pos] = mergedSize;
            offset += f;
          } else {
            // Full left shift
            // Append at the end
            System.arraycopy(segmentSizes, offset + f, segmentSizes, 0, n - f);
            segmentSizes[n - f] = mergedSize;
            offset = 0;
          }
        }
        n -=  f - 1;
        f = factor;
      }

      return totalBytes;
    }

    public Progress getProgress() {
      return mergeProgress;
    }

    @Override
    public boolean isSameKey() throws IOException {
      return (hasNext != null) && (hasNext == KeyState.SAME_KEY);
    }

    public boolean hasNext() throws IOException {
      if (size() == 0)
        return false;

      if (minSegment != null) {
        //minSegment is non-null for all invocations of next except the first
        //one. For the first invocation, the priority queue is ready for use
        //but for the subsequent invocations, first adjust the queue
        adjustPriorityQueue(minSegment);
        if (size() == 0) {
          minSegment = null;
          return false;
        }
      }

      return true;
    }

  }

  private static class EmptyIterator implements TezRawKeyValueIterator {
    final Progress progress = new Progress();

    EmptyIterator() {
      progress.set(1.0f);
    }

    @Override
    public DataInputBuffer getKey() throws IOException {
      throw new RuntimeException("No keys on an empty iterator");
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      throw new RuntimeException("No values on an empty iterator");
    }

    @Override
    public boolean next() throws IOException {
      return false;
    }

    @Override
    public boolean hasNext() throws IOException {
      return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Progress getProgress() {
      return progress;
    }

    @Override
    public boolean isSameKey() throws IOException {
      throw new UnsupportedOperationException("isSameKey is not supported");
    }
  }
}
