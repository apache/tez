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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;


/**
 * Merger is an utility class used by the Map and Reduce tasks for merging
 * both their memory and disk segments
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings({"unchecked", "rawtypes"})
public class TezMerger {  
  private static final Log LOG = LogFactory.getLog(TezMerger.class);

  
  // Local directories
  private static LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator(TezJobConfig.LOCAL_DIRS);

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
  throws IOException {
    return 
      new MergeQueue(conf, fs, inputs, deleteInputs, codec, ifileReadAhead,
                           ifileReadAheadLength, ifileBufferSize, false, comparator, 
                           reporter, null).merge(keyClass, valueClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter,
                                           bytesReadCounter,
                                           mergePhase);
  }

  public static 
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class keyClass, Class valueClass, 
                            CompressionCodec codec, boolean ifileReadAhead,
                            int ifileReadAheadLength, int ifileBufferSize,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator comparator,
                            Progressable reporter,
                            TezCounter readsCounter,
                            TezCounter writesCounter,
                            TezCounter mergedMapOutputsCounter,
                            TezCounter bytesReadCounter,
                            Progress mergePhase)
  throws IOException {
    return 
      new MergeQueue(conf, fs, inputs, deleteInputs, codec, ifileReadAhead,
                           ifileReadAheadLength, ifileBufferSize, false, comparator, 
                           reporter, mergedMapOutputsCounter).merge(
                                           keyClass, valueClass,
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
      throws IOException {
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
      throws IOException {
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
                            Progress mergePhase)
      throws IOException {
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
    throws IOException {
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
  throws IOException {
    long recordCtr = 0;
    while(records.next()) {
      writer.append(records.getKey(), records.getValue());
      
      if (((recordCtr++) % recordsBeforeProgress) == 0) {
        progressable.progress();
      }
    }
}

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Segment<K extends Object, V extends Object> {
    Reader reader = null;
    final DataInputBuffer key = new DataInputBuffer();
    
    Configuration conf = null;
    FileSystem fs = null;
    Path file = null;
    boolean preserve = false; // Signifies whether the segment should be kept after a merge is complete. Checked in the close method.
    CompressionCodec codec = null;
    long segmentOffset = 0;
    long segmentLength = -1;
    boolean ifileReadAhead;
    int ifileReadAheadLength;
    int bufferSize = -1;
    
    TezCounter mapOutputsCounter = null;

    public Segment(Configuration conf, FileSystem fs, Path file,
        CompressionCodec codec, boolean ifileReadAhead,
        int ifileReadAheadLength, int bufferSize, boolean preserve)
    throws IOException {
      this(conf, fs, file, codec, ifileReadAhead, ifileReadAheadLength,
          bufferSize, preserve, null);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLenth,
                   int bufferSize, boolean preserve, TezCounter mergedMapOutputsCounter)
  throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec,
          ifileReadAhead, ifileReadAheadLenth, bufferSize, preserve,
          mergedMapOutputsCounter);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
                   long segmentOffset, long segmentLength,
                   CompressionCodec codec, boolean ifileReadAhead,
                   int ifileReadAheadLength,  int bufferSize, 
                   boolean preserve) throws IOException {
      this(conf, fs, file, segmentOffset, segmentLength, codec, ifileReadAhead,
          ifileReadAheadLength, bufferSize, preserve, null);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean ifileReadAhead, int ifileReadAheadLength, int bufferSize,
        boolean preserve, TezCounter mergedMapOutputsCounter)
    throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;
      this.ifileReadAhead = ifileReadAhead;
      this.ifileReadAheadLength =ifileReadAheadLength;
      this.bufferSize = bufferSize;

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;
      
      this.mapOutputsCounter = mergedMapOutputsCounter;
    }
    
    public Segment(Reader reader, boolean preserve) {
      this(reader, preserve, null);
    }
    
    public Segment(Reader reader, boolean preserve, 
                   TezCounter mapOutputsCounter) {
      this.reader = reader;
      this.preserve = preserve;
      
      this.segmentLength = reader.getLength();
      
      this.mapOutputsCounter = mapOutputsCounter;
    }

    void init(TezCounter readsCounter, TezCounter byetsReadCounter) throws IOException {      
      if (reader == null) { 
        FSDataInputStream in = fs.open(file);
        in.seek(segmentOffset);
        reader = new Reader(in, segmentLength, codec, readsCounter, byetsReadCounter,
            ifileReadAhead, ifileReadAheadLength, bufferSize);
      }
      if (mapOutputsCounter != null) {
        mapOutputsCounter.increment(1);
      }
    }
    
    boolean inMemory() {
      return fs == null;
    }
    
    DataInputBuffer getKey() { return key; }

    DataInputBuffer getValue(DataInputBuffer value) throws IOException {
      nextRawValue(value);
      return value;
    }

    public long getLength() { 
      return (reader == null) ?
        segmentLength : reader.getLength();
    }
    
    boolean nextRawKey() throws IOException {
      return reader.nextRawKey(key);
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
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }

    public long getPosition() throws IOException {
      return reader.getPosition();
    }

    // This method is used by BackupStore to extract the 
    // absolute position after a reset
    long getActualPosition() throws IOException {
      return segmentOffset + reader.getPosition();
    }

    Reader getReader() {
      return reader;
    }
    
    // This method is used by BackupStore to reinitialize the
    // reader to start reading from a different segment offset
    void reinitReader(int offset) throws IOException {
      if (!inMemory()) {
        closeReader();
        segmentOffset = offset;
        segmentLength = fs.getFileStatus(file).getLen() - segmentOffset;
        init(null, null);
      }
    }
  }

  private static class MergeQueue<K extends Object, V extends Object> 
  extends PriorityQueue<Segment> implements TezRawKeyValueIterator {
    Configuration conf;
    FileSystem fs;
    CompressionCodec codec;
    boolean ifileReadAhead = TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT;
    int ifileReadAheadLength = TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT;
    int ifileBufferSize = TezJobConfig.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT;
    long recordsBeforeProgress = TezJobConfig.DEFAULT_RECORDS_BEFORE_PROGRESS;
    
    List<Segment> segments = new ArrayList<Segment>();
    
    RawComparator comparator;

    private long totalBytesProcessed;
    private float progPerByte;
    private Progress mergeProgress = new Progress();
    // Boolean variable for including/considering final merge as part of sort
    // phase or not. This is true in map task, false in reduce task. It is
    // used in calculating mergeProgress.
    private final boolean considerFinalMergeForProgress;

    Progressable reporter;
    
    DataInputBuffer key;
    final DataInputBuffer value = new DataInputBuffer();
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

    public MergeQueue(Configuration conf, FileSystem fs, 
                      Path[] inputs, boolean deleteInputs, 
                      CompressionCodec codec, boolean ifileReadAhead,
                      int ifileReadAheadLength, int ifileBufferSize,
                      boolean considerFinalMergeForProgress,
                      RawComparator comparator, Progressable reporter, 
                      TezCounter mergedMapOutputsCounter) 
    throws IOException {
      this.conf = conf;
      // this.recordsBeforeProgress =
      // conf.getLong(TezJobConfig.RECORDS_BEFORE_PROGRESS,
      // TezJobConfig.DEFAULT_RECORDS_BEFORE_PROGRESS);
      this.fs = fs;
      this.codec = codec;
      this.comparator = comparator;
      this.reporter = reporter;
      this.considerFinalMergeForProgress = considerFinalMergeForProgress;
      
      for (Path file : inputs) {
        LOG.debug("MergeQ: adding: " + file);
        segments.add(new Segment(conf, fs, file, codec, ifileReadAhead,
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
      this.conf = conf;
      this.fs = fs;
      this.comparator = comparator;
      this.segments = segments;
      this.reporter = reporter;
      this.considerFinalMergeForProgress = considerFinalMergeForProgress;
      if (sortSegments) {
        Collections.sort(segments, segmentComparator);
      }
    }

    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment> segments, RawComparator comparator,
        Progressable reporter, boolean sortSegments, CompressionCodec codec,
        boolean considerFinalMergeForProgress) {
      this(conf, fs, segments, comparator, reporter, sortSegments, considerFinalMergeForProgress);
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

    private void adjustPriorityQueue(Segment reader) throws IOException{
      long startPos = reader.getPosition();
      boolean hasNext = reader.nextRawKey();
      long endPos = reader.getPosition();
      totalBytesProcessed += endPos - startPos;
      mergeProgress.set(totalBytesProcessed * progPerByte);
      if (hasNext) {
        adjustTop();
      } else {
        pop();
        reader.close();
      }
    }

    public boolean next() throws IOException {
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
      minSegment = top();
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
        value.reset(diskIFileValue.getData(), diskIFileValue.getLength());
      }
      long startPos = minSegment.getPosition();
      key = minSegment.getKey();
      minSegment.getValue(value);
      long endPos = minSegment.getPosition();
      totalBytesProcessed += endPos - startPos;
      mergeProgress.set(totalBytesProcessed * progPerByte);
      return true;
    }

    protected boolean lessThan(Object a, Object b) {
      DataInputBuffer key1 = ((Segment)a).getKey();
      DataInputBuffer key2 = ((Segment)b).getKey();
      int s1 = key1.getPosition();
      int l1 = key1.getLength() - s1;
      int s2 = key2.getPosition();
      int l2 = key2.getLength() - s2;

      return comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2) < 0;
    }
    
    public TezRawKeyValueIterator merge(Class keyClass, Class valueClass,
                                     int factor, Path tmpDir,
                                     TezCounter readsCounter,
                                     TezCounter writesCounter,
                                     TezCounter bytesReadCounter,
                                     Progress mergePhase)
        throws IOException {
      return merge(keyClass, valueClass, factor, 0, tmpDir,
                   readsCounter, writesCounter, bytesReadCounter, mergePhase);
    }

    TezRawKeyValueIterator merge(Class keyClass, Class valueClass,
                                     int factor, int inMem, Path tmpDir,
                                     TezCounter readsCounter,
                                     TezCounter writesCounter,
                                     TezCounter bytesReadCounter,
                                     Progress mergePhase)
        throws IOException {
      LOG.info("Merging " + segments.size() + " sorted segments");
      if (segments.size() == 0) {
        LOG.info("Nothing to merge. Returning an empty iterator");
        return new EmptyIterator();
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

      long totalBytes = computeBytesInMerges(factor, inMem);
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
            boolean hasNext = segment.nextRawKey();
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
          
          LOG.info("Down to the last merge-pass, with " + numSegments + 
                   " segments left of total size: " +
                   (totalBytes - totalBytesProcessed) + " bytes");
          // At this point, Factor Segments have not been physically
          // materialized. The merge will be done dynamically. Some of them may
          // be in-memory segments, other on-disk semgnets. Decision to be made
          // by a finalMerge is that is required.
          return this;
        } else {
          LOG.info("Merging " + segmentsToMerge.size() + 
                   " intermediate segments out of a total of " + 
                   (segments.size()+segmentsToMerge.size()));
          
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
            new Segment(conf, fs, outputFile, codec, ifileReadAhead,
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
    private int getPassFactor(int factor, int passNo, int numSegments) {
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
      
      List<Segment> subList = 
        new ArrayList<Segment>(segments.subList(0, numDescriptors));
      // TODO Replace this with a batch operation
      for (int i=0; i < numDescriptors; ++i) {
        segments.remove(0);
      }
      return subList;
    }
    
    /**
     * Compute expected size of input bytes to merges, will be used in
     * calculating mergeProgress. This simulates the above merge() method and
     * tries to obtain the number of bytes that are going to be merged in all
     * merges(assuming that there is no combiner called while merging).
     * @param factor mapreduce.task.io.sort.factor
     * @param inMem  number of segments in memory to be merged
     */
    long computeBytesInMerges(int factor, int inMem) {
      int numSegments = segments.size();
      List<Long> segmentSizes = new ArrayList<Long>(numSegments);
      long totalBytes = 0;
      int n = numSegments - inMem;
      // factor for 1st pass
      int f = getPassFactor(factor, 1, n) + inMem;
      n = numSegments;
 
      for (int i = 0; i < numSegments; i++) {
        // Not handling empty segments here assuming that it would not affect
        // much in calculation of mergeProgress.
        segmentSizes.add(segments.get(i).getLength());
      }
      
      // If includeFinalMerge is true, allow the following while loop iterate
      // for 1 more iteration. This is to include final merge as part of the
      // computation of expected input bytes of merges
      boolean considerFinalMerge = considerFinalMergeForProgress;
      
      while (n > f || considerFinalMerge) {
        if (n <=f ) {
          considerFinalMerge = false;
        }
        long mergedSize = 0;
        f = Math.min(f, segmentSizes.size());
        for (int j = 0; j < f; j++) {
          mergedSize += segmentSizes.remove(0);
        }
        totalBytes += mergedSize;
        
        // insert new size into the sorted list
        int pos = Collections.binarySearch(segmentSizes, mergedSize);
        if (pos < 0) {
          pos = -pos-1;
        }
        segmentSizes.add(pos, mergedSize);

        n -= (f-1);
        f = factor;
      }

      return totalBytes;
    }

    public Progress getProgress() {
      return mergeProgress;
    }

  }
  
  private static class EmptyIterator implements TezRawKeyValueIterator {
    final Progress progress;

    EmptyIterator() {
      progress = new Progress();
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
    public void close() throws IOException {
    }

    @Override
    public Progress getProgress() {
      return progress;
    }
  }
}
