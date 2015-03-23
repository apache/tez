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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.FileChunk;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.hadoop.compat.NullProgressable;


/**
 * Usage. Create instance. setInitialMemoryAvailable(long), configureAndStart()
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings(value={"rawtypes"})
public class MergeManager {
  
  private static final Logger LOG = LoggerFactory.getLogger(MergeManager.class);

  private final Configuration conf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  private final LocalDirAllocator localDirAllocator;
  
  private final  TezTaskOutputFiles mapOutputFile;
  private final Progressable nullProgressable = new NullProgressable();
  private final Combiner combiner;  
  
  private final Set<MapOutput> inMemoryMergedMapOutputs = 
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final IntermediateMemoryToMemoryMerger memToMemMerger;

  private final Set<MapOutput> inMemoryMapOutputs = 
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final InMemoryMerger inMemoryMerger;

  @VisibleForTesting
  final Set<FileChunk> onDiskMapOutputs = new TreeSet<FileChunk>();
  @VisibleForTesting
  final OnDiskMerger onDiskMerger;
  
  private final long memoryLimit;
  @VisibleForTesting
  final long postMergeMemLimit;
  private long usedMemory;
  private long commitMemory;
  private final int ioSortFactor;
  private final long maxSingleShuffleLimit;
  
  private final int memToMemMergeOutputsThreshold; 
  private final long mergeThreshold;
  
  private final long initialMemoryAvailable;

  private final ExceptionReporter exceptionReporter;
  
  private final InputContext inputContext;

  private final TezCounter spilledRecordsCounter;

  private final TezCounter reduceCombineInputCounter;

  private final TezCounter mergedMapOutputsCounter;
  
  private final TezCounter numMemToDiskMerges;
  private final TezCounter numDiskToDiskMerges;
  private final TezCounter additionalBytesWritten;
  private final TezCounter additionalBytesRead;
  
  private final CompressionCodec codec;
  
  private volatile boolean finalMergeComplete = false;
  
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final int ifileBufferSize;

  private AtomicInteger mergeFileSequenceId = new AtomicInteger(0);

  /**
   * Construct the MergeManager. Must call start before it becomes usable.
   */
  public MergeManager(Configuration conf,
                      FileSystem localFS,
                      LocalDirAllocator localDirAllocator,  
                      InputContext inputContext,
                      Combiner combiner,
                      TezCounter spilledRecordsCounter,
                      TezCounter reduceCombineInputCounter,
                      TezCounter mergedMapOutputsCounter,
                      ExceptionReporter exceptionReporter,
                      long initialMemoryAvailable,
                      CompressionCodec codec,
                      boolean ifileReadAheadEnabled,
                      int ifileReadAheadLength) {
    this.inputContext = inputContext;
    this.conf = conf;
    this.localDirAllocator = localDirAllocator;
    this.exceptionReporter = exceptionReporter;
    this.initialMemoryAvailable = initialMemoryAvailable;
    
    this.combiner = combiner;

    this.reduceCombineInputCounter = reduceCombineInputCounter;
    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;
    this.mapOutputFile = new TezTaskOutputFiles(conf, inputContext.getUniqueIdentifier());
    
    this.localFS = localFS;
    this.rfs = ((LocalFileSystem)localFS).getRaw();
    
    this.numDiskToDiskMerges = inputContext.getCounters().findCounter(TaskCounter.NUM_DISK_TO_DISK_MERGES);
    this.numMemToDiskMerges = inputContext.getCounters().findCounter(TaskCounter.NUM_MEM_TO_DISK_MERGES);
    this.additionalBytesWritten = inputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    this.additionalBytesRead = inputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);

    this.codec = codec;
    this.ifileReadAhead = ifileReadAheadEnabled;
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = ifileReadAheadLength;
    } else {
      this.ifileReadAheadLength = 0;
    }
    this.ifileBufferSize = conf.getInt("io.file.buffer.size",
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);
    
    // Figure out initial memory req start
    final float maxInMemCopyUse =
      conf.getFloat(
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for " +
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": " +
          maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    long memLimit = conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY, (long)(inputContext
        .getTotalMemoryAvailableToTask() * maxInMemCopyUse));

    float maxRedPer = conf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new TezUncheckedException(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT + maxRedPer);
    }

    long maxRedBuffer = (long) (inputContext.getTotalMemoryAvailableToTask() * maxRedPer);
    // Figure out initial memory req end
    
    if (this.initialMemoryAvailable < memLimit) {
      this.memoryLimit = this.initialMemoryAvailable;
    } else {
      this.memoryLimit = memLimit;
    }
    
    if (this.initialMemoryAvailable < maxRedBuffer) {
      this.postMergeMemLimit = this.initialMemoryAvailable;
    } else {
      this.postMergeMemLimit = maxRedBuffer;
    }
    
    LOG.info("InitialRequest: ShuffleMem=" + memLimit + ", postMergeMem=" + maxRedBuffer
        + ", RuntimeTotalAvailable=" + this.initialMemoryAvailable + ". Updated to: ShuffleMem="
        + this.memoryLimit + ", postMergeMem: " + this.postMergeMemLimit);

    this.ioSortFactor = 
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, 
            TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT);
    
    final float singleShuffleMemoryLimitPercent =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    //TODO: Cap it to MAX_VALUE until MapOutput starts supporting > 2 GB
    this.maxSingleShuffleLimit = 
        (long) Math.min((memoryLimit * singleShuffleMemoryLimitPercent), Integer.MAX_VALUE);
    this.memToMemMergeOutputsThreshold = 
            conf.getInt(
                TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, 
                ioSortFactor);
    this.mergeThreshold = 
        (long)(this.memoryLimit * 
               conf.getFloat(
                   TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 
                   TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT_DEFAULT));
    LOG.info("MergerManager: memoryLimit=" + memoryLimit + ", " +
             "maxSingleShuffleLimit=" + maxSingleShuffleLimit + ", " +
             "mergeThreshold=" + mergeThreshold + ", " + 
             "ioSortFactor=" + ioSortFactor + ", " +
             "memToMemMergeOutputsThreshold=" + memToMemMergeOutputsThreshold);
    
    if (this.maxSingleShuffleLimit >= this.mergeThreshold) {
      throw new RuntimeException("Invlaid configuration: "
          + "maxSingleShuffleLimit should be less than mergeThreshold"
          + "maxSingleShuffleLimit: " + this.maxSingleShuffleLimit
          + ", mergeThreshold: " + this.mergeThreshold);
    }
    
    boolean allowMemToMemMerge = 
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM, 
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM_DEFAULT);
      if (allowMemToMemMerge) {
        this.memToMemMerger = 
          new IntermediateMemoryToMemoryMerger(this,
                                               memToMemMergeOutputsThreshold);
      } else {
        this.memToMemMerger = null;
      }
      
      this.inMemoryMerger = new InMemoryMerger(this);
      
      this.onDiskMerger = new OnDiskMerger(this);
  }

  @Private
  void configureAndStart() {
    if (this.memToMemMerger != null) {
      memToMemMerger.start();
    }
    this.inMemoryMerger.start();
    this.onDiskMerger.start();
  }

  /**
   * Exposing this to get an initial memory ask without instantiating the object.
   */
  @Private
  static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    final float maxInMemCopyUse =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
      if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
        throw new IllegalArgumentException("Invalid value for " +
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": " +
            maxInMemCopyUse);
      }

      // Allow unit tests to fix Runtime memory
      long memLimit = conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
          (long)(maxAvailableTaskMemory * maxInMemCopyUse));
      
      LOG.info("Initial Shuffle Memory Required: " + memLimit + ", based on INPUT_BUFFER_factor: " + maxInMemCopyUse);

      float maxRedPer = conf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT,
          TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT);
      if (maxRedPer > 1.0 || maxRedPer < 0.0) {
        throw new TezUncheckedException(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT + maxRedPer);
      }
      long maxRedBuffer = (long) (maxAvailableTaskMemory * maxRedPer);

      LOG.info("Initial Memory required for final merged output: " + maxRedBuffer + ", using factor: " + maxRedPer);

      long reqMem = Math.max(maxRedBuffer, memLimit);
      return reqMem;
  }

  public void waitForInMemoryMerge() throws InterruptedException {
    inMemoryMerger.waitForMerge();
  }
  
  private boolean canShuffleToMemory(long requestedSize) {
    return (requestedSize < maxSingleShuffleLimit);
  }

  public synchronized void waitForShuffleToMergeMemory() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    while(usedMemory > memoryLimit) {
      wait();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Waited for " + (System.currentTimeMillis() - startTime) + " for memory to become"
          + " available");
    }
  }

  final private MapOutput stallShuffle = MapOutput.createWaitMapOutput(null);

  public synchronized MapOutput reserve(InputAttemptIdentifier srcAttemptIdentifier, 
                                             long requestedSize,
                                             long compressedLength,
                                             int fetcher
                                             ) throws IOException {
    if (!canShuffleToMemory(requestedSize)) {
      LOG.info(srcAttemptIdentifier + ": Shuffling to disk since " + requestedSize + 
               " is greater than maxSingleShuffleLimit (" + 
               maxSingleShuffleLimit + ")");
      return MapOutput.createDiskMapOutput(srcAttemptIdentifier, this, compressedLength, conf,
          fetcher, true, mapOutputFile);
    }
    
    // Stall shuffle if we are above the memory limit

    // It is possible that all threads could just be stalling and not make
    // progress at all. This could happen when:
    //
    // requested size is causing the used memory to go above limit &&
    // requested size < singleShuffleLimit &&
    // current used size < mergeThreshold (merge will not get triggered)
    //
    // To avoid this from happening, we allow exactly one thread to go past
    // the memory limit. We check (usedMemory > memoryLimit) and not
    // (usedMemory + requestedSize > memoryLimit). When this thread is done
    // fetching, this will automatically trigger a merge thereby unlocking
    // all the stalled threads
    
    if (usedMemory > memoryLimit) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(srcAttemptIdentifier + ": Stalling shuffle since usedMemory (" + usedMemory
            + ") is greater than memoryLimit (" + memoryLimit + ")." +
            " CommitMemory is (" + commitMemory + ")");
      }
      return stallShuffle;
    }
    
    // Allow the in-memory shuffle to progress
    if (LOG.isDebugEnabled()) {
      LOG.debug(srcAttemptIdentifier + ": Proceeding with shuffle since usedMemory ("
          + usedMemory + ") is lesser than memoryLimit (" + memoryLimit + ")."
          + "CommitMemory is (" + commitMemory + ")");
    }
    return unconditionalReserve(srcAttemptIdentifier, requestedSize, true);
  }
  
  /**
   * Unconditional Reserve is used by the Memory-to-Memory thread
   */
  private synchronized MapOutput unconditionalReserve(
      InputAttemptIdentifier srcAttemptIdentifier, long requestedSize, boolean primaryMapOutput) throws
      IOException {
    usedMemory += requestedSize;
    return MapOutput.createMemoryMapOutput(srcAttemptIdentifier, this, (int)requestedSize,
        primaryMapOutput);
  }
  
  synchronized void unreserve(long size) {
    commitMemory -= size;
    usedMemory -= size;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Notifying unreserve : commitMemory=" + commitMemory + ", usedMemory=" + usedMemory
          + ", mergeThreshold=" + mergeThreshold);
    }
    notifyAll();
  }

  public synchronized void closeInMemoryFile(MapOutput mapOutput) { 
    inMemoryMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryFile -> map-output of size: " + mapOutput.getSize()
        + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
        + ", commitMemory -> " + commitMemory + ", usedMemory ->" + usedMemory);

    commitMemory+= mapOutput.getSize();

    if (commitMemory >= mergeThreshold) {
      startMemToDiskMerge();
    }

    // This should likely run a Combiner.
    if (memToMemMerger != null) {
      synchronized (memToMemMerger) {
        if (!memToMemMerger.isInProgress() && 
            inMemoryMapOutputs.size() >= memToMemMergeOutputsThreshold) {
          memToMemMerger.startMerge(inMemoryMapOutputs);
        }
      }
    }
  }

  private void startMemToDiskMerge() {
    synchronized (inMemoryMerger) {
      if (!inMemoryMerger.isInProgress()) {
        LOG.info("Starting inMemoryMerger's merge since commitMemory=" +
            commitMemory + " > mergeThreshold=" + mergeThreshold +
            ". Current usedMemory=" + usedMemory);
        inMemoryMapOutputs.addAll(inMemoryMergedMapOutputs);
        inMemoryMergedMapOutputs.clear();
        inMemoryMerger.startMerge(inMemoryMapOutputs);
      }
    }
  }
  
  public synchronized void closeInMemoryMergedFile(MapOutput mapOutput) {
    inMemoryMergedMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryMergedFile -> size: " + mapOutput.getSize() + 
             ", inMemoryMergedMapOutputs.size() -> " + 
             inMemoryMergedMapOutputs.size());
  }
  
  public synchronized void closeOnDiskFile(FileChunk file) {
    onDiskMapOutputs.add(file);

    synchronized (onDiskMerger) {
      if (!onDiskMerger.isInProgress() &&
          onDiskMapOutputs.size() >= (2 * ioSortFactor - 1)) {
        onDiskMerger.startMerge(onDiskMapOutputs);
      }
    }
  }

  /**
   * Should <b>only</b> be used after the Shuffle phaze is complete, otherwise can
   * return an invalid state since a merge may not be in progress dur to
   * inadequate inputs
   * 
   * @return true if the merge process is complete, otherwise false
   */
  @Private
  public boolean isMergeComplete() {
    return finalMergeComplete;
  }
  
  public TezRawKeyValueIterator close() throws Throwable {
    // Wait for on-going merges to complete
    if (memToMemMerger != null) { 
      memToMemMerger.close();
    }
    inMemoryMerger.close();
    onDiskMerger.close();
    
    List<MapOutput> memory = 
      new ArrayList<MapOutput>(inMemoryMergedMapOutputs);
    inMemoryMergedMapOutputs.clear();
    memory.addAll(inMemoryMapOutputs);
    inMemoryMapOutputs.clear();
    List<FileChunk> disk = new ArrayList<FileChunk>(onDiskMapOutputs);
    onDiskMapOutputs.clear();
    TezRawKeyValueIterator kvIter = finalMerge(conf, rfs, memory, disk);
    this.finalMergeComplete = true;
    return kvIter;
  }
   
  void runCombineProcessor(TezRawKeyValueIterator kvIter, Writer writer)
      throws IOException, InterruptedException {
    combiner.combine(kvIter, writer);
  }

  /**
   * Merges multiple in-memory segment to another in-memory segment
   */
  private class IntermediateMemoryToMemoryMerger 
  extends MergeThread<MapOutput> {
    
    public IntermediateMemoryToMemoryMerger(MergeManager manager, 
                                            int mergeFactor) {
      super(manager, mergeFactor, exceptionReporter);
      setName("MemToMemMerger [" + TezUtilsInternal
          .cleanVertexName(inputContext.getSourceVertexName()) + "]");
      setDaemon(true);
    }

    @Override
    public void merge(List<MapOutput> inputs) throws IOException {
      if (inputs == null || inputs.size() == 0) {
        return;
      }


      InputAttemptIdentifier dummyMapId = inputs.get(0).getAttemptIdentifier(); 
      List<Segment> inMemorySegments = new ArrayList<Segment>();
      long mergeOutputSize = 
        createInMemorySegments(inputs, inMemorySegments, 0);
      int noInMemorySegments = inMemorySegments.size();

      MapOutput mergedMapOutputs =
        unconditionalReserve(dummyMapId, mergeOutputSize, false);
      
      Writer writer = 
        new InMemoryWriter(mergedMapOutputs.getArrayStream());

      LOG.info("Initiating Memory-to-Memory merge with " + noInMemorySegments +
               " segments of total-size: " + mergeOutputSize);

      // Nothing will be materialized to disk because the sort factor is being
      // set to the number of in memory segments.
      // TODO Is this doing any combination ?
      TezRawKeyValueIterator rIter = 
        TezMerger.merge(conf, rfs,
                       ConfigUtils.getIntermediateInputKeyClass(conf),
                       ConfigUtils.getIntermediateInputValueClass(conf),
                       inMemorySegments, inMemorySegments.size(),
                       new Path(inputContext.getUniqueIdentifier()),
                       (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
                       nullProgressable, null, null, null, null); 
      TezMerger.writeFile(rIter, writer, nullProgressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
      writer.close();

      LOG.info(inputContext.getUniqueIdentifier() +  
               " Memory-to-Memory merge of the " + noInMemorySegments +
               " files in-memory complete.");

      // Note the output of the merge
      closeInMemoryMergedFile(mergedMapOutputs);
    }
  }
  
  /**
   * Merges multiple in-memory segment to a disk segment
   */
  private class InMemoryMerger extends MergeThread<MapOutput> {
    
    public InMemoryMerger(MergeManager manager) {
      super(manager, Integer.MAX_VALUE, exceptionReporter);
      setName("MemtoDiskMerger [" + TezUtilsInternal
          .cleanVertexName(inputContext.getSourceVertexName()) + "]");
      setDaemon(true);
    }
    
    @Override
    public void merge(List<MapOutput> inputs) throws IOException, InterruptedException {
      if (inputs == null || inputs.size() == 0) {
        return;
      }
      
      numMemToDiskMerges.increment(1);
      
      //name this output file same as the name of the first file that is 
      //there in the current list of inmem files (this is guaranteed to
      //be absent on the disk currently. So we don't overwrite a prev. 
      //created spill). Also we need to create the output file now since
      //it is not guaranteed that this file will be present after merge
      //is called (we delete empty files as soon as we see them
      //in the merge method)

      //figure out the mapId 
      InputAttemptIdentifier srcTaskIdentifier = inputs.get(0).getAttemptIdentifier();

      List<Segment> inMemorySegments = new ArrayList<Segment>();
      long mergeOutputSize = 
        createInMemorySegments(inputs, inMemorySegments,0);
      int noInMemorySegments = inMemorySegments.size();

      // TODO Maybe track serialized vs deserialized bytes.
      
      // All disk writes done by this merge are overhead - due to the lac of
      // adequate memory to keep all segments in memory.
      Path outputPath = mapOutputFile.getInputFileForWrite(
          srcTaskIdentifier.getInputIdentifier().getInputIndex(),
          mergeOutputSize).suffix(Constants.MERGED_OUTPUT_PREFIX);

      Writer writer = null;
      long outFileLen = 0;
      try {
        writer =
            new Writer(conf, rfs, outputPath,
                (Class)ConfigUtils.getIntermediateInputKeyClass(conf),
                (Class)ConfigUtils.getIntermediateInputValueClass(conf),
                codec, null, null);

        TezRawKeyValueIterator rIter = null;
        LOG.info("Initiating in-memory merge with " + noInMemorySegments + 
            " segments...");

        // Nothing actually materialized to disk - controlled by setting sort-factor to #segments.
        rIter = TezMerger.merge(conf, rfs,
            (Class)ConfigUtils.getIntermediateInputKeyClass(conf),
            (Class)ConfigUtils.getIntermediateInputValueClass(conf),
            inMemorySegments, inMemorySegments.size(),
            new Path(inputContext.getUniqueIdentifier()),
            (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
            nullProgressable, spilledRecordsCounter, null, additionalBytesRead, null);
        // spilledRecordsCounter is tracking the number of keys that will be
        // read from each of the segments being merged - which is essentially
        // what will be written to disk.

        if (null == combiner) {
          TezMerger.writeFile(rIter, writer, nullProgressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        } else {
          // TODO Counters for Combine
          runCombineProcessor(rIter, writer);
        }
        writer.close();
        additionalBytesWritten.increment(writer.getCompressedLength());
        writer = null;

        outFileLen = localFS.getFileStatus(outputPath).getLen();
        LOG.info(inputContext.getUniqueIdentifier() +
            " Merge of the " + noInMemorySegments +
            " files in-memory complete." +
            " Local file is " + outputPath + " of size " +
            outFileLen);
      } catch (IOException e) { 
        //make sure that we delete the ondisk file that we created 
        //earlier when we invoked cloneFileAttributes
        localFS.delete(outputPath, true);
        throw e;
      } finally {
        if (writer != null) {
          writer.close();
        }
      }

      // Note the output of the merge
      closeOnDiskFile(new FileChunk(outputPath, 0, outFileLen));
    }

  }

  /**
   * Merges multiple on-disk segments
   */
  @VisibleForTesting
  class OnDiskMerger extends MergeThread<FileChunk> {

    public OnDiskMerger(MergeManager manager) {
      super(manager, ioSortFactor, exceptionReporter);
      setName("DiskToDiskMerger [" + TezUtilsInternal
          .cleanVertexName(inputContext.getSourceVertexName()) + "]");
      setDaemon(true);
    }
    
    @Override
    public void merge(List<FileChunk> inputs) throws IOException {
      // sanity check
      if (inputs == null || inputs.isEmpty()) {
        LOG.info("No ondisk files to merge...");
        return;
      }
      numDiskToDiskMerges.increment(1);
      
      long approxOutputSize = 0;
      int bytesPerSum = 
        conf.getInt("io.bytes.per.checksum", 512);
      
      LOG.info("OnDiskMerger: We have  " + inputs.size() + 
               " map outputs on disk. Triggering merge...");

      List<Segment> inputSegments = new ArrayList<Segment>(inputs.size());

      // 1. Prepare the list of files to be merged.
      for (FileChunk fileChunk : inputs) {
        final long offset = fileChunk.getOffset();
        final long size = fileChunk.getLength();
        final boolean preserve = fileChunk.isLocalFile();
        final Path file = fileChunk.getPath();
        approxOutputSize += size;
        Segment segment = new Segment(rfs, file, offset, size, codec, ifileReadAhead,
            ifileReadAheadLength, ifileBufferSize, preserve);
        inputSegments.add(segment);
      }

      // add the checksum length
      approxOutputSize += 
        ChecksumFileSystem.getChecksumLength(approxOutputSize, bytesPerSum);

      // 2. Start the on-disk merge process
      FileChunk file0 = inputs.get(0);
      String namePart;
      if (file0.isLocalFile()) {
        // This is setup the same way a type DISK MapOutput is setup when fetching.
        namePart = mapOutputFile.getSpillFileName(
            file0.getInputAttemptIdentifier().getInputIdentifier().getInputIndex());

      } else {
        namePart = file0.getPath().getName().toString();
      }

      // namePart includes the suffix of the file. We need to remove it.
      namePart = FilenameUtils.removeExtension(namePart);
      Path outputPath = localDirAllocator.getLocalPathForWrite(namePart, approxOutputSize, conf);
      outputPath = outputPath.suffix(Constants.MERGED_OUTPUT_PREFIX + mergeFileSequenceId.getAndIncrement());

      Writer writer =
        new Writer(conf, rfs, outputPath, 
                        (Class)ConfigUtils.getIntermediateInputKeyClass(conf), 
                        (Class)ConfigUtils.getIntermediateInputValueClass(conf),
                        codec, null, null);
      Path tmpDir = new Path(inputContext.getUniqueIdentifier());
      try {
        TezRawKeyValueIterator iter = TezMerger.merge(conf, rfs,
            (Class)ConfigUtils.getIntermediateInputKeyClass(conf),
            (Class)ConfigUtils.getIntermediateInputValueClass(conf),
            inputSegments,
            ioSortFactor, tmpDir,
            (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
            nullProgressable, true, spilledRecordsCounter, null,
            mergedMapOutputsCounter, null);

        // TODO Maybe differentiate between data written because of Merges and
        // the finalMerge (i.e. final mem available may be different from
        // initial merge mem)
        TezMerger.writeFile(iter, writer, nullProgressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        writer.close();
        additionalBytesWritten.increment(writer.getCompressedLength());
      } catch (IOException e) {
        localFS.delete(outputPath, true);
        throw e;
      }

      final long outputLen = localFS.getFileStatus(outputPath).getLen();
      closeOnDiskFile(new FileChunk(outputPath, 0, outputLen));

      LOG.info(inputContext.getUniqueIdentifier() +
          " Finished merging " + inputs.size() + 
          " map output files on disk of total-size " + 
          approxOutputSize + "." + 
          " Local output file is " + outputPath + " of size " +
          outputLen);
    }
  }
  
  private long createInMemorySegments(List<MapOutput> inMemoryMapOutputs,
                                      List<Segment> inMemorySegments, 
                                      long leaveBytes
                                      ) throws IOException {
    long totalSize = 0L;
    // We could use fullSize could come from the RamManager, but files can be
    // closed but not yet present in inMemoryMapOutputs
    long fullSize = 0L;
    for (MapOutput mo : inMemoryMapOutputs) {
      fullSize += mo.getMemory().length;
    }
    while(fullSize > leaveBytes) {
      MapOutput mo = inMemoryMapOutputs.remove(0);
      byte[] data = mo.getMemory();
      long size = data.length;
      totalSize += size;
      fullSize -= size;
      IFile.Reader reader = new InMemoryReader(MergeManager.this, 
                                                   mo.getAttemptIdentifier(),
                                                   data, 0, (int)size);
      inMemorySegments.add(new Segment(reader, true, 
                                            (mo.isPrimaryMapOutput() ? 
                                            mergedMapOutputsCounter : null)));
    }
    return totalSize;
  }

  class RawKVIteratorReader extends IFile.Reader {

    private final TezRawKeyValueIterator kvIter;

    public RawKVIteratorReader(TezRawKeyValueIterator kvIter, long size)
        throws IOException {
      super(null, size, null, spilledRecordsCounter, null, ifileReadAhead,
          ifileReadAheadLength, ifileBufferSize);
      this.kvIter = kvIter;
    }
    @Override
    public KeyState readRawKey(DataInputBuffer key) throws IOException {
      if (kvIter.next()) {
        final DataInputBuffer kb = kvIter.getKey();
        final int kp = kb.getPosition();
        final int klen = kb.getLength() - kp;
        key.reset(kb.getData(), kp, klen);
        bytesRead += klen;
        return KeyState.NEW_KEY;
      }
      return KeyState.NO_KEY;
    }
    public void nextRawValue(DataInputBuffer value) throws IOException {
      final DataInputBuffer vb = kvIter.getValue();
      final int vp = vb.getPosition();
      final int vlen = vb.getLength() - vp;
      value.reset(vb.getData(), vp, vlen);
      bytesRead += vlen;
    }
    public long getPosition() throws IOException {
      return bytesRead;
    }

    public void close() throws IOException {
      kvIter.close();
    }
  }

  private TezRawKeyValueIterator finalMerge(Configuration job, FileSystem fs,
                                       List<MapOutput> inMemoryMapOutputs,
                                       List<FileChunk> onDiskMapOutputs
                                       ) throws IOException {
    LOG.info("finalMerge called with " + 
             inMemoryMapOutputs.size() + " in-memory map-outputs and " + 
             onDiskMapOutputs.size() + " on-disk map-outputs");
    
    
    

    // merge config params
    Class keyClass = (Class)ConfigUtils.getIntermediateInputKeyClass(job);
    Class valueClass = (Class)ConfigUtils.getIntermediateInputValueClass(job);
    final Path tmpDir = new Path(inputContext.getUniqueIdentifier());
    final RawComparator comparator =
      (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(job);

    // segments required to vacate memory
    List<Segment> memDiskSegments = new ArrayList<Segment>();
    long inMemToDiskBytes = 0;
    boolean mergePhaseFinished = false;
    if (inMemoryMapOutputs.size() > 0) {
      int srcTaskId = inMemoryMapOutputs.get(0).getAttemptIdentifier().getInputIdentifier().getInputIndex();
      inMemToDiskBytes = createInMemorySegments(inMemoryMapOutputs, 
                                                memDiskSegments,
                                                this.postMergeMemLimit);
      final int numMemDiskSegments = memDiskSegments.size();
      if (numMemDiskSegments > 0 &&
            ioSortFactor > onDiskMapOutputs.size()) {
        
        // If we reach here, it implies that we have less than io.sort.factor
        // disk segments and this will be incremented by 1 (result of the 
        // memory segments merge). Since this total would still be 
        // <= io.sort.factor, we will not do any more intermediate merges,
        // the merge of all these disk segments would be directly fed to the
        // reduce method
        
        mergePhaseFinished = true;
        // must spill to disk, but can't retain in-mem for intermediate merge
        final Path outputPath = 
          mapOutputFile.getInputFileForWrite(srcTaskId,
                                             inMemToDiskBytes).suffix(
                                                 Constants.MERGED_OUTPUT_PREFIX);
        final TezRawKeyValueIterator rIter = TezMerger.merge(job, fs, keyClass, valueClass,
            memDiskSegments, numMemDiskSegments, tmpDir, comparator, nullProgressable,
            spilledRecordsCounter, null, additionalBytesRead, null);
        final Writer writer = new Writer(job, fs, outputPath,
            keyClass, valueClass, codec, null, null);
        try {
          TezMerger.writeFile(rIter, writer, nullProgressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        } catch (IOException e) {
          if (null != outputPath) {
            try {
              fs.delete(outputPath, true);
            } catch (IOException ie) {
              // NOTHING
            }
          }
          throw e;
        } finally {
          if (null != writer) {
            writer.close();
            additionalBytesWritten.increment(writer.getCompressedLength());
          }
        }

        final FileStatus fStatus = localFS.getFileStatus(outputPath);
        // add to list of final disk outputs.
        onDiskMapOutputs.add(new FileChunk(outputPath, 0, fStatus.getLen()));

        LOG.info("Merged " + numMemDiskSegments + " segments, " +
                 inMemToDiskBytes + " bytes to disk to satisfy " +
                 "reduce memory limit");
        inMemToDiskBytes = 0;
        memDiskSegments.clear();
      } else if (inMemToDiskBytes != 0) {
        LOG.info("Keeping " + numMemDiskSegments + " segments, " +
                 inMemToDiskBytes + " bytes in memory for " +
                 "intermediate, on-disk merge");
      }
    }

    // segments on disk
    List<Segment> diskSegments = new ArrayList<Segment>();
    long onDiskBytes = inMemToDiskBytes;
    FileChunk[] onDisk = onDiskMapOutputs.toArray(new FileChunk[onDiskMapOutputs.size()]);
    for (FileChunk fileChunk : onDisk) {
      final long fileLength = fileChunk.getLength();
      onDiskBytes += fileLength;
      LOG.debug("Disk file: " + fileChunk.getPath() + " Length is " + fileLength);

      final Path file = fileChunk.getPath();
      TezCounter counter =
          file.toString().endsWith(Constants.MERGED_OUTPUT_PREFIX) ? null : mergedMapOutputsCounter;

      final long fileOffset = fileChunk.getOffset();
      final boolean preserve = fileChunk.isLocalFile();
      diskSegments.add(new Segment(fs, file, fileOffset, fileLength, codec, ifileReadAhead,
                                   ifileReadAheadLength, ifileBufferSize, preserve, counter));
    }
    LOG.info("Merging " + onDisk.length + " files, " +
             onDiskBytes + " bytes from disk");
    Collections.sort(diskSegments, new Comparator<Segment>() {
      public int compare(Segment o1, Segment o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }
        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    });

    // build final list of segments from merged backed by disk + in-mem
    List<Segment> finalSegments = new ArrayList<Segment>();
    long inMemBytes = createInMemorySegments(inMemoryMapOutputs, 
                                             finalSegments, 0);
    LOG.info("Merging " + finalSegments.size() + " segments, " +
             inMemBytes + " bytes from memory into reduce");
    if (0 != onDiskBytes) {
      final int numInMemSegments = memDiskSegments.size();
      diskSegments.addAll(0, memDiskSegments);
      memDiskSegments.clear();
      TezRawKeyValueIterator diskMerge = TezMerger.merge(
          job, fs, keyClass, valueClass, codec, diskSegments,
          ioSortFactor, numInMemSegments, tmpDir, comparator,
          nullProgressable, false, spilledRecordsCounter, null, additionalBytesRead, null);
      diskSegments.clear();
      if (0 == finalSegments.size()) {
        return diskMerge;
      }
      finalSegments.add(new Segment(
            new RawKVIteratorReader(diskMerge, onDiskBytes), true));
    }
    // This is doing nothing but creating an iterator over the segments.
    return TezMerger.merge(job, fs, keyClass, valueClass,
                 finalSegments, finalSegments.size(), tmpDir,
                 comparator, nullProgressable, spilledRecordsCounter, null,
                 additionalBytesRead, null);
  }
}
