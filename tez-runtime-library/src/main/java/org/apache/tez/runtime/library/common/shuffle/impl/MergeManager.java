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
package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.TezInputContext;
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

import com.google.common.base.Preconditions;

/**
 * Usage. Create instance. setInitialMemoryAvailable(long), configureAndStart()
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings(value={"rawtypes"})
public class MergeManager {
  
  private static final Log LOG = LogFactory.getLog(MergeManager.class);

  private final Configuration conf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  private final LocalDirAllocator localDirAllocator;
  
  private final  TezTaskOutputFiles mapOutputFile;
  private final Progressable nullProgressable = new NullProgressable();
  private final Combiner combiner;  
  
  Set<MapOutput> inMemoryMergedMapOutputs = 
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private IntermediateMemoryToMemoryMerger memToMemMerger;

  Set<MapOutput> inMemoryMapOutputs = 
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private InMemoryMerger inMemoryMerger;
  
  Set<Path> onDiskMapOutputs = new TreeSet<Path>();
  private OnDiskMerger onDiskMerger;
  
  private  long memoryLimit;
  private int postMergeMemLimit;
  private long usedMemory;
  private long commitMemory;
  private int ioSortFactor;
  private long maxSingleShuffleLimit;
  
  private int memToMemMergeOutputsThreshold; 
  private long mergeThreshold;
  
  private long initialMemoryAvailable = -1;

  private final ExceptionReporter exceptionReporter;
  
  private final TezInputContext inputContext;

  private final TezCounter spilledRecordsCounter;

  private final TezCounter reduceCombineInputCounter;

  private final TezCounter mergedMapOutputsCounter;
  
  private final TezCounter numMemToDiskMerges;
  private final TezCounter numDiskToDiskMerges;
  private final TezCounter additionalBytesWritten;
  private final TezCounter additionalBytesRead;
  
  private CompressionCodec codec;
  
  private volatile boolean finalMergeComplete = false;
  
  private boolean ifileReadAhead;
  private int ifileReadAheadLength;
  private int ifileBufferSize;


  /**
   * Construct the MergeManager. Must call start before it becomes usable.
   */
  public MergeManager(Configuration conf, 
                      FileSystem localFS,
                      LocalDirAllocator localDirAllocator,  
                      TezInputContext inputContext,
                      Combiner combiner,
                      TezCounter spilledRecordsCounter,
                      TezCounter reduceCombineInputCounter,
                      TezCounter mergedMapOutputsCounter,
                      ExceptionReporter exceptionReporter) {
    this.inputContext = inputContext;
    this.conf = conf;
    this.localDirAllocator = localDirAllocator;
    this.exceptionReporter = exceptionReporter;
    
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

  }
  
  void setInitialMemoryAvailable(long available) {
    this.initialMemoryAvailable = available;
  }
  
  @Private
  void configureAndStart() {
    Preconditions.checkState(initialMemoryAvailable != -1,
        "Initial available memory must be configured before starting");
    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
    } else {
      codec = null;
    }
    this.ifileReadAhead = conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
          TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
          TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }
    this.ifileBufferSize = conf.getInt("io.file.buffer.size",
        TezJobConfig.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);

    // Figure out initial memory req start
    final float maxInMemCopyUse =
      conf.getFloat(
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 
          TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for " +
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT + ": " +
          maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    long memLimit = (long) (conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
        Math.min(inputContext.getTotalMemoryAvailableToTask(), Integer.MAX_VALUE)) * maxInMemCopyUse);

    float maxRedPer = conf.getFloat(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_INPUT_BUFFER_PERCENT);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new TezUncheckedException(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT + maxRedPer);
    }
    // TODO maxRedBuffer should be a long.
    int maxRedBuffer = (int) Math.min(inputContext.getTotalMemoryAvailableToTask() * maxRedPer,
        Integer.MAX_VALUE);
    // Figure out initial memory req end
    
    if (this.initialMemoryAvailable < memLimit) {
      this.memoryLimit = this.initialMemoryAvailable;
    } else {
      this.memoryLimit = memLimit;
    }

    if (this.initialMemoryAvailable < maxRedBuffer) {
      this.postMergeMemLimit = (int) this.initialMemoryAvailable;
    } else {
      this.postMergeMemLimit = maxRedBuffer;
    }

    LOG.info("InitialRequest: ShuffleMem=" + memLimit + ", postMergeMem=" + maxRedBuffer
        + ", RuntimeTotalAvailable=" + this.initialMemoryAvailable + "Updated to: ShuffleMem="
        + this.memoryLimit + ", postMergeMem: " + this.postMergeMemLimit);

    this.ioSortFactor = 
        conf.getInt(
            TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_IO_SORT_FACTOR);

    final float singleShuffleMemoryLimitPercent =
        conf.getFloat(
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    this.maxSingleShuffleLimit = 
      (long)(memoryLimit * singleShuffleMemoryLimitPercent);
    this.memToMemMergeOutputsThreshold = 
            conf.getInt(
                TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, 
                ioSortFactor);
    this.mergeThreshold = 
        (long)(this.memoryLimit * 
               conf.getFloat(
                   TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 
                   TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT));
    LOG.info("MergerManager: memoryLimit=" + memoryLimit + ", " +
             "maxSingleShuffleLimit=" + maxSingleShuffleLimit + ", " +
             "mergeThreshold=" + mergeThreshold + ", " + 
             "ioSortFactor=" + ioSortFactor + ", " +
             "memToMemMergeOutputsThreshold=" + memToMemMergeOutputsThreshold);

    if (this.maxSingleShuffleLimit >= this.mergeThreshold) {
      throw new RuntimeException("Invlaid configuration: "
          + "maxSingleShuffleLimit should be less than mergeThreshold"
          + "maxSingleShuffleLimit: " + this.maxSingleShuffleLimit
          + "mergeThreshold: " + this.mergeThreshold);
    }

    boolean allowMemToMemMerge = 
      conf.getBoolean(
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM, 
          TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM);
    if (allowMemToMemMerge) {
      this.memToMemMerger = 
        new IntermediateMemoryToMemoryMerger(this,
                                             memToMemMergeOutputsThreshold);
      this.memToMemMerger.start();
    } else {
      this.memToMemMerger = null;
    }
    
    this.inMemoryMerger = new InMemoryMerger(this);
    this.inMemoryMerger.start();
    
    this.onDiskMerger = new OnDiskMerger(this);
    this.onDiskMerger.start();
  }
  
  /**
   * Exposing this to get an initial memory ask without instantiating the object.
   */
  @Private
  static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    final float maxInMemCopyUse =
        conf.getFloat(
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT);
      if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
        throw new IllegalArgumentException("Invalid value for " +
            TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT + ": " +
            maxInMemCopyUse);
      }

      // Allow unit tests to fix Runtime memory
      long memLimit = (long) (conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
          Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE)) * maxInMemCopyUse);
      
      LOG.info("Initial Shuffle Memory Required: " + memLimit + ", based on INPUT_BUFFER_factor: " + maxInMemCopyUse);

      float maxRedPer = conf.getFloat(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT,
          TezJobConfig.DEFAULT_TEZ_RUNTIME_INPUT_BUFFER_PERCENT);
      if (maxRedPer > 1.0 || maxRedPer < 0.0) {
        throw new TezUncheckedException(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT + maxRedPer);
      }
      // TODO maxRedBuffer should be a long.
      int maxRedBuffer = (int) Math.min(maxAvailableTaskMemory * maxRedPer,
          Integer.MAX_VALUE);
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

  final private MapOutput stallShuffle = new MapOutput(null);

  public synchronized MapOutput reserve(InputAttemptIdentifier srcAttemptIdentifier, 
                                             long requestedSize,
                                             int fetcher
                                             ) throws IOException {
    if (!canShuffleToMemory(requestedSize)) {
      LOG.info(srcAttemptIdentifier + ": Shuffling to disk since " + requestedSize + 
               " is greater than maxSingleShuffleLimit (" + 
               maxSingleShuffleLimit + ")");
      return new MapOutput(srcAttemptIdentifier, this, requestedSize, conf, 
                                localDirAllocator, fetcher, true,
                                mapOutputFile);
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
      LOG.debug(srcAttemptIdentifier + ": Stalling shuffle since usedMemory (" + usedMemory
          + ") is greater than memoryLimit (" + memoryLimit + ")." + 
          " CommitMemory is (" + commitMemory + ")"); 
      return stallShuffle;
    }
    
    // Allow the in-memory shuffle to progress
    LOG.debug(srcAttemptIdentifier + ": Proceeding with shuffle since usedMemory ("
        + usedMemory + ") is lesser than memoryLimit (" + memoryLimit + ")."
        + "CommitMemory is (" + commitMemory + ")"); 
    return unconditionalReserve(srcAttemptIdentifier, requestedSize, true);
  }
  
  /**
   * Unconditional Reserve is used by the Memory-to-Memory thread
   */
  private synchronized MapOutput unconditionalReserve(
      InputAttemptIdentifier srcAttemptIdentifier, long requestedSize, boolean primaryMapOutput) {
    usedMemory += requestedSize;
    return new MapOutput(srcAttemptIdentifier, this, (int)requestedSize, 
        primaryMapOutput);
  }
  
  synchronized void unreserve(long size) {
    commitMemory -= size;
    usedMemory -= size;
  }

  public synchronized void closeInMemoryFile(MapOutput mapOutput) { 
    inMemoryMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryFile -> map-output of size: " + mapOutput.getSize()
        + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
        + ", commitMemory -> " + commitMemory + ", usedMemory ->" + usedMemory);

    commitMemory+= mapOutput.getSize();

    synchronized (inMemoryMerger) {
      // Can hang if mergeThreshold is really low.
      // TODO Can avoid spilling in case total input size is between
      // mergeTghreshold and total available size.
      if (!inMemoryMerger.isInProgress() && commitMemory >= mergeThreshold) {
        LOG.info("Starting inMemoryMerger's merge since commitMemory=" +
            commitMemory + " > mergeThreshold=" + mergeThreshold + 
            ". Current usedMemory=" + usedMemory);
        inMemoryMapOutputs.addAll(inMemoryMergedMapOutputs);
        inMemoryMergedMapOutputs.clear();
        inMemoryMerger.startMerge(inMemoryMapOutputs);
      } 
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
  
  
  public synchronized void closeInMemoryMergedFile(MapOutput mapOutput) {
    inMemoryMergedMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryMergedFile -> size: " + mapOutput.getSize() + 
             ", inMemoryMergedMapOutputs.size() -> " + 
             inMemoryMergedMapOutputs.size());
  }
  
  public synchronized void closeOnDiskFile(Path file) {
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
    memory.addAll(inMemoryMapOutputs);
    List<Path> disk = new ArrayList<Path>(onDiskMapOutputs);
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
      setName("InMemoryMerger - Thread to do in-memory merge of in-memory " +
      		    "shuffled map-outputs");
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
      TezMerger.writeFile(rIter, writer, nullProgressable, TezJobConfig.DEFAULT_RECORDS_BEFORE_PROGRESS);
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
      setName
      ("InMemoryMerger - Thread to merge in-memory shuffled map-outputs");
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
          TezMerger.writeFile(rIter, writer, nullProgressable, TezJobConfig.DEFAULT_RECORDS_BEFORE_PROGRESS);
        } else {
          // TODO Counters for Combine
          runCombineProcessor(rIter, writer);
        }
        writer.close();
        additionalBytesWritten.increment(writer.getCompressedLength());
        writer = null;

        LOG.info(inputContext.getUniqueIdentifier() +  
            " Merge of the " + noInMemorySegments +
            " files in-memory complete." +
            " Local file is " + outputPath + " of size " + 
            localFS.getFileStatus(outputPath).getLen());
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
      closeOnDiskFile(outputPath);
    }

  }

  /**
   * Merges multiple on-disk segments
   */
  private class OnDiskMerger extends MergeThread<Path> {
    
    public OnDiskMerger(MergeManager manager) {
      super(manager, Integer.MAX_VALUE, exceptionReporter);
      setName("OnDiskMerger - Thread to merge on-disk map-outputs");
      setDaemon(true);
    }
    
    @Override
    public void merge(List<Path> inputs) throws IOException {
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
      
      // 1. Prepare the list of files to be merged. 
      for (Path file : inputs) {
        approxOutputSize += localFS.getFileStatus(file).getLen();
      }

      // add the checksum length
      approxOutputSize += 
        ChecksumFileSystem.getChecksumLength(approxOutputSize, bytesPerSum);

      // 2. Start the on-disk merge process
      Path outputPath = 
        localDirAllocator.getLocalPathForWrite(inputs.get(0).toString(), 
            approxOutputSize, conf).suffix(Constants.MERGED_OUTPUT_PREFIX);
      Writer writer = 
        new Writer(conf, rfs, outputPath, 
                        (Class)ConfigUtils.getIntermediateInputKeyClass(conf), 
                        (Class)ConfigUtils.getIntermediateInputValueClass(conf),
                        codec, null, null);
      TezRawKeyValueIterator iter  = null;
      Path tmpDir = new Path(inputContext.getUniqueIdentifier());
      try {
        iter = TezMerger.merge(conf, rfs,
                            (Class)ConfigUtils.getIntermediateInputKeyClass(conf), 
                            (Class)ConfigUtils.getIntermediateInputValueClass(conf),
                            codec, ifileReadAhead, ifileReadAheadLength, ifileBufferSize,
                            inputs.toArray(new Path[inputs.size()]), true, ioSortFactor, tmpDir, 
                            (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf), 
                            nullProgressable, spilledRecordsCounter, null, 
                            mergedMapOutputsCounter, null);

        // TODO Maybe differentiate between data written because of Merges and
        // the finalMerge (i.e. final mem available may be different from
        // initial merge mem)
        TezMerger.writeFile(iter, writer, nullProgressable, TezJobConfig.DEFAULT_RECORDS_BEFORE_PROGRESS);
        writer.close();
        additionalBytesWritten.increment(writer.getCompressedLength());
      } catch (IOException e) {
        localFS.delete(outputPath, true);
        throw e;
      }

      closeOnDiskFile(outputPath);

      LOG.info(inputContext.getUniqueIdentifier() +
          " Finished merging " + inputs.size() + 
          " map output files on disk of total-size " + 
          approxOutputSize + "." + 
          " Local output file is " + outputPath + " of size " +
          localFS.getFileStatus(outputPath).getLen());
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
    public boolean nextRawKey(DataInputBuffer key) throws IOException {
      if (kvIter.next()) {
        final DataInputBuffer kb = kvIter.getKey();
        final int kp = kb.getPosition();
        final int klen = kb.getLength() - kp;
        key.reset(kb.getData(), kp, klen);
        bytesRead += klen;
        return true;
      }
      return false;
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
                                       List<Path> onDiskMapOutputs
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
          TezMerger.writeFile(rIter, writer, nullProgressable, TezJobConfig.DEFAULT_RECORDS_BEFORE_PROGRESS);
          // add to list of final disk outputs.
          onDiskMapOutputs.add(outputPath);
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
    Path[] onDisk = onDiskMapOutputs.toArray(new Path[onDiskMapOutputs.size()]);
    for (Path file : onDisk) {
      onDiskBytes += fs.getFileStatus(file).getLen();
      LOG.debug("Disk file: " + file + " Length is " + 
          fs.getFileStatus(file).getLen());
      diskSegments.add(new Segment(job, fs, file, codec, ifileReadAhead,
                                   ifileReadAheadLength, ifileBufferSize, false,
                                         (file.toString().endsWith(
                                             Constants.MERGED_OUTPUT_PREFIX) ?
                                          null : mergedMapOutputsCounter)
                                        ));
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
          job, fs, keyClass, valueClass, diskSegments,
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
