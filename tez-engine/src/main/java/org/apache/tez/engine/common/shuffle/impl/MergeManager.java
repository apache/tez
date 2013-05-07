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
package org.apache.tez.engine.common.shuffle.impl;

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
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.Constants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTaskReporter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.common.ConfigUtils;
import org.apache.tez.engine.common.combine.CombineInput;
import org.apache.tez.engine.common.combine.CombineOutput;
import org.apache.tez.engine.common.sort.impl.IFile;
import org.apache.tez.engine.common.sort.impl.TezMerger;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.engine.common.sort.impl.IFile.Writer;
import org.apache.tez.engine.common.sort.impl.TezMerger.Segment;
import org.apache.tez.engine.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskID;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings(value={"rawtypes"})
public class MergeManager {
  
  private static final Log LOG = LogFactory.getLog(MergeManager.class);
  
  private final TezTaskAttemptID taskAttemptId;
  
  private final Configuration conf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  private final LocalDirAllocator localDirAllocator;
  
  private final  TezTaskOutputFiles mapOutputFile;
  
  Set<MapOutput> inMemoryMergedMapOutputs = 
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final IntermediateMemoryToMemoryMerger memToMemMerger;

  Set<MapOutput> inMemoryMapOutputs = 
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final InMemoryMerger inMemoryMerger;
  
  Set<Path> onDiskMapOutputs = new TreeSet<Path>();
  private final OnDiskMerger onDiskMerger;
  
  private final long memoryLimit;
  private long usedMemory;
  private long commitMemory;
  private final long maxSingleShuffleLimit;
  
  private final int memToMemMergeOutputsThreshold; 
  private final long mergeThreshold;
  
  private final int ioSortFactor;

  private final TezTaskReporter reporter;
  private final ExceptionReporter exceptionReporter;
  
  /**
   * Combiner processor to run during in-memory merge, if defined.
   */
  private final Processor combineProcessor;

  private final TezCounter spilledRecordsCounter;

  private final TezCounter reduceCombineInputCounter;

  private final TezCounter mergedMapOutputsCounter;
  
  private final CompressionCodec codec;
  
  private final Progress mergePhase;

  public MergeManager(TezTaskAttemptID taskAttemptId, 
                      Configuration conf, 
                      FileSystem localFS,
                      LocalDirAllocator localDirAllocator,  
                      TezTaskReporter reporter,
                      Processor combineProcessor,
                      TezCounter spilledRecordsCounter,
                      TezCounter reduceCombineInputCounter,
                      TezCounter mergedMapOutputsCounter,
                      ExceptionReporter exceptionReporter,
                      Progress mergePhase) {
    this.taskAttemptId = taskAttemptId;
    this.conf = conf;
    this.localDirAllocator = localDirAllocator;
    this.exceptionReporter = exceptionReporter;
    
    this.reporter = reporter;
    this.combineProcessor = combineProcessor;
    this.reduceCombineInputCounter = reduceCombineInputCounter;
    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;
    this.mapOutputFile = new TezTaskOutputFiles();
    this.mapOutputFile.setConf(conf);
    
    this.localFS = localFS;
    this.rfs = ((LocalFileSystem)localFS).getRaw();

    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
    } else {
      codec = null;
    }

    final float maxInMemCopyUse =
      conf.getFloat(
          TezJobConfig.TEZ_ENGINE_SHUFFLE_INPUT_BUFFER_PERCENT, 
          TezJobConfig.DEFAULT_TEZ_ENGINE_SHUFFLE_INPUT_BUFFER_PERCENT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for " +
          TezJobConfig.TEZ_ENGINE_SHUFFLE_INPUT_BUFFER_PERCENT + ": " +
          maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    this.memoryLimit = 
      (long)(conf.getLong(Constants.TEZ_ENGINE_TASK_MEMORY,
          Math.min(Runtime.getRuntime().maxMemory(), Integer.MAX_VALUE))
        * maxInMemCopyUse);
 
    this.ioSortFactor = 
        conf.getInt(
            TezJobConfig.TEZ_ENGINE_IO_SORT_FACTOR, 
            TezJobConfig.DEFAULT_TEZ_ENGINE_IO_SORT_FACTOR);

    final float singleShuffleMemoryLimitPercent =
        conf.getFloat(
            TezJobConfig.TEZ_ENGINE_SHUFFLE_MEMORY_LIMIT_PERCENT,
            TezJobConfig.DEFAULT_TEZ_ENGINE_SHUFFLE_MEMORY_LIMIT_PERCENT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezJobConfig.TEZ_ENGINE_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    this.maxSingleShuffleLimit = 
      (long)(memoryLimit * singleShuffleMemoryLimitPercent);
    this.memToMemMergeOutputsThreshold = 
            conf.getInt(
                TezJobConfig.TEZ_ENGINE_SHUFFLE_MEMTOMEM_SEGMENTS, 
                ioSortFactor);
    this.mergeThreshold = 
        (long)(this.memoryLimit * 
               conf.getFloat(
                   TezJobConfig.TEZ_ENGINE_SHUFFLE_MERGE_PERCENT, 
                   TezJobConfig.DEFAULT_TEZ_ENGINE_SHUFFLE_MERGE_PERCENT));
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
          TezJobConfig.TEZ_ENGINE_SHUFFLE_ENABLE_MEMTOMEM, 
          TezJobConfig.DEFAULT_TEZ_ENGINE_SHUFFLE_ENABLE_MEMTOMEM);
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
    
    this.mergePhase = mergePhase;
  }
  

  TezTaskAttemptID getReduceId() {
    return taskAttemptId;
  }

  public void waitForInMemoryMerge() throws InterruptedException {
    inMemoryMerger.waitForMerge();
  }
  
  private boolean canShuffleToMemory(long requestedSize) {
    return (requestedSize < maxSingleShuffleLimit); 
  }
  
  final private MapOutput stallShuffle = new MapOutput(null);

  public synchronized MapOutput reserve(TezTaskAttemptID mapId, 
                                             long requestedSize,
                                             int fetcher
                                             ) throws IOException {
    if (!canShuffleToMemory(requestedSize)) {
      LOG.info(mapId + ": Shuffling to disk since " + requestedSize + 
               " is greater than maxSingleShuffleLimit (" + 
               maxSingleShuffleLimit + ")");
      return new MapOutput(mapId, this, requestedSize, conf, 
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
      LOG.debug(mapId + ": Stalling shuffle since usedMemory (" + usedMemory
          + ") is greater than memoryLimit (" + memoryLimit + ")." + 
          " CommitMemory is (" + commitMemory + ")"); 
      return stallShuffle;
    }
    
    // Allow the in-memory shuffle to progress
    LOG.debug(mapId + ": Proceeding with shuffle since usedMemory ("
        + usedMemory + ") is lesser than memoryLimit (" + memoryLimit + ")."
        + "CommitMemory is (" + commitMemory + ")"); 
    return unconditionalReserve(mapId, requestedSize, true);
  }
  
  /**
   * Unconditional Reserve is used by the Memory-to-Memory thread
   * @return
   */
  private synchronized MapOutput unconditionalReserve(
      TezTaskAttemptID mapId, long requestedSize, boolean primaryMapOutput) {
    usedMemory += requestedSize;
    return new MapOutput(mapId, this, (int)requestedSize, 
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
      if (!inMemoryMerger.isInProgress() && commitMemory >= mergeThreshold) {
        LOG.info("Starting inMemoryMerger's merge since commitMemory=" +
            commitMemory + " > mergeThreshold=" + mergeThreshold + 
            ". Current usedMemory=" + usedMemory);
        inMemoryMapOutputs.addAll(inMemoryMergedMapOutputs);
        inMemoryMergedMapOutputs.clear();
        inMemoryMerger.startMerge(inMemoryMapOutputs);
      } 
    }
    
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
    return finalMerge(conf, rfs, memory, disk);
  }
   
  void runCombineProcessor(TezRawKeyValueIterator kvIter, Writer writer)
  throws IOException, InterruptedException {

    CombineInput combineIn = new CombineInput(kvIter);
    combineIn.initialize(conf, reporter);
    
    CombineOutput combineOut = new CombineOutput(writer);
    combineOut.initialize(conf, reporter);

    try {
      combineProcessor.process(new Input[] {combineIn},
          new Output[] {combineOut});
    } catch (IOException ioe) {
      try {
        combineProcessor.close();
      } catch (IOException ignoredException) {}

      throw ioe;
    }
  
  }

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

      TezTaskAttemptID dummyMapId = inputs.get(0).getMapId(); 
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

      TezRawKeyValueIterator rIter = 
        TezMerger.merge(conf, rfs,
                       ConfigUtils.getIntermediateInputKeyClass(conf),
                       ConfigUtils.getIntermediateInputValueClass(conf),
                       inMemorySegments, inMemorySegments.size(),
                       new Path(taskAttemptId.toString()),
                       (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
                       reporter, null, null, null);
      TezMerger.writeFile(rIter, writer, reporter, conf);
      writer.close();

      LOG.info(taskAttemptId +  
               " Memory-to-Memory merge of the " + noInMemorySegments +
               " files in-memory complete.");

      // Note the output of the merge
      closeInMemoryMergedFile(mergedMapOutputs);
    }
  }
  
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
      
      //name this output file same as the name of the first file that is 
      //there in the current list of inmem files (this is guaranteed to
      //be absent on the disk currently. So we don't overwrite a prev. 
      //created spill). Also we need to create the output file now since
      //it is not guaranteed that this file will be present after merge
      //is called (we delete empty files as soon as we see them
      //in the merge method)

      //figure out the mapId 
      TezTaskAttemptID mapId = inputs.get(0).getMapId();
      TezTaskID mapTaskId = mapId.getTaskID();

      List<Segment> inMemorySegments = new ArrayList<Segment>();
      long mergeOutputSize = 
        createInMemorySegments(inputs, inMemorySegments,0);
      int noInMemorySegments = inMemorySegments.size();

      Path outputPath = 
        mapOutputFile.getInputFileForWrite(mapTaskId,
                                           mergeOutputSize).suffix(
                                               Constants.MERGED_OUTPUT_PREFIX);

      Writer writer = null;
      try {
        writer =
            new Writer(conf, rfs, outputPath,
                (Class)ConfigUtils.getIntermediateInputKeyClass(conf),
                (Class)ConfigUtils.getIntermediateInputValueClass(conf),
                codec, null);

        TezRawKeyValueIterator rIter = null;
        LOG.info("Initiating in-memory merge with " + noInMemorySegments + 
            " segments...");

        rIter = TezMerger.merge(conf, rfs,
            (Class)ConfigUtils.getIntermediateInputKeyClass(conf),
            (Class)ConfigUtils.getIntermediateInputValueClass(conf),
            inMemorySegments, inMemorySegments.size(),
            new Path(taskAttemptId.toString()),
            (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
            reporter, spilledRecordsCounter, null, null);

        if (null == combineProcessor) {
          TezMerger.writeFile(rIter, writer, reporter, conf);
        } else {
          runCombineProcessor(rIter, writer);
        }
        writer.close();
        writer = null;

        LOG.info(taskAttemptId +  
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
                        codec, null);
      TezRawKeyValueIterator iter  = null;
      Path tmpDir = new Path(taskAttemptId.toString());
      try {
        iter = TezMerger.merge(conf, rfs,
                            (Class)ConfigUtils.getIntermediateInputKeyClass(conf), 
                            (Class)ConfigUtils.getIntermediateInputValueClass(conf),
                            codec, inputs.toArray(new Path[inputs.size()]), 
                            true, ioSortFactor, tmpDir, 
                            (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf), 
                            reporter, spilledRecordsCounter, null, 
                            mergedMapOutputsCounter, null);

        TezMerger.writeFile(iter, writer, reporter, conf);
        writer.close();
      } catch (IOException e) {
        localFS.delete(outputPath, true);
        throw e;
      }

      closeOnDiskFile(outputPath);

      LOG.info(taskAttemptId +
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
                                                   mo.getMapId(),
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
      super(null, null, size, null, spilledRecordsCounter);
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
    
    final float maxRedPer =
      job.getFloat(
          TezJobConfig.TEZ_ENGINE_INPUT_BUFFER_PERCENT,
          TezJobConfig.DEFAULT_TEZ_ENGINE_INPUT_BUFFER_PERCENT);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new IOException(TezJobConfig.TEZ_ENGINE_INPUT_BUFFER_PERCENT +
                            maxRedPer);
    }
    int maxInMemReduce = (int)Math.min(
        Runtime.getRuntime().maxMemory() * maxRedPer, Integer.MAX_VALUE);
    

    // merge config params
    Class keyClass = (Class)ConfigUtils.getIntermediateInputKeyClass(job);
    Class valueClass = (Class)ConfigUtils.getIntermediateInputValueClass(job);
    final Path tmpDir = new Path(taskAttemptId.toString());
    final RawComparator comparator =
      (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(job);

    // segments required to vacate memory
    List<Segment> memDiskSegments = new ArrayList<Segment>();
    long inMemToDiskBytes = 0;
    boolean mergePhaseFinished = false;
    if (inMemoryMapOutputs.size() > 0) {
      TezTaskID mapId = inMemoryMapOutputs.get(0).getMapId().getTaskID();
      inMemToDiskBytes = createInMemorySegments(inMemoryMapOutputs, 
                                                memDiskSegments,
                                                maxInMemReduce);
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
          mapOutputFile.getInputFileForWrite(mapId,
                                             inMemToDiskBytes).suffix(
                                                 Constants.MERGED_OUTPUT_PREFIX);
        final TezRawKeyValueIterator rIter = TezMerger.merge(job, fs,
            keyClass, valueClass, memDiskSegments, numMemDiskSegments,
            tmpDir, comparator, reporter, spilledRecordsCounter, null, 
            mergePhase);
        final Writer writer = new Writer(job, fs, outputPath,
            keyClass, valueClass, codec, null);
        try {
          TezMerger.writeFile(rIter, writer, reporter, job);
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
      diskSegments.add(new Segment(job, fs, file, codec, false,
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
      // Pass mergePhase only if there is a going to be intermediate
      // merges. See comment where mergePhaseFinished is being set
      Progress thisPhase = (mergePhaseFinished) ? null : mergePhase; 
      TezRawKeyValueIterator diskMerge = TezMerger.merge(
          job, fs, keyClass, valueClass, diskSegments,
          ioSortFactor, numInMemSegments, tmpDir, comparator,
          reporter, false, spilledRecordsCounter, null, thisPhase);
      diskSegments.clear();
      if (0 == finalSegments.size()) {
        return diskMerge;
      }
      finalSegments.add(new Segment(
            new RawKVIteratorReader(diskMerge, onDiskBytes), true));
    }
    return TezMerger.merge(job, fs, keyClass, valueClass,
                 finalSegments, finalSegments.size(), tmpDir,
                 comparator, reporter, spilledRecordsCounter, null,
                 null);
  
  }
}
