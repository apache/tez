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
package org.apache.tez.runtime.library.common.writers;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Deflater;

import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;

public class UnorderedPartitionedKVWriter extends BaseUnorderedPartitionedKVWriter {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedPartitionedKVWriter.class);

  private static final int INT_SIZE = 4;
  private static final int NUM_META = 3; // Number of meta fields.
  private static final int INDEX_KEYLEN = 0; // KeyLength index
  private static final int INDEX_VALLEN = 1; // ValLength index
  private static final int INDEX_NEXT = 2; // Next Record Index.
  private static final int META_SIZE = NUM_META * INT_SIZE; // Size of total meta-data

  private final static int APPROX_HEADER_LENGTH = 150;

  // Maybe setup a separate statistics class which can be shared between the
  // buffer and the main path instead of having multiple arrays.

  private final String destNameTrimmed;
  private final long availableMemory;
  @VisibleForTesting
  final WrappedBuffer[] buffers;
  @VisibleForTesting
  final BlockingQueue<WrappedBuffer> availableBuffers;
  private final ByteArrayOutputStream baos;
  private final NonSyncDataOutputStream dos;
  @VisibleForTesting
  WrappedBuffer currentBuffer;
  private final FileSystem rfs;

  @VisibleForTesting
  final List<SpillInfo> spillInfoList = Collections.synchronizedList(new ArrayList<SpillInfo>());

  private final ListeningExecutorService spillExecutor;

  private final int[] numRecordsPerPartition;
  private long localOutputRecordBytesCounter;
  private long localOutputBytesWithOverheadCounter;
  private long localOutputRecordsCounter;
  // notify after x records
  private static final int NOTIFY_THRESHOLD = 1000;
  // uncompressed size for each partition
  private final long[] sizePerPartition;
  private volatile long spilledSize = 0;

  static final ThreadLocal<Deflater> deflater = new ThreadLocal<Deflater>() {

    @Override
    public Deflater initialValue() {
      return TezCommonUtils.newBestCompressionDeflater();
    }

    @Override
    public Deflater get() {
      Deflater deflater = super.get();
      deflater.reset();
      return deflater;
    }
  };

  private final Semaphore availableSlots;

  /**
   * Represents final number of records written (spills are not counted)
   */
  protected final TezCounter outputLargeRecordsCounter;

  @VisibleForTesting
  int numBuffers;
  @VisibleForTesting
  int sizePerBuffer;
  @VisibleForTesting
  int lastBufferSize;
  @VisibleForTesting
  int numInitializedBuffers;
  @VisibleForTesting
  int spillLimit;

  private Throwable spillException;
  private AtomicBoolean isShutdown = new AtomicBoolean(false);
  @VisibleForTesting
  final AtomicInteger numSpills = new AtomicInteger(0);
  private final AtomicInteger pendingSpillCount = new AtomicInteger(0);

  @VisibleForTesting
  Path finalIndexPath;
  @VisibleForTesting
  Path finalOutPath;

  //for single partition cases (e.g UnorderedKVOutput)
  private final IFile.Writer writer;
  @VisibleForTesting
  final boolean skipBuffers;

  private final ReentrantLock spillLock = new ReentrantLock();
  private final Condition spillInProgress = spillLock.newCondition();

  private final boolean pipelinedShuffle;
  private final boolean isFinalMergeEnabled;
  // To store events when final merge is disabled
  private final List<Event> finalEvents;
  // How partition stats should be reported.
  final ReportPartitionStats reportPartitionStats;

  private final long indexFileSizeEstimate;

  private List<WrappedBuffer> filledBuffers = new ArrayList<>();

  public UnorderedPartitionedKVWriter(OutputContext outputContext, Configuration conf,
      int numOutputs, long availableMemoryBytes) throws IOException {
    super(outputContext, conf, numOutputs);

    Preconditions.checkArgument(availableMemoryBytes >= 0, "availableMemory should be >= 0 bytes");

    this.destNameTrimmed = TezUtilsInternal.cleanVertexName(outputContext.getDestinationVertexName());
    //Not checking for TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT as it might not add much value in
    // this case.  Add it later if needed.
    boolean pipelinedShuffleConf = this.conf.getBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);
    this.isFinalMergeEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT_DEFAULT);
    this.pipelinedShuffle = pipelinedShuffleConf && !isFinalMergeEnabled;
    this.finalEvents = Lists.newLinkedList();

    if (availableMemoryBytes == 0) {
      Preconditions.checkArgument(((numPartitions == 1) && !pipelinedShuffle), "availableMemory "
          + "can be set to 0 only when numPartitions=1 and " + TezRuntimeConfiguration
          .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED + " is disabled. current numPartitions=" +
          numPartitions + ", " + TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED + "="
          + pipelinedShuffle);
    }

    // Ideally, should be significantly larger.
    availableMemory = availableMemoryBytes;

    // Allow unit tests to control the buffer sizes.
    int maxSingleBufferSizeBytes = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES,
        Integer.MAX_VALUE);
    computeNumBuffersAndSize(maxSingleBufferSizeBytes);

    availableBuffers = new LinkedBlockingQueue<WrappedBuffer>();
    buffers = new WrappedBuffer[numBuffers];
    // Set up only the first buffer to start with.
    buffers[0] = new WrappedBuffer(numOutputs, sizePerBuffer);
    numInitializedBuffers = 1;
    if (LOG.isDebugEnabled()) {
      LOG.debug(destNameTrimmed + ": " + "Initializing Buffer #" +
          numInitializedBuffers + " with size=" + sizePerBuffer);
    }
    currentBuffer = buffers[0];
    baos = new ByteArrayOutputStream();
    dos = new NonSyncDataOutputStream(baos);
    keySerializer.open(dos);
    valSerializer.open(dos);
    rfs = ((LocalFileSystem) FileSystem.getLocal(this.conf)).getRaw();

    int maxThreads = Math.max(2, numBuffers/2);
    //TODO: Make use of TezSharedExecutor later
    ExecutorService executor = new ThreadPoolExecutor(1, maxThreads,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(
                "UnorderedOutSpiller {" + TezUtilsInternal.cleanVertexName(
                    outputContext.getDestinationVertexName()) + "} #%d")
            .build()
    );
    // to restrict submission of more tasks than threads (e.g numBuffers > numThreads)
    // This is maxThreads - 1, to avoid race between callback thread releasing semaphore and the
    // thread calling tryAcquire.
    availableSlots = new Semaphore(maxThreads - 1, true);
    spillExecutor = MoreExecutors.listeningDecorator(executor);
    numRecordsPerPartition = new int[numPartitions];
    reportPartitionStats = ReportPartitionStats.fromString(
        conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));
    sizePerPartition = (reportPartitionStats.isEnabled()) ?
        new long[numPartitions] : null;

    outputLargeRecordsCounter = outputContext.getCounters().findCounter(
        TaskCounter.OUTPUT_LARGE_RECORDS);



    indexFileSizeEstimate = numPartitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;

    if (numPartitions == 1 && !pipelinedShuffle) {
      //special case, where in only one partition is available.
      finalOutPath = outputFileHandler.getOutputFileForWrite();
      finalIndexPath = outputFileHandler.getOutputIndexFileForWrite(indexFileSizeEstimate);
      skipBuffers = true;
      writer = new IFile.Writer(conf, rfs, finalOutPath, keyClass, valClass,
          codec, outputRecordsCounter, outputRecordBytesCounter);
    } else {
      skipBuffers = false;
      writer = null;
    }
    LOG.info(destNameTrimmed + ": "
        + "numBuffers=" + numBuffers
        + ", sizePerBuffer=" + sizePerBuffer
        + ", skipBuffers=" + skipBuffers
        + ", numPartitions=" + numPartitions
        + ", availableMemory=" + availableMemory
        + ", maxSingleBufferSizeBytes=" + maxSingleBufferSizeBytes
        + ", pipelinedShuffle=" + pipelinedShuffle
        + ", isFinalMergeEnabled=" + isFinalMergeEnabled
        + ", numPartitions=" + numPartitions
        + ", reportPartitionStats=" + reportPartitionStats);
  }

  private static final int ALLOC_OVERHEAD = 64;
  private void computeNumBuffersAndSize(int bufferLimit) {
    numBuffers = (int)(availableMemory / bufferLimit);

    if (numBuffers >= 2) {
      sizePerBuffer = bufferLimit - ALLOC_OVERHEAD;
      lastBufferSize = (int)(availableMemory % bufferLimit);
      // Use leftover memory last buffer only if the leftover memory > 50% of bufferLimit
      if (lastBufferSize > bufferLimit / 2) {
        numBuffers += 1;
      } else {
        if (lastBufferSize > 0) {
          LOG.warn("Underallocating memory. Unused memory size: {}.",  lastBufferSize);
        }
        lastBufferSize = sizePerBuffer;
      }
    } else {
      // We should have minimum of 2 buffers.
      numBuffers = 2;
      if (availableMemory / numBuffers > Integer.MAX_VALUE) {
        sizePerBuffer = Integer.MAX_VALUE;
      } else {
        sizePerBuffer = (int)(availableMemory / numBuffers);
      }
      // 2 equal sized buffers.
      lastBufferSize = sizePerBuffer;
    }
    // Ensure allocation size is multiple of INT_SIZE, truncate down.
    sizePerBuffer = sizePerBuffer - (sizePerBuffer % INT_SIZE);
    lastBufferSize = lastBufferSize - (lastBufferSize % INT_SIZE);

    int mergePercent = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT_DEFAULT);
    spillLimit = numBuffers * mergePercent / 100;
    // Keep within limits.
    if (spillLimit < 1) {
      spillLimit = 1;
    }
    if (spillLimit > numBuffers) {
      spillLimit = numBuffers;
    }
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    // Skipping checks for key-value types. IFile takes care of these, but should be removed from
    // there as well.

    // How expensive are checks like these ?
    if (isShutdown.get()) {
      throw new RuntimeException("Writer already closed");
    }
    if (spillException != null) {
      // Already reported as a fatalError - report to the user code
      throw new IOException("Exception during spill", new IOException(spillException));
    }
    if (skipBuffers) {
      //special case, where we have only one partition and pipelining is disabled.
      // The reason outputRecordsCounter isn't updated here:
      // For skipBuffers case, IFile writer has the reference to
      // outputRecordsCounter and during its close method call,
      // it will update the outputRecordsCounter.
      writer.append(key, value);
      outputContext.notifyProgress();
    } else {
      int partition = partitioner.getPartition(key, value, numPartitions);
      write(key, value, partition);
    }
  }

  @SuppressWarnings("unchecked")
  private void write(Object key, Object value, int partition) throws IOException {
    // Wrap to 4 byte (Int) boundary for metaData
    int mod = currentBuffer.nextPosition % INT_SIZE;
    int metaSkip = mod == 0 ? 0 : (INT_SIZE - mod);
    if ((currentBuffer.availableSize < (META_SIZE + metaSkip)) || (currentBuffer.full)) {
      // Move over to the next buffer.
      metaSkip = 0;
      setupNextBuffer();
    }
    currentBuffer.nextPosition += metaSkip;
    int metaStart = currentBuffer.nextPosition;
    currentBuffer.availableSize -= (META_SIZE + metaSkip);
    currentBuffer.nextPosition += META_SIZE;

    keySerializer.serialize(key);

    if (currentBuffer.full) {
      if (metaStart == 0) { // Started writing at the start of the buffer. Write Key to disk.
        // Key too large for any buffer. Write entire record to disk.
        currentBuffer.reset();
        writeLargeRecord(key, value, partition);
        return;
      } else { // Exceeded length on current buffer.
        // Try resetting the buffer to the next one, if this was not the start of a buffer,
        // and begin spilling the current buffer to disk if it has any records.
        setupNextBuffer();
        write(key, value, partition);
        return;
      }
    }

    int valStart = currentBuffer.nextPosition;
    valSerializer.serialize(value);

    if (currentBuffer.full) {
      // Value too large for current buffer, or K-V too large for entire buffer.
      if (metaStart == 0) {
        // Key + Value too large for a single buffer.
        currentBuffer.reset();
        writeLargeRecord(key, value, partition);
        return;
      } else { // Exceeded length on current buffer.
        // Try writing key+value to a new buffer - will fall back to disk if that fails.
        setupNextBuffer();
        write(key, value, partition);
        return;
      }
    }

    // Meta-data updates
    int metaIndex = metaStart / INT_SIZE;
    int indexNext = currentBuffer.partitionPositions[partition];

    currentBuffer.metaBuffer.put(metaIndex + INDEX_KEYLEN, (valStart - (metaStart + META_SIZE)));
    currentBuffer.metaBuffer.put(metaIndex + INDEX_VALLEN, (currentBuffer.nextPosition - valStart));
    currentBuffer.metaBuffer.put(metaIndex + INDEX_NEXT, indexNext);
    currentBuffer.skipSize += metaSkip; // For size estimation
    // Update stats on number of records
    localOutputRecordBytesCounter += (currentBuffer.nextPosition - (metaStart + META_SIZE));
    localOutputBytesWithOverheadCounter += ((currentBuffer.nextPosition - metaStart) + metaSkip);
    localOutputRecordsCounter++;
    if (localOutputRecordBytesCounter % NOTIFY_THRESHOLD == 0) {
      updateTezCountersAndNotify();
    }
    currentBuffer.partitionPositions[partition] = metaStart;
    currentBuffer.recordsPerPartition[partition]++;
    currentBuffer.sizePerPartition[partition] +=
        currentBuffer.nextPosition - (metaStart + META_SIZE);
    currentBuffer.numRecords++;

  }

  private void updateTezCountersAndNotify() {
    outputRecordBytesCounter.increment(localOutputRecordBytesCounter);
    outputBytesWithOverheadCounter.increment(localOutputBytesWithOverheadCounter);
    outputRecordsCounter.increment(localOutputRecordsCounter);
    outputContext.notifyProgress();
    localOutputRecordBytesCounter = 0;
    localOutputBytesWithOverheadCounter = 0;
    localOutputRecordsCounter = 0;
  }

  private void setupNextBuffer() throws IOException {

    if (currentBuffer.numRecords == 0) {
      currentBuffer.reset();
    } else {
      // Update overall stats
      final int filledBufferCount = filledBuffers.size();
      if (LOG.isDebugEnabled() || (filledBufferCount % 10) == 0) {
        LOG.info(destNameTrimmed + ": " + "Moving to next buffer. Total filled buffers: " + filledBufferCount);
      }
      updateGlobalStats(currentBuffer);

      filledBuffers.add(currentBuffer);
      mayBeSpill(false);

      currentBuffer = getNextAvailableBuffer();

      // in case spill threads are free, check if spilling is needed
      mayBeSpill(false);
    }
  }

  private void mayBeSpill(boolean shouldBlock) throws IOException {
    if (filledBuffers.size() >= spillLimit) {
      // Do not block; possible that there are more buffers
      scheduleSpill(shouldBlock);
    }
  }

  private boolean scheduleSpill(boolean block) throws IOException {
    if (filledBuffers.isEmpty()) {
      return false;
    }

    try {
      if (block) {
        availableSlots.acquire();
      } else {
        if (!availableSlots.tryAcquire()) {
          // Data in filledBuffers would be spilled in subsequent iteration.
          return false;
        }
      }

      final int filledBufferCount = filledBuffers.size();
      if (LOG.isDebugEnabled() || (filledBufferCount % 10) == 0) {
        LOG.info(destNameTrimmed + ": triggering spill. filledBuffers.size=" + filledBufferCount);
      }
      pendingSpillCount.incrementAndGet();
      int spillNumber = numSpills.getAndIncrement();

      ListenableFuture<SpillResult> future = spillExecutor.submit(new SpillCallable(
          new ArrayList<WrappedBuffer>(filledBuffers), codec, spilledRecordsCounter,
          spillNumber));
      filledBuffers.clear();
      Futures.addCallback(future, new SpillCallback(spillNumber));
      // Update once per buffer (instead of every record)
      updateTezCountersAndNotify();
      return true;
    } catch(InterruptedException ie) {
      Thread.currentThread().interrupt(); // reset interrupt status
    }
    return false;
  }

  private boolean reportPartitionStats() {
    return (sizePerPartition != null);
  }

  private void updateGlobalStats(WrappedBuffer buffer) {
    for (int i = 0; i < numPartitions; i++) {
      numRecordsPerPartition[i] += buffer.recordsPerPartition[i];
      if (reportPartitionStats()) {
        sizePerPartition[i] += buffer.sizePerPartition[i];
      }
    }
  }

  private WrappedBuffer getNextAvailableBuffer() throws IOException {
    if (availableBuffers.peek() == null) {
      if (numInitializedBuffers < numBuffers) {
        buffers[numInitializedBuffers] = new WrappedBuffer(numPartitions,
            numInitializedBuffers == numBuffers - 1 ? lastBufferSize : sizePerBuffer);
        numInitializedBuffers++;
        return buffers[numInitializedBuffers - 1];
      } else {
        // All buffers initialized, and none available right now. Wait
        try {
          // Ensure that spills are triggered so that buffers can be released.
          mayBeSpill(true);
          return availableBuffers.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOInterruptedException("Interrupted while waiting for next buffer", e);
        }
      }
    } else {
      return availableBuffers.poll();
    }
  }

  // All spills using compression for now.
  private class SpillCallable extends CallableWithNdc<SpillResult> {

    private final List<WrappedBuffer> filledBuffers;
    private final CompressionCodec codec;
    private final TezCounter numRecordsCounter;
    private int spillIndex;
    private SpillPathDetails spillPathDetails;
    private int spillNumber;

    public SpillCallable(List<WrappedBuffer> filledBuffers, CompressionCodec codec,
        TezCounter numRecordsCounter, SpillPathDetails spillPathDetails) {
      this(filledBuffers, codec, numRecordsCounter, spillPathDetails.spillIndex);
      Preconditions.checkArgument(spillPathDetails.outputFilePath != null, "Spill output file "
          + "path can not be null");
      this.spillPathDetails = spillPathDetails;
    }

    public SpillCallable(List<WrappedBuffer> filledBuffers, CompressionCodec codec,
        TezCounter numRecordsCounter, int spillNumber) {
      this.filledBuffers = filledBuffers;
      this.codec = codec;
      this.numRecordsCounter = numRecordsCounter;
      this.spillNumber = spillNumber;
    }

    @Override
    protected SpillResult callInternal() throws IOException {
      // This should not be called with an empty buffer. Check before invoking.

      // Number of parallel spills determined by number of threads.
      // Last spill synchronization handled separately.
      SpillResult spillResult = null;
      if (spillPathDetails == null) {
        this.spillPathDetails = getSpillPathDetails(false, -1, spillNumber);
        this.spillIndex = spillPathDetails.spillIndex;
      }
      FSDataOutputStream out = rfs.create(spillPathDetails.outputFilePath);
      TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      DataInputBuffer key = new DataInputBuffer();
      DataInputBuffer val = new DataInputBuffer();
      long compressedLength = 0;
      for (int i = 0; i < numPartitions; i++) {
        IFile.Writer writer = null;
        try {
          long segmentStart = out.getPos();
          long numRecords = 0;
          for (WrappedBuffer buffer : filledBuffers) {
            outputContext.notifyProgress();
            if (buffer.partitionPositions[i] == WrappedBuffer.PARTITION_ABSENT_POSITION) {
              // Skip empty partition.
              continue;
            }
            if (writer == null) {
              writer = new Writer(conf, out, keyClass, valClass, codec, null, null);
            }
            numRecords += writePartition(buffer.partitionPositions[i], buffer, writer, key, val);
          }
          if (writer != null) {
            if (numRecordsCounter != null) {
              // TezCounter is not threadsafe; Since numRecordsCounter would be updated from
              // multiple threads, it is good to synchronize it when incrementing it for correctness.
              synchronized (numRecordsCounter) {
                numRecordsCounter.increment(numRecords);
              }
            }
            writer.close();
            compressedLength += writer.getCompressedLength();
            TezIndexRecord indexRecord = new TezIndexRecord(segmentStart, writer.getRawLength(),
                writer.getCompressedLength());
            spillRecord.putIndex(indexRecord, i);
            writer = null;
          }
        } finally {
          if (writer != null) {
            writer.close();
          }
        }
      }
      key.close();
      val.close();

      spillResult = new SpillResult(compressedLength, this.filledBuffers);

      handleSpillIndex(spillPathDetails, spillRecord);
      LOG.info(destNameTrimmed + ": " + "Finished spill " + spillIndex);

      if (LOG.isDebugEnabled()) {
        LOG.debug(destNameTrimmed + ": " + "Spill=" + spillIndex + ", indexPath="
            + spillPathDetails.indexFilePath + ", outputPath=" + spillPathDetails.outputFilePath);
      }
      return spillResult;
    }
  }

  private long writePartition(int pos, WrappedBuffer wrappedBuffer, Writer writer,
      DataInputBuffer keyBuffer, DataInputBuffer valBuffer) throws IOException {
    long numRecords = 0;
    while (pos != WrappedBuffer.PARTITION_ABSENT_POSITION) {
      int metaIndex = pos / INT_SIZE;
      int keyLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_KEYLEN);
      int valLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_VALLEN);
      keyBuffer.reset(wrappedBuffer.buffer, pos + META_SIZE, keyLength);
      valBuffer.reset(wrappedBuffer.buffer, pos + META_SIZE + keyLength, valLength);

      writer.append(keyBuffer, valBuffer);
      numRecords++;
      pos = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_NEXT);
    }
    return numRecords;
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    int initialMemRequestMb = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB_DEFAULT);
    Preconditions.checkArgument(initialMemRequestMb != 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB + " should be larger than 0");
    long reqBytes = initialMemRequestMb << 20;
    LOG.info("Requested BufferSize (" + TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB
        + ") : " + initialMemRequestMb);
    return reqBytes;
  }

  @Override
  public List<Event> close() throws IOException, InterruptedException {
    // In case there are buffers to be spilled, schedule spilling
    scheduleSpill(true);
    List<Event> eventList = Lists.newLinkedList();
    isShutdown.set(true);
    spillLock.lock();
    try {
      LOG.info(destNameTrimmed + ": " + "Waiting for all spills to complete : Pending : " + pendingSpillCount.get());
      while (pendingSpillCount.get() != 0 && spillException == null) {
        spillInProgress.await();
      }
    } finally {
      spillLock.unlock();
    }
    if (spillException != null) {
      LOG.error(destNameTrimmed + ": " + "Error during spill, throwing");
      // Assuming close will be called on the same thread as the write
      cleanup();
      currentBuffer.cleanup();
      currentBuffer = null;
      if (spillException instanceof IOException) {
        throw (IOException) spillException;
      } else {
        throw new IOException(spillException);
      }
    } else {
      LOG.info(destNameTrimmed + ": " + "All spills complete");
      // Assuming close will be called on the same thread as the write
      cleanup();

      List<Event> events = Lists.newLinkedList();
      if (!pipelinedShuffle) {
        if (skipBuffers) {
          writer.close();
          long rawLen = writer.getRawLength();
          long compLen = writer.getCompressedLength();
          TezIndexRecord rec = new TezIndexRecord(0, rawLen, compLen);
          TezSpillRecord sr = new TezSpillRecord(1);
          sr.putIndex(rec, 0);
          sr.writeToFile(finalIndexPath, conf);

          BitSet emptyPartitions = new BitSet();
          if (outputRecordsCounter.getValue() == 0) {
            emptyPartitions.set(0);
          }
          if (reportPartitionStats()) {
            if (outputRecordsCounter.getValue() > 0) {
              sizePerPartition[0] = rawLen;
            }
          }
          cleanupCurrentBuffer();

          if (outputRecordsCounter.getValue() > 0) {
            outputBytesWithOverheadCounter.increment(rawLen);
            fileOutputBytesCounter.increment(compLen + indexFileSizeEstimate);
          }
          eventList.add(generateVMEvent());
          eventList.add(generateDMEvent(false, -1, false, outputContext
              .getUniqueIdentifier(), emptyPartitions));
          return eventList;
        }

        /*
          1. Final merge enabled
             - When lots of spills are there, mergeAll, generate events and return
             - If there are no existing spills, check for final spill and generate events
          2. Final merge disabled
             - If finalSpill generated data, generate events and return
             - If finalSpill did not generate data, it would automatically populate events
         */
        if (isFinalMergeEnabled) {
          if (numSpills.get() > 0) {
            mergeAll();
          } else {
            finalSpill();
          }
          updateTezCountersAndNotify();
          eventList.add(generateVMEvent());
          eventList.add(generateDMEvent());
        } else {
          // if no data is generated, finalSpill would create VMEvent & add to finalEvents
          SpillResult result = finalSpill();
          if (result != null) {
            updateTezCountersAndNotify();
            // Generate vm event
            finalEvents.add(generateVMEvent());

            // compute empty partitions based on spill result and generate DME
            int spillNum = numSpills.get() - 1;
            SpillCallback callback = new SpillCallback(spillNum);
            callback.computePartitionStats(result);
            BitSet emptyPartitions = getEmptyPartitions(callback.getRecordsPerPartition());
            String pathComponent = generatePathComponent(outputContext.getUniqueIdentifier(), spillNum);
            Event finalEvent = generateDMEvent(true, spillNum,
                true, pathComponent, emptyPartitions);
            finalEvents.add(finalEvent);
          }
          //all events to be sent out are in finalEvents.
          eventList.addAll(finalEvents);
        }
        cleanupCurrentBuffer();
        return eventList;
      }

      //For pipelined case, send out an event in case finalspill generated a spill file.
      if (finalSpill() != null) {
        // VertexManagerEvent is only sent at the end and thus sizePerPartition is used
        // for the sum of all spills.
        mayBeSendEventsForSpill(currentBuffer.recordsPerPartition,
            sizePerPartition, numSpills.get() - 1, true);
      }
      updateTezCountersAndNotify();
      cleanupCurrentBuffer();
      return events;
    }
  }

  private BitSet getEmptyPartitions(int[] recordsPerPartition) {
    Preconditions.checkArgument(recordsPerPartition != null, "records per partition can not be null");
    BitSet emptyPartitions = new BitSet();
    for (int i = 0; i < numPartitions; i++) {
      if (recordsPerPartition[i] == 0 ) {
        emptyPartitions.set(i);
      }
    }
    return emptyPartitions;
  }

  public boolean reportDetailedPartitionStats() {
    return reportPartitionStats.isPrecise();
  }

  private Event generateVMEvent() throws IOException {
    return ShuffleUtils.generateVMEvent(outputContext, this.sizePerPartition,
        this.reportDetailedPartitionStats(), deflater.get());
  }

  private Event generateDMEvent() throws IOException {
    BitSet emptyPartitions = getEmptyPartitions(numRecordsPerPartition);
    return generateDMEvent(false, -1, false, outputContext.getUniqueIdentifier(), emptyPartitions);
  }

  private Event generateDMEvent(boolean addSpillDetails, int spillId,
      boolean isLastSpill, String pathComponent, BitSet emptyPartitions)
      throws IOException {

    outputContext.notifyProgress();
    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();

    String host = getHost();
    if (emptyPartitions.cardinality() != 0) {
      // Empty partitions exist
      ByteString emptyPartitionsByteString =
          TezCommonUtils.compressByteArrayToByteString(TezUtilsInternal.toByteArray
              (emptyPartitions), deflater.get());
      payloadBuilder.setEmptyPartitions(emptyPartitionsByteString);
    }

    if (emptyPartitions.cardinality() != numPartitions) {
      // Populate payload only if at least 1 partition has data
      payloadBuilder.setHost(host);
      payloadBuilder.setPort(getShufflePort());
      payloadBuilder.setPathComponent(pathComponent);
    }

    if (addSpillDetails) {
      payloadBuilder.setSpillId(spillId);
      payloadBuilder.setLastEvent(isLastSpill);
    }

    ByteBuffer payload = payloadBuilder.build().toByteString().asReadOnlyByteBuffer();
    return CompositeDataMovementEvent.create(0, numPartitions, payload);
  }

  private void cleanupCurrentBuffer() {
    currentBuffer.cleanup();
    currentBuffer = null;
  }

  private void cleanup() {
    if (spillExecutor != null) {
      spillExecutor.shutdownNow();
    }
    for (int i = 0; i < buffers.length; i++) {
      if (buffers[i] != null && buffers[i] != currentBuffer) {
        buffers[i].cleanup();
        buffers[i] = null;
      }
    }
    availableBuffers.clear();
  }

  private SpillResult finalSpill() throws IOException {
    if (currentBuffer.nextPosition == 0) {
      if (pipelinedShuffle || !isFinalMergeEnabled) {
        List<Event> eventList = Lists.newLinkedList();
        eventList.add(ShuffleUtils.generateVMEvent(outputContext,
            reportPartitionStats() ? new long[numPartitions] : null,
            reportDetailedPartitionStats(), deflater.get()));
        if (localOutputRecordsCounter == 0 && outputLargeRecordsCounter.getValue() == 0) {
          // Should send this event (all empty partitions) only when no records are written out.
          BitSet emptyPartitions = new BitSet(numPartitions);
          emptyPartitions.flip(0, numPartitions);
          eventList.add(generateDMEvent(true, numSpills.get(), true,
              null, emptyPartitions));
        }
        if (pipelinedShuffle) {
          outputContext.sendEvents(eventList);
        } else if (!isFinalMergeEnabled) {
          finalEvents.addAll(0, eventList);
        }
      }
      return null;
    } else {
      updateGlobalStats(currentBuffer);
      filledBuffers.add(currentBuffer);

      //setup output file and index file
      SpillPathDetails spillPathDetails = getSpillPathDetails(true, -1);
      SpillCallable spillCallable = new SpillCallable(filledBuffers,
          codec, null, spillPathDetails);
      try {
        SpillResult spillResult = spillCallable.call();

        fileOutputBytesCounter.increment(spillResult.spillSize);
        fileOutputBytesCounter.increment(indexFileSizeEstimate);
        return spillResult;
      } catch (Exception ex) {
        throw (ex instanceof IOException) ? (IOException)ex : new IOException(ex);
      }
    }

  }

  /**
   * Set up spill output file, index file details.
   *
   * @param isFinalSpill
   * @param expectedSpillSize
   * @return SpillPathDetails
   * @throws IOException
   */
  private SpillPathDetails getSpillPathDetails(boolean isFinalSpill, long expectedSpillSize)
      throws IOException {
    int spillNumber = numSpills.getAndIncrement();
    return getSpillPathDetails(isFinalSpill, expectedSpillSize, spillNumber);
  }

  /**
   * Set up spill output file, index file details.
   *
   * @param isFinalSpill
   * @param expectedSpillSize
   * @param spillNumber
   * @return SpillPathDetails
   * @throws IOException
   */
  private SpillPathDetails getSpillPathDetails(boolean isFinalSpill, long expectedSpillSize,
      int spillNumber) throws IOException {
    long spillSize = (expectedSpillSize < 0) ?
        (currentBuffer.nextPosition + numPartitions * APPROX_HEADER_LENGTH) : expectedSpillSize;

    Path outputFilePath = null;
    Path indexFilePath = null;

    if (!pipelinedShuffle && isFinalMergeEnabled) {
      if (isFinalSpill) {
        outputFilePath = outputFileHandler.getOutputFileForWrite(spillSize);
        indexFilePath = outputFileHandler.getOutputIndexFileForWrite(indexFileSizeEstimate);

        //Setting this for tests
        finalOutPath = outputFilePath;
        finalIndexPath = indexFilePath;
      } else {
        outputFilePath = outputFileHandler.getSpillFileForWrite(spillNumber, spillSize);
      }
    } else {
      outputFilePath = outputFileHandler.getSpillFileForWrite(spillNumber, spillSize);
      indexFilePath  = outputFileHandler.getSpillIndexFileForWrite(spillNumber, indexFileSizeEstimate);
    }

    return new SpillPathDetails(outputFilePath, indexFilePath, spillNumber);
  }

  private void mergeAll() throws IOException {
    long expectedSize = spilledSize;
    if (currentBuffer.nextPosition != 0) {
      expectedSize += currentBuffer.nextPosition - (currentBuffer.numRecords * META_SIZE)
          - currentBuffer.skipSize + numPartitions * APPROX_HEADER_LENGTH;
      // Update final statistics.
      updateGlobalStats(currentBuffer);
    }

    SpillPathDetails spillPathDetails = getSpillPathDetails(true, expectedSize);
    finalIndexPath = spillPathDetails.indexFilePath;
    finalOutPath = spillPathDetails.outputFilePath;

    TezSpillRecord finalSpillRecord = new TezSpillRecord(numPartitions);

    DataInputBuffer keyBuffer = new DataInputBuffer();
    DataInputBuffer valBuffer = new DataInputBuffer();

    DataInputBuffer keyBufferIFile = new DataInputBuffer();
    DataInputBuffer valBufferIFile = new DataInputBuffer();

    FSDataOutputStream out = null;
    try {
      out = rfs.create(finalOutPath);
      Writer writer = null;

      for (int i = 0; i < numPartitions; i++) {
        long segmentStart = out.getPos();
        if (numRecordsPerPartition[i] == 0) {
          LOG.info(destNameTrimmed + ": " + "Skipping partition: " + i + " in final merge since it has no records");
          continue;
        }
        writer = new Writer(conf, out, keyClass, valClass, codec, null, null);
        try {
          if (currentBuffer.nextPosition != 0
              && currentBuffer.partitionPositions[i] != WrappedBuffer.PARTITION_ABSENT_POSITION) {
            // Write current buffer.
            writePartition(currentBuffer.partitionPositions[i], currentBuffer, writer, keyBuffer,
                valBuffer);
          }
          synchronized (spillInfoList) {
            for (SpillInfo spillInfo : spillInfoList) {
              TezIndexRecord indexRecord = spillInfo.spillRecord.getIndex(i);
              if (indexRecord.getPartLength() == 0) {
                // Skip empty partitions within a spill
                continue;
              }
              FSDataInputStream in = rfs.open(spillInfo.outPath);
              in.seek(indexRecord.getStartOffset());
              IFile.Reader reader = new IFile.Reader(in, indexRecord.getPartLength(), codec, null,
                  additionalSpillBytesReadCounter, ifileReadAhead, ifileReadAheadLength,
                  ifileBufferSize);
              while (reader.nextRawKey(keyBufferIFile)) {
                // TODO Inefficient. If spills are not compressed, a direct copy should be possible
                // given the current IFile format. Also exteremely inefficient for large records,
                // since the entire record will be read into memory.
                reader.nextRawValue(valBufferIFile);
                writer.append(keyBufferIFile, valBufferIFile);
              }
              reader.close();
            }
          }
          writer.close();
          fileOutputBytesCounter.increment(writer.getCompressedLength());
          TezIndexRecord indexRecord = new TezIndexRecord(segmentStart, writer.getRawLength(),
              writer.getCompressedLength());
          writer = null;
          finalSpillRecord.putIndex(indexRecord, i);
          outputContext.notifyProgress();
        } finally {
          if (writer != null) {
            writer.close();
          }
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
      deleteIntermediateSpills();
    }
    finalSpillRecord.writeToFile(finalIndexPath, conf);
    fileOutputBytesCounter.increment(indexFileSizeEstimate);
    LOG.info(destNameTrimmed + ": " + "Finished final spill after merging : " + numSpills.get() + " spills");
  }

  private void deleteIntermediateSpills() {
    // Delete the intermediate spill files
    synchronized (spillInfoList) {
      for (SpillInfo spill : spillInfoList) {
        try {
          rfs.delete(spill.outPath, false);
        } catch (IOException e) {
          LOG.warn("Unable to delete intermediate spill " + spill.outPath, e);
        }
      }
    }
  }

  private void writeLargeRecord(final Object key, final Object value, final int partition)
      throws IOException {
    numAdditionalSpillsCounter.increment(1);
    long size = sizePerBuffer - (currentBuffer.numRecords * META_SIZE) - currentBuffer.skipSize
        + numPartitions * APPROX_HEADER_LENGTH;
    SpillPathDetails spillPathDetails = getSpillPathDetails(false, size);
    int spillIndex = spillPathDetails.spillIndex;
    FSDataOutputStream out = null;
    long outSize = 0;
    try {
      final TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      final Path outPath = spillPathDetails.outputFilePath;
      out = rfs.create(outPath);
      BitSet emptyPartitions = null;
      if (pipelinedShuffle || !isFinalMergeEnabled) {
        emptyPartitions = new BitSet(numPartitions);
      }
      for (int i = 0; i < numPartitions; i++) {
        final long recordStart = out.getPos();
        if (i == partition) {
          spilledRecordsCounter.increment(1);
          Writer writer = null;
          try {
            writer = new IFile.Writer(conf, out, keyClass, valClass, codec, null, null);
            writer.append(key, value);
            outputLargeRecordsCounter.increment(1);
            numRecordsPerPartition[i]++;
            if (reportPartitionStats()) {
              sizePerPartition[i] += writer.getRawLength();
            }
            writer.close();
            synchronized (additionalSpillBytesWritternCounter) {
              additionalSpillBytesWritternCounter.increment(writer.getCompressedLength());
            }
            TezIndexRecord indexRecord = new TezIndexRecord(recordStart, writer.getRawLength(),
                writer.getCompressedLength());
            spillRecord.putIndex(indexRecord, i);
            outSize = writer.getCompressedLength();
            writer = null;
          } finally {
            if (writer != null) {
              writer.close();
            }
          }
        } else {
          if (emptyPartitions != null) {
            emptyPartitions.set(i);
          }
        }
      }
      handleSpillIndex(spillPathDetails, spillRecord);

      mayBeSendEventsForSpill(emptyPartitions, sizePerPartition,
          spillIndex, false);

      LOG.info(destNameTrimmed + ": " + "Finished writing large record of size " + outSize + " to spill file " + spillIndex);
      if (LOG.isDebugEnabled()) {
        LOG.debug(destNameTrimmed + ": " + "LargeRecord Spill=" + spillIndex + ", indexPath="
            + spillPathDetails.indexFilePath + ", outputPath="
            + spillPathDetails.outputFilePath);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  private void handleSpillIndex(SpillPathDetails spillPathDetails, TezSpillRecord spillRecord)
      throws IOException {
    if (spillPathDetails.indexFilePath != null) {
      //write the index record
      spillRecord.writeToFile(spillPathDetails.indexFilePath, conf);
    } else {
      //add to cache
      SpillInfo spillInfo = new SpillInfo(spillRecord, spillPathDetails.outputFilePath);
      spillInfoList.add(spillInfo);
      numAdditionalSpillsCounter.increment(1);
    }
  }

  private class ByteArrayOutputStream extends OutputStream {

    private final byte[] scratch = new byte[1];

    @Override
    public void write(int v) throws IOException {
      scratch[0] = (byte) v;
      write(scratch, 0, 1);
    }

    public void write(byte[] b, int off, int len) throws IOException {
      if (currentBuffer.full) {
          /* no longer do anything until reset */
      } else if (len > currentBuffer.availableSize) {
        currentBuffer.full = true; /* stop working & signal we hit the end */
      } else {
        System.arraycopy(b, off, currentBuffer.buffer, currentBuffer.nextPosition, len);
        currentBuffer.nextPosition += len;
        currentBuffer.availableSize -= len;
      }
    }
  }

  private static class WrappedBuffer {

    private static final int PARTITION_ABSENT_POSITION = -1;

    private final int[] partitionPositions;
    private final int[] recordsPerPartition;
    // uncompressed size for each partition
    private final long[] sizePerPartition;
    private final int numPartitions;
    private final int size;

    private byte[] buffer;
    private IntBuffer metaBuffer;

    private int numRecords = 0;
    private int skipSize = 0;

    private int nextPosition = 0;
    private int availableSize;
    private boolean full = false;

    WrappedBuffer(int numPartitions, int size) {
      this.partitionPositions = new int[numPartitions];
      this.recordsPerPartition = new int[numPartitions];
      this.sizePerPartition = new long[numPartitions];
      this.numPartitions = numPartitions;
      for (int i = 0; i < numPartitions; i++) {
        this.partitionPositions[i] = PARTITION_ABSENT_POSITION;
        this.recordsPerPartition[i] = 0;
        this.sizePerPartition[i] = 0;
      }
      size = size - (size % INT_SIZE);
      this.size = size;
      this.buffer = new byte[size];
      this.metaBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.nativeOrder()).asIntBuffer();
      availableSize = size;
    }

    void reset() {
      for (int i = 0; i < numPartitions; i++) {
        this.partitionPositions[i] = PARTITION_ABSENT_POSITION;
        this.recordsPerPartition[i] = 0;
        this.sizePerPartition[i] = 0;
      }
      numRecords = 0;
      nextPosition = 0;
      skipSize = 0;
      availableSize = size;
      full = false;
    }

    void cleanup() {
      buffer = null;
      metaBuffer = null;
    }
  }

  private String generatePathComponent(String uniqueId, int spillNumber) {
    return (uniqueId + "_" + spillNumber);
  }

  private List<Event> generateEventForSpill(BitSet emptyPartitions, long[] sizePerPartition,
      int spillNumber,
      boolean isFinalUpdate) throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    //Send out an event for consuming.
    String pathComponent = generatePathComponent(outputContext.getUniqueIdentifier(), spillNumber);
    if (isFinalUpdate) {
      eventList.add(ShuffleUtils.generateVMEvent(outputContext,
          sizePerPartition, reportDetailedPartitionStats(), deflater.get()));
    }
    Event compEvent = generateDMEvent(true, spillNumber, isFinalUpdate,
        pathComponent, emptyPartitions);
    eventList.add(compEvent);
    return eventList;
  }

  private void mayBeSendEventsForSpill(
      BitSet emptyPartitions, long[] sizePerPartition,
      int spillNumber, boolean isFinalUpdate) {
    if (!pipelinedShuffle) {
      if (isFinalMergeEnabled) {
        return;
      }
    }
    List<Event> events = null;
    try {
      events = generateEventForSpill(emptyPartitions, sizePerPartition, spillNumber,
          isFinalUpdate);
      LOG.info(destNameTrimmed + ": " + "Adding spill event for spill"
          + " (final update=" + isFinalUpdate + "), spillId=" + spillNumber);
      if (pipelinedShuffle) {
        //Send out an event for consuming.
        outputContext.sendEvents(events);
      } else if (!isFinalMergeEnabled) {
        this.finalEvents.addAll(events);
      }
    } catch (IOException e) {
      LOG.error(destNameTrimmed + ": " + "Error in sending pipelined events", e);
      outputContext.reportFailure(TaskFailureType.NON_FATAL, e,
          "Error in sending events.");
    }
  }

  private void mayBeSendEventsForSpill(int[] recordsPerPartition,
      long[] sizePerPartition, int spillNumber, boolean isFinalUpdate) {
    BitSet emptyPartitions = getEmptyPartitions(recordsPerPartition);
    mayBeSendEventsForSpill(emptyPartitions, sizePerPartition, spillNumber,
        isFinalUpdate);
  }

  private class SpillCallback implements FutureCallback<SpillResult> {

    private final int spillNumber;
    private int recordsPerPartition[];
    private long sizePerPartition[];

    SpillCallback(int spillNumber) {
      this.spillNumber = spillNumber;
    }

    void computePartitionStats(SpillResult result) {
      if (result.filledBuffers.size() == 1) {
        recordsPerPartition = result.filledBuffers.get(0).recordsPerPartition;
        sizePerPartition = result.filledBuffers.get(0).sizePerPartition;
      } else {
        recordsPerPartition = new int[numPartitions];
        sizePerPartition = new long[numPartitions];
        for (WrappedBuffer buffer : result.filledBuffers) {
          for (int i = 0; i < numPartitions; ++i) {
            recordsPerPartition[i] += buffer.recordsPerPartition[i];
            sizePerPartition[i] += buffer.sizePerPartition[i];
          }
        }
      }
    }

    int[] getRecordsPerPartition() {
      return recordsPerPartition;
    }

    @Override
    public void onSuccess(SpillResult result) {
      synchronized (UnorderedPartitionedKVWriter.this) {
        spilledSize += result.spillSize;
      }

      computePartitionStats(result);

      mayBeSendEventsForSpill(recordsPerPartition, sizePerPartition, spillNumber, false);

      try {
        for (WrappedBuffer buffer : result.filledBuffers) {
          buffer.reset();
          availableBuffers.add(buffer);
        }
      } catch (Throwable e) {
        LOG.error(destNameTrimmed + ": Failure while attempting to reset buffer after spill", e);
        outputContext.reportFailure(TaskFailureType.NON_FATAL, e, "Failure while attempting to reset buffer after spill");
      }

      if (!pipelinedShuffle && isFinalMergeEnabled) {
        synchronized(additionalSpillBytesWritternCounter) {
          additionalSpillBytesWritternCounter.increment(result.spillSize);
        }
      } else {
        synchronized(fileOutputBytesCounter) {
          fileOutputBytesCounter.increment(indexFileSizeEstimate);
          fileOutputBytesCounter.increment(result.spillSize);
        }
      }

      spillLock.lock();
      try {
        if (pendingSpillCount.decrementAndGet() == 0) {
          spillInProgress.signal();
        }
      } finally {
        spillLock.unlock();
        availableSlots.release();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // spillException setup to throw an exception back to the user. Requires synchronization.
      // Consider removing it in favor of having Tez kill the task
      LOG.error(destNameTrimmed + ": " + "Failure while spilling to disk", t);
      spillException = t;
      outputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Failure while spilling to disk");
      spillLock.lock();
      try {
        spillInProgress.signal();
      } finally {
        spillLock.unlock();
        availableSlots.release();
      }
    }
  }

  private static class SpillResult {
    final long spillSize;
    final List<WrappedBuffer> filledBuffers;

    SpillResult(long size, List<WrappedBuffer> filledBuffers) {
      this.spillSize = size;
      this.filledBuffers = filledBuffers;
    }
  }

  @VisibleForTesting
  static class SpillInfo {
    final TezSpillRecord spillRecord;
    final Path outPath;

    SpillInfo(TezSpillRecord spillRecord, Path outPath) {
      this.spillRecord = spillRecord;
      this.outPath = outPath;
    }
  }

  @VisibleForTesting
  String getHost() {
    return outputContext.getExecutionContext().getHostName();
  }

  @VisibleForTesting
  int getShufflePort() throws IOException {
    String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    ByteBuffer shuffleMetadata = outputContext
        .getServiceProviderMetaData(auxiliaryService);
    int shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);
    return shufflePort;
  }

  @InterfaceAudience.Private
  static class SpillPathDetails {
    final Path indexFilePath;
    final Path outputFilePath;
    final int spillIndex;

    SpillPathDetails(Path outputFilePath, Path indexFilePath, int spillIndex) {
      this.outputFilePath = outputFilePath;
      this.indexFilePath = indexFilePath;
      this.spillIndex = spillIndex;
    }
  }
}
