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

import java.io.DataOutputStream;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

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

  private static final Log LOG = LogFactory.getLog(UnorderedPartitionedKVWriter.class);

  private static final int INT_SIZE = 4;
  private static final int NUM_META = 3; // Number of meta fields.
  private static final int INDEX_KEYLEN = 0; // KeyLength index
  private static final int INDEX_VALLEN = 1; // ValLength index
  private static final int INDEX_NEXT = 2; // Next Record Index.
  private static final int META_SIZE = NUM_META * INT_SIZE; // Size of total meta-data

  private final static int APPROX_HEADER_LENGTH = 150;

  // Maybe setup a separate statistics class which can be shared between the
  // buffer and the main path instead of having multiple arrays.

  private final long availableMemory;
  @VisibleForTesting
  final WrappedBuffer[] buffers;
  @VisibleForTesting
  final BlockingQueue<WrappedBuffer> availableBuffers;
  private final ByteArrayOutputStream baos;
  private final DataOutputStream dos;
  @VisibleForTesting
  WrappedBuffer currentBuffer;
  private final FileSystem rfs;

  private final List<SpillInfo> spillInfoList = Collections
      .synchronizedList(new ArrayList<SpillInfo>());

  private final ListeningExecutorService spillExecutor;

  private final int[] numRecordsPerPartition;
  private volatile long spilledSize = 0;

  /**
   * Represents final number of records written (spills are not counted)
   */
  protected final TezCounter outputLargeRecordsCounter;

  @VisibleForTesting
  int numBuffers;
  @VisibleForTesting
  int sizePerBuffer;
  @VisibleForTesting
  int numInitializedBuffers;

  private Throwable spillException;
  private AtomicBoolean isShutdown = new AtomicBoolean(false);
  @VisibleForTesting
  final AtomicInteger numSpills = new AtomicInteger(0);
  private final AtomicInteger pendingSpillCount = new AtomicInteger(0);

  private final ReentrantLock spillLock = new ReentrantLock();
  private final Condition spillInProgress = spillLock.newCondition();

  public UnorderedPartitionedKVWriter(OutputContext outputContext, Configuration conf,
      int numOutputs, long availableMemoryBytes) throws IOException {
    super(outputContext, conf, numOutputs);
    Preconditions.checkArgument(availableMemoryBytes > 0, "availableMemory should not be > 0 bytes");
    // Ideally, should be significantly larger.
    availableMemory = availableMemoryBytes;

    // Allow unit tests to control the buffer sizes.
    int maxSingleBufferSizeBytes = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES, Integer.MAX_VALUE);
    computeNumBuffersAndSize(maxSingleBufferSizeBytes);
    LOG.info("Running with numBuffers=" + numBuffers + ", sizePerBuffer=" + sizePerBuffer);
    availableBuffers = new LinkedBlockingQueue<WrappedBuffer>();
    buffers = new WrappedBuffer[numBuffers];
    // Set up only the first buffer to start with.
    buffers[0] = new WrappedBuffer(numOutputs, sizePerBuffer);
    numInitializedBuffers = 1;
    LOG.info("Initialize Buffer #" + numInitializedBuffers + " with size=" + sizePerBuffer);
    currentBuffer = buffers[0];
    baos = new ByteArrayOutputStream();
    dos = new DataOutputStream(baos);
    keySerializer.open(dos);
    valSerializer.open(dos);
    rfs = ((LocalFileSystem) FileSystem.getLocal(this.conf)).getRaw();

    ExecutorService executor = Executors.newFixedThreadPool(
        1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(
                "UnorderedOutSpiller ["
                    + TezUtils.cleanVertexName(outputContext.getDestinationVertexName()) + "]")
            .build());
    spillExecutor = MoreExecutors.listeningDecorator(executor);
    numRecordsPerPartition = new int[numPartitions];

    outputLargeRecordsCounter = outputContext.getCounters().findCounter(
        TaskCounter.OUTPUT_LARGE_RECORDS);
  }

  private void computeNumBuffersAndSize(int bufferLimit) {
    numBuffers = Math.max(2, (int) (availableMemory / bufferLimit)
        + ((availableMemory % bufferLimit) == 0 ? 0 : 1));
    sizePerBuffer = (int) (availableMemory / numBuffers);
    sizePerBuffer = sizePerBuffer - (sizePerBuffer % INT_SIZE);
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
    int partition = partitioner.getPartition(key, value, numPartitions);
    write(key, value, partition);
  }

  @SuppressWarnings("unchecked")
  private void write(Object key, Object value, int partition) throws IOException {
    // Wrap to 4 byte (Int) boundary for metaData
    int mod = currentBuffer.nextPosition % INT_SIZE;
    int metaSkip = mod == 0 ? 0 : (INT_SIZE - mod);
    if (currentBuffer.availableSize < (META_SIZE + metaSkip)) {
      // Move over to the next buffer.
      metaSkip = 0;
      setupNextBuffer();
    }
    currentBuffer.nextPosition += metaSkip;
    int metaStart = currentBuffer.nextPosition;
    currentBuffer.availableSize -= (META_SIZE + metaSkip);
    currentBuffer.nextPosition += META_SIZE;
    try {
      keySerializer.serialize(key);
    } catch (BufferTooSmallException e) {
      if (metaStart == 0) { // Started writing at the start of the buffer. Write Key to disk.
        // Key too large for any buffer. Write entire record to disk.
        currentBuffer.reset();
        writeLargeRecord(key, value, partition, numSpills.incrementAndGet());
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
    try {
      valSerializer.serialize(value);
    } catch (BufferTooSmallException e) {
      // Value too large for current buffer, or K-V too large for entire buffer.
      if (metaStart == 0) {
        // Key + Value too large for a single buffer.
        currentBuffer.reset();
        writeLargeRecord(key, value, partition, numSpills.incrementAndGet());
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
    outputRecordBytesCounter.increment(currentBuffer.nextPosition - (metaStart + META_SIZE));
    outputBytesWithOverheadCounter.increment((currentBuffer.nextPosition - metaStart) + metaSkip);
    outputRecordsCounter.increment(1);
    currentBuffer.partitionPositions[partition] = metaStart;
    currentBuffer.recordsPerPartition[partition]++;
    currentBuffer.numRecords++;

  }

  private void setupNextBuffer() throws IOException {

    if (currentBuffer.numRecords == 0) {
      currentBuffer.reset();
    } else {
      // Update overall stats
      LOG.info("Moving to next buffer and triggering spill");
      updateGlobalStats(currentBuffer);

      pendingSpillCount.incrementAndGet();

      ListenableFuture<SpillResult> future = spillExecutor.submit(new SpillCallable(currentBuffer,
          numSpills.incrementAndGet(), codec, spilledRecordsCounter, false));
      Futures.addCallback(future, new SpillCallback(numSpills.get()));

      WrappedBuffer wb = getNextAvailableBuffer();
      currentBuffer = wb;
    }
  }

  private void updateGlobalStats(WrappedBuffer buffer) {
    for (int i = 0; i < numPartitions; i++) {
      numRecordsPerPartition[i] += buffer.recordsPerPartition[i];
    }
  }

  private WrappedBuffer getNextAvailableBuffer() throws IOException {
    if (availableBuffers.peek() == null) {
      if (numInitializedBuffers < numBuffers) {
        buffers[numInitializedBuffers] = new WrappedBuffer(numPartitions, sizePerBuffer);
        numInitializedBuffers++;
        return buffers[numInitializedBuffers - 1];
      } else {
        // All buffers initialized, and none available right now. Wait
        try {
          return availableBuffers.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for next buffer", e);
        }
      }
    } else {
      return availableBuffers.poll();
    }
  }

  // All spills using compression for now.
  private class SpillCallable implements Callable<SpillResult> {

    private final WrappedBuffer wrappedBuffer;
    private final CompressionCodec codec;
    private final TezCounter numRecordsCounter;
    private final int spillNumber;
    private final boolean isFinalSpill;

    public SpillCallable(WrappedBuffer wrappedBuffer, int spillNumber, CompressionCodec codec,
        TezCounter numRecordsCounter, boolean isFinal) {
      this.wrappedBuffer = wrappedBuffer;
      this.codec = codec;
      this.numRecordsCounter = numRecordsCounter;
      this.spillNumber = spillNumber;
      this.isFinalSpill = isFinal;
    }

    @Override
    public SpillResult call() throws IOException {
      // This should not be called with an empty buffer. Check before invoking.

      // Number of parallel spills determined by number of threads.
      // Last spill synchronization handled separately.
      SpillResult spillResult = null;
      long spillSize = wrappedBuffer.nextPosition + numPartitions * APPROX_HEADER_LENGTH;
      Path outPath = null;
      if (isFinalSpill) {
        outPath = outputFileHandler.getOutputFileForWrite(spillSize);
      } else {
        outPath = outputFileHandler.getSpillFileForWrite(spillNumber, spillSize);
      }
      FSDataOutputStream out = rfs.create(outPath);
      TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      DataInputBuffer key = new DataInputBuffer();
      DataInputBuffer val = new DataInputBuffer();
      for (int i = 0; i < numPartitions; i++) {
        IFile.Writer writer = null;
        try {
          long segmentStart = out.getPos();
          if (wrappedBuffer.partitionPositions[i] == WrappedBuffer.PARTITION_ABSENT_POSITION) {
            // Skip empty partition.
            continue;
          }
          writer = new Writer(conf, out, keyClass, valClass, codec, numRecordsCounter, null);
          writePartition(wrappedBuffer.partitionPositions[i], wrappedBuffer, writer, key, val);
          writer.close();
          if (isFinalSpill) {
            fileOutputBytesCounter.increment(writer.getCompressedLength());
          } else {
            additionalSpillBytesWritternCounter.increment(writer.getCompressedLength());
          }
          spillResult = new SpillResult(writer.getCompressedLength(), this.wrappedBuffer);
          TezIndexRecord indexRecord = new TezIndexRecord(segmentStart, writer.getRawLength(),
              writer.getCompressedLength());
          spillRecord.putIndex(indexRecord, i);
          writer = null;
        } finally {
          if (writer != null) {
            writer.close();
          }
        }
      }
      if (isFinalSpill) {
        long indexFileSizeEstimate = numPartitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;
        Path finalSpillFile = outputFileHandler.getOutputIndexFileForWrite(indexFileSizeEstimate);
        spillRecord.writeToFile(finalSpillFile, conf);
        fileOutputBytesCounter.increment(indexFileSizeEstimate);
        LOG.info("Finished final and only spill");
      } else {
        SpillInfo spillInfo = new SpillInfo(spillRecord, outPath);
        spillInfoList.add(spillInfo);
        numAdditionalSpillsCounter.increment(1);
        LOG.info("Finished spill " + spillNumber);
      }
      return spillResult;
    }
  }

  private void writePartition(int pos, WrappedBuffer wrappedBuffer, Writer writer,
      DataInputBuffer keyBuffer, DataInputBuffer valBuffer) throws IOException {
    while (pos != WrappedBuffer.PARTITION_ABSENT_POSITION) {
      int metaIndex = pos / INT_SIZE;
      int keyLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_KEYLEN);
      int valLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_VALLEN);
      keyBuffer.reset(wrappedBuffer.buffer, pos + META_SIZE, keyLength);
      valBuffer.reset(wrappedBuffer.buffer, pos + META_SIZE + keyLength, valLength);

      writer.append(keyBuffer, valBuffer);
      pos = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_NEXT);
    }
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    int initialMemRequestMb = conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB,
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
    isShutdown.set(true);
    spillLock.lock();
    LOG.info("Waiting for all spills to complete : Pending : " + pendingSpillCount.get());
    try {
      while (pendingSpillCount.get() != 0 && spillException == null) {
        spillInProgress.await();
      }
    } finally {
      spillLock.unlock();
    }
    if (spillException != null) {
      LOG.fatal("Error during spill, throwing");
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
      LOG.info("All spills complete");
      // Assuming close will be called on the same thread as the write
      cleanup();
      if (numSpills.get() > 0) {
        mergeAll();
      } else {
        finalSpill();
      }

      currentBuffer.cleanup();
      currentBuffer = null;
    }

    return Collections.singletonList(generateEvent());
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

  private Event generateEvent() throws IOException {
    DataMovementEventPayloadProto.Builder payloadBuidler = DataMovementEventPayloadProto
        .newBuilder();

    String host = getHost();
    int shufflePort = getShufflePort();

    BitSet emptyPartitions = new BitSet();
    for (int i = 0; i < numPartitions; i++) {
      if (numRecordsPerPartition[i] == 0) {
        emptyPartitions.set(i);
      }
    }
    if (emptyPartitions.cardinality() != 0) {
      // Empty partitions exist
      ByteString emptyPartitionsByteString = TezCommonUtils.compressByteArrayToByteString(TezUtils
          .toByteArray(emptyPartitions));
      payloadBuidler.setEmptyPartitions(emptyPartitionsByteString);
    }
    if (emptyPartitions.cardinality() != numPartitions) {
      // Populate payload only if at least 1 partition has data
      payloadBuidler.setHost(host);
      payloadBuidler.setPort(shufflePort);
      payloadBuidler.setPathComponent(outputContext.getUniqueIdentifier());
    }

    CompositeDataMovementEvent cDme = new CompositeDataMovementEvent(0, numPartitions,
        payloadBuidler.build().toByteArray());
    return cDme;
  }

  private void finalSpill() throws IOException {
    if (currentBuffer.nextPosition == 0) {
      return;
    } else {
      updateGlobalStats(currentBuffer);
      SpillCallable spillCallable = new SpillCallable(currentBuffer, 0, codec, null, true);
      spillCallable.call();
      return;
    }

  }

  private void mergeAll() throws IOException {
    long expectedSize = spilledSize;
    if (currentBuffer.nextPosition != 0) {
      expectedSize += currentBuffer.nextPosition - (currentBuffer.numRecords * META_SIZE)
          - currentBuffer.skipSize + numPartitions * APPROX_HEADER_LENGTH;
      // Update final statistics.
      updateGlobalStats(currentBuffer);
    }

    long indexFileSizeEstimate = numPartitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;
    Path finalOutPath = outputFileHandler.getOutputFileForWrite(expectedSize);
    Path finalIndexPath = outputFileHandler.getOutputIndexFileForWrite(indexFileSizeEstimate);

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
          LOG.info("Skipping partition: " + i + " in final merge since it has no records");
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
    }
    finalSpillRecord.writeToFile(finalIndexPath, conf);
    fileOutputBytesCounter.increment(indexFileSizeEstimate);
    LOG.info("Finished final spill after merging : " + numSpills.get() + " spills");
  }

  private void writeLargeRecord(final Object key, final Object value, final int partition,
      final int spillNumber) throws IOException {
    numAdditionalSpillsCounter.increment(1);
    long size = sizePerBuffer - (currentBuffer.numRecords * META_SIZE) - currentBuffer.skipSize
        + numPartitions * APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    long outSize = 0;
    try {
      final TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      final Path outPath = outputFileHandler.getSpillFileForWrite(spillNumber, size);
      out = rfs.create(outPath);
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
            writer.close();
            additionalSpillBytesWritternCounter.increment(writer.getCompressedLength());
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
        }
      }
      SpillInfo spillInfo = new SpillInfo(spillRecord, outPath);
      spillInfoList.add(spillInfo);
      LOG.info("Finished writing large record of size " + outSize + " to spill file " + spillNumber);
    } finally {
      if (out != null) {
        out.close();
      }
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
      if (len > currentBuffer.availableSize) {
        throw new BufferTooSmallException();
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
    private final int numPartitions;
    private final int size;

    private byte[] buffer;
    private IntBuffer metaBuffer;

    private int numRecords = 0;
    private int skipSize = 0;

    private int nextPosition = 0;
    private int availableSize;

    WrappedBuffer(int numPartitions, int size) {
      this.partitionPositions = new int[numPartitions];
      this.recordsPerPartition = new int[numPartitions];
      this.numPartitions = numPartitions;
      for (int i = 0; i < numPartitions; i++) {
        this.partitionPositions[i] = PARTITION_ABSENT_POSITION;
        this.recordsPerPartition[i] = 0;
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
      }
      numRecords = 0;
      nextPosition = 0;
      skipSize = 0;
      availableSize = size;
    }

    void cleanup() {
      buffer = null;
      metaBuffer = null;
    }
  }

  private static class BufferTooSmallException extends IOException {
    private static final long serialVersionUID = 1L;
  }

  private class SpillCallback implements FutureCallback<SpillResult> {

    private final int spillNumber;

    SpillCallback(int spillNumber) {
      this.spillNumber = spillNumber;
    }

    @Override
    public void onSuccess(SpillResult result) {
      LOG.info("Spill# " + spillNumber + " complete.");
      spilledSize += result.spillSize;
      try {
        result.wrappedBuffer.reset();
        availableBuffers.add(result.wrappedBuffer);

      } catch (Throwable e) {
        LOG.fatal("Failure while attempting to reset buffer after spill", e);
        outputContext.fatalError(e, "Failure while attempting to reset buffer after spill");
      }

      spillLock.lock();
      try {
        if (pendingSpillCount.decrementAndGet() == 0) {
          spillInProgress.signal();
        }
      } finally {
        spillLock.unlock();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // spillException setup to throw an exception back to the user. Requires synchronization.
      // Consider removing it in favor of having Tez kill the task
      LOG.fatal("Failure while spilling to disk", t);
      spillException = t;
      outputContext.fatalError(t, "Failure while spilling to disk");
      spillLock.lock();
      try {
        spillInProgress.signal();
      } finally {
        spillLock.unlock();
      }
    }
  }

  private static class SpillResult {
    final long spillSize;
    final WrappedBuffer wrappedBuffer;

    SpillResult(long size, WrappedBuffer wrappedBuffer) {
      this.spillSize = size;
      this.wrappedBuffer = wrappedBuffer;
    }
  }

  private static class SpillInfo {
    final TezSpillRecord spillRecord;
    final Path outPath;

    SpillInfo(TezSpillRecord spillRecord, Path outPath) {
      this.spillRecord = spillRecord;
      this.outPath = outPath;
    }
  }

  @VisibleForTesting
  String getHost() {
    return System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
  }

  @VisibleForTesting
  int getShufflePort() throws IOException {
    ByteBuffer shuffleMetadata = outputContext
        .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    int shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);
    return shufflePort;
  }
}