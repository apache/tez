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

package org.apache.tez.runtime.library.common.shuffle;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;

import javax.annotation.Nullable;
import javax.crypto.SecretKey;

import org.apache.tez.common.Preconditions;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.utils.DATA_RANGE_IN_MB;
import org.apache.tez.util.FastNumberFormat;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DetailedPartitionStatsProto;

public class ShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleUtils.class);
  private static final long MB = 1024l * 1024l;

  static final ThreadLocal<DecimalFormat> MBPS_FORMAT =
      new ThreadLocal<DecimalFormat>() {
        @Override
        protected DecimalFormat initialValue() {
          return new DecimalFormat("0.00");
        }
      };
  static final ThreadLocal<FastNumberFormat> MBPS_FAST_FORMAT =
      new ThreadLocal<FastNumberFormat>() {
        @Override
        protected FastNumberFormat initialValue() {
          FastNumberFormat fmt = FastNumberFormat.getInstance();
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };

  public static SecretKey getJobTokenSecretFromTokenBytes(ByteBuffer meta)
      throws IOException {
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>();
    jt.readFields(in);
    SecretKey sk = JobTokenSecretManager.createSecretKey(jt.getPassword());
    return sk;
  }

  public static ByteBuffer convertJobTokenToBytes(
      Token<JobTokenIdentifier> jobToken) throws IOException {
    return TezCommonUtils.convertJobTokenToBytes(jobToken);
  }

  public static int deserializeShuffleProviderMetaData(ByteBuffer meta)
      throws IOException {
    return TezRuntimeUtils.deserializeShuffleProviderMetaData(meta);
  }

  public static void shuffleToMemory(byte[] shuffleData,
      InputStream input, int decompressedLength, int compressedLength,
      CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
      Logger LOG, InputAttemptIdentifier identifier) throws IOException {
    try {
      IFile.Reader.readToMemory(shuffleData, input, compressedLength, codec,
          ifileReadAhead, ifileReadAheadLength);
      // metrics.inputBytes(shuffleData.length);
      LOG.debug("Read {} bytes from input for {}", shuffleData.length, identifier);
    } catch (InternalError | Exception e) {
      // Close the streams
      LOG.info("Failed to read data to memory for " + identifier + ". len=" + compressedLength +
          ", decomp=" + decompressedLength + ". ExceptionMessage=" + e.getMessage());
      ioCleanup(input);
      if (e instanceof InternalError) {
        // The codec for lz0,lz4,snappy,bz2,etc. throw java.lang.InternalError
        // on decompression failures. Catching and re-throwing as IOException
        // to allow fetch failure logic to be processed.
        throw new IOException(e);
      } else if (e instanceof IOException) {
        throw e;
      } else {
        // Re-throw as an IOException
        throw new IOException(e);
      }
    }
  }
  
  public static void shuffleToDisk(OutputStream output, String hostIdentifier,
      InputStream input, long compressedLength, long decompressedLength, Logger LOG, InputAttemptIdentifier identifier,
      boolean ifileReadAhead, int ifileReadAheadLength, boolean verifyChecksum) throws IOException {
    // Copy data to local-disk
    long bytesLeft = compressedLength;
    try {
      if (verifyChecksum) {
        bytesLeft -= IFile.Reader.readToDisk(output, input, compressedLength,
            ifileReadAhead, ifileReadAheadLength);
      } else {
        final int BYTES_TO_READ = 64 * 1024;
        byte[] buf = new byte[BYTES_TO_READ];
        while (bytesLeft > 0) {
          int n = input.read(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
          if (n < 0) {
            throw new IOException("read past end of stream reading "
                + identifier);
          }
          output.write(buf, 0, n);
          bytesLeft -= n;
          // metrics.inputBytes(n);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Read " + (compressedLength - bytesLeft)
            + " bytes from input for " + identifier);
      }

      output.close();
    } catch (IOException ioe) {
      // Close the streams
      LOG.info("Failed to read data to disk for " + identifier + ". len=" + compressedLength +
          ", decomp=" + decompressedLength + ". ExceptionMessage=" + ioe.getMessage());
      ioCleanup(input, output);
      // Re-throw
      throw ioe;
    }

    // Sanity check
    if (bytesLeft != 0) {
      throw new IOException("Incomplete map output received for " +
          identifier + " from " +
          hostIdentifier + " (" + 
          bytesLeft + " bytes missing of " + 
          compressedLength + ")");
    }
  }

  public static void ioCleanup(Closeable... closeables) {
    for (Closeable c : closeables) {
      if (c == null)
        continue;
      try {
        c.close();
      } catch (IOException e) {
        LOG.debug("Exception in closing {}", c, e);
      }
    }
  }

  public static StringBuilder constructBaseURIForShuffleHandler(String host,
      int port, int partition, int partitionCount, String appId, int dagIdentifier, boolean sslShuffle) {
    final String http_protocol = (sslShuffle) ? "https://" : "http://";
    StringBuilder sb = new StringBuilder(http_protocol);
    sb.append(host);
    sb.append(":");
    sb.append(port);
    sb.append("/");
    sb.append("mapOutput?job=");
    sb.append(appId.replace("application", "job"));
    sb.append("&dag=");
    sb.append(String.valueOf(dagIdentifier));
    sb.append("&reduce=");
    sb.append(String.valueOf(partition));
    if (partitionCount > 1) {
      sb.append("-");
      sb.append(String.valueOf(partition + partitionCount - 1));
    }
    sb.append("&map=");
    return sb;
  }

  public static URL constructInputURL(String baseURI,
      Collection<InputAttemptIdentifier> inputs, boolean keepAlive) throws MalformedURLException {
    StringBuilder url = new StringBuilder(baseURI);
    boolean first = true;
    for (InputAttemptIdentifier input : inputs) {
      if (first) {
        first = false;
        url.append(input.getPathComponent());
      } else {
        url.append(",").append(input.getPathComponent());
      }
    }
    //It is possible to override keep-alive setting in cluster by adding keepAlive in url.
    //Refer MAPREDUCE-5787 to enable/disable keep-alive in the cluster.
    if (keepAlive) {
      url.append("&keepAlive=true");
    }
    return new URL(url.toString());
  }

  public static BaseHttpConnection getHttpConnection(boolean asyncHttp, URL url,
      HttpConnectionParams params, String logIdentifier, JobTokenSecretManager jobTokenSecretManager)
      throws IOException {
    return TezRuntimeUtils.getHttpConnection(asyncHttp, url, params, logIdentifier, jobTokenSecretManager);
  }

  public static String stringify(DataMovementEventPayloadProto dmProto) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    if (dmProto.hasEmptyPartitions()) {
      sb.append("hasEmptyPartitions: ").append(dmProto.hasEmptyPartitions()).append(", ");
    }
    sb.append("host: " + dmProto.getHost()).append(", ");
    sb.append("port: " + dmProto.getPort()).append(", ");
    sb.append("pathComponent: " + dmProto.getPathComponent()).append(", ");
    sb.append("runDuration: " + dmProto.getRunDuration()).append(", ");
    sb.append("hasDataInEvent: " + dmProto.hasData());
    sb.append("]");
    return sb.toString();
  }

  /**
   * Generate DataMovementEvent
   *
   * @param sendEmptyPartitionDetails
   * @param numPhysicalOutputs
   * @param spillRecord
   * @param context
   * @param spillId
   * @param finalMergeEnabled
   * @param isLastEvent
   * @param pathComponent
   * @param auxiliaryService
   * @param deflater
   * @return ByteBuffer
   * @throws IOException
   */
  static ByteBuffer generateDMEPayload(boolean sendEmptyPartitionDetails,
      int numPhysicalOutputs, TezSpillRecord spillRecord, OutputContext context,
      int spillId, boolean finalMergeEnabled, boolean isLastEvent, String pathComponent, String auxiliaryService, Deflater deflater)
      throws IOException {
    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();

    boolean outputGenerated = true;
    if (sendEmptyPartitionDetails) {
      BitSet emptyPartitionDetails = new BitSet();
      for(int i=0;i<spillRecord.size();i++) {
        TezIndexRecord indexRecord = spillRecord.getIndex(i);
        if (!indexRecord.hasData()) {
          emptyPartitionDetails.set(i);
        }
      }
      int emptyPartitions = emptyPartitionDetails.cardinality();
      outputGenerated = (spillRecord.size() != emptyPartitions);
      if (emptyPartitions > 0) {
        ByteString emptyPartitionsBytesString =
            TezCommonUtils.compressByteArrayToByteString(
                TezUtilsInternal.toByteArray(emptyPartitionDetails), deflater);
        payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
        LOG.info("EmptyPartition bitsetSize=" + emptyPartitionDetails.cardinality() + ", numOutputs="
            + numPhysicalOutputs + ", emptyPartitions=" + emptyPartitions
            + ", compressedSize=" + emptyPartitionsBytesString.size());
      }
    }

    if (!sendEmptyPartitionDetails || outputGenerated) {
      String host = context.getExecutionContext().getHostName();
      ByteBuffer shuffleMetadata = context
          .getServiceProviderMetaData(auxiliaryService);
      int shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);
      payloadBuilder.setHost(host);
      payloadBuilder.setPort(shufflePort);
      //Path component is always 0 indexed
      payloadBuilder.setPathComponent(pathComponent);
    }

    if (!finalMergeEnabled) {
      payloadBuilder.setSpillId(spillId);
      payloadBuilder.setLastEvent(isLastEvent);
    }

    payloadBuilder.setRunDuration(0); //TODO: who is dependent on this?
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();
    ByteBuffer payload = payloadProto.toByteString().asReadOnlyByteBuffer();
    return payload;
  }

  /**
   * Generate events for outputs which have not been started.
   * @param eventList
   * @param numPhysicalOutputs
   * @param context
   * @param generateVmEvent whether to generate a vm event or not
   * @param isCompositeEvent whether to generate a CompositeDataMovementEvent or a DataMovementEvent
   * @param deflater
   * @throws IOException
   */
  public static void generateEventsForNonStartedOutput(List<Event> eventList,
                                                       int numPhysicalOutputs,
                                                       OutputContext context,
                                                       boolean generateVmEvent,
                                                       boolean isCompositeEvent, Deflater deflater) throws
      IOException {
    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();


    // Construct the VertexManager event if required.
    if (generateVmEvent) {
      ShuffleUserPayloads.VertexManagerEventPayloadProto.Builder vmBuilder =
          ShuffleUserPayloads.VertexManagerEventPayloadProto.newBuilder();
      vmBuilder.setOutputSize(0);
      VertexManagerEvent vmEvent = VertexManagerEvent.create(
          context.getDestinationVertexName(),
          vmBuilder.build().toByteString().asReadOnlyByteBuffer());
      eventList.add(vmEvent);
    }

    // Construct the DataMovementEvent
    // Always set empty partition information since no files were generated.
    LOG.info("Setting all {} partitions as empty for non-started output", numPhysicalOutputs);
    BitSet emptyPartitionDetails = new BitSet(numPhysicalOutputs);
    emptyPartitionDetails.set(0, numPhysicalOutputs, true);
    ByteString emptyPartitionsBytesString =
        TezCommonUtils.compressByteArrayToByteString(
            TezUtilsInternal.toByteArray(emptyPartitionDetails), deflater);
    payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
    payloadBuilder.setRunDuration(0);
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();
    ByteBuffer dmePayload = payloadProto.toByteString().asReadOnlyByteBuffer();


    if (isCompositeEvent) {
      CompositeDataMovementEvent cdme =
          CompositeDataMovementEvent.create(0, numPhysicalOutputs, dmePayload);
      eventList.add(cdme);
    } else {
      DataMovementEvent dme = DataMovementEvent.create(0, dmePayload);
      eventList.add(dme);
    }
  }

  /**
   * Generate events when spill happens
   *
   * @param eventList events would be added to this list
   * @param finalMergeEnabled
   * @param isLastEvent
   * @param context
   * @param spillId
   * @param spillRecord
   * @param numPhysicalOutputs
   * @param pathComponent
   * @param partitionStats
   * @param auxiliaryService
   * @throws IOException
   */
  public static void generateEventOnSpill(List<Event> eventList, boolean finalMergeEnabled,
      boolean isLastEvent, OutputContext context, int spillId, TezSpillRecord spillRecord,
      int numPhysicalOutputs, boolean sendEmptyPartitionDetails, String pathComponent,
      @Nullable long[] partitionStats, boolean reportDetailedPartitionStats, String auxiliaryService, Deflater deflater)
      throws IOException {
    Objects.requireNonNull(eventList, "EventList can't be null");

    context.notifyProgress();
    if (finalMergeEnabled) {
      Preconditions.checkArgument(isLastEvent, "Can not send multiple events when final merge is "
          + "enabled");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("pathComponent=" + pathComponent + ", isLastEvent="
          + isLastEvent + ", spillId=" + spillId + ", finalMergeDisabled=" + finalMergeEnabled +
          ", numPhysicalOutputs=" + numPhysicalOutputs);
    }

    ByteBuffer payload = generateDMEPayload(sendEmptyPartitionDetails, numPhysicalOutputs,
        spillRecord, context, spillId,
        finalMergeEnabled, isLastEvent, pathComponent, auxiliaryService, deflater);

    if (finalMergeEnabled || isLastEvent) {
      VertexManagerEvent vmEvent = generateVMEvent(context, partitionStats,
          reportDetailedPartitionStats, deflater);
      eventList.add(vmEvent);
    }

    CompositeDataMovementEvent csdme =
        CompositeDataMovementEvent.create(0, numPhysicalOutputs, payload);
    eventList.add(csdme);
  }

  public static VertexManagerEvent generateVMEvent(OutputContext context,
      long[] sizePerPartition, boolean reportDetailedPartitionStats, Deflater deflater)
          throws IOException {
    ShuffleUserPayloads.VertexManagerEventPayloadProto.Builder vmBuilder =
        ShuffleUserPayloads.VertexManagerEventPayloadProto.newBuilder();

    long outputSize = context.getCounters().
        findCounter(TaskCounter.OUTPUT_BYTES).getValue();

    // Set this information only when required.  In pipelined shuffle,
    // multiple events would end up adding up to final output size.
    // This is needed for auto-reduce parallelism to work properly.
    vmBuilder.setOutputSize(outputSize);
    vmBuilder.setNumRecord(context.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS).getValue()
     + context.getCounters().findCounter(TaskCounter.OUTPUT_LARGE_RECORDS).getValue());

    //set partition stats
    if (sizePerPartition != null && sizePerPartition.length > 0) {
      if (reportDetailedPartitionStats) {
        vmBuilder.setDetailedPartitionStats(
            getDetailedPartitionStatsForPhysicalOutput(sizePerPartition));
      } else {
        RoaringBitmap stats = getPartitionStatsForPhysicalOutput(
            sizePerPartition);
        DataOutputBuffer dout = new DataOutputBuffer();
        stats.serialize(dout);
        ByteString partitionStatsBytes =
            TezCommonUtils.compressByteArrayToByteString(dout.getData(), deflater);
        vmBuilder.setPartitionStats(partitionStatsBytes);
      }
    }

    VertexManagerEvent vmEvent = VertexManagerEvent.create(
        context.getDestinationVertexName(),
        vmBuilder.build().toByteString().asReadOnlyByteBuffer());
    return vmEvent;
  }

  /**
   * Data size for the destinations
   *
   * @param sizes for physical outputs
   */
  public static RoaringBitmap getPartitionStatsForPhysicalOutput(long[] sizes) {
    RoaringBitmap partitionStats = new RoaringBitmap();
    if (sizes == null || sizes.length == 0) {
      return partitionStats;
    }
    final int RANGE_LEN = DATA_RANGE_IN_MB.values().length;
    for (int i = 0; i < sizes.length; i++) {
      int bucket = DATA_RANGE_IN_MB.getRange(sizes[i]).ordinal();
      int index = i * (RANGE_LEN);
      partitionStats.add(index + bucket);
    }
    return partitionStats;
  }

  static long ceil(long a, long b) {
    return (a + (b - 1)) / b;
  }

  /**
   * Detailed partition stats
   *
   * @param sizes actual partition sizes
   */
  public static DetailedPartitionStatsProto
  getDetailedPartitionStatsForPhysicalOutput(long[] sizes) {
    DetailedPartitionStatsProto.Builder builder =
        DetailedPartitionStatsProto.newBuilder();
    for (int i=0; i<sizes.length; i++) {
      // Round the size up. So 1 byte -> the value of sizeInMB == 1
      // Throws IllegalArgumentException if value is greater than
      // Integer.MAX_VALUE. That should be ok given Integer.MAX_VALUE * MB
      // means PB.
      int sizeInMb = Ints.checkedCast(ceil(sizes[i], MB));
      builder.addSizeInMb(sizeInMb);
    }
    return builder.build();
  }

  public static class FetchStatsLogger {
    private final Logger activeLogger;
    private final Logger aggregateLogger;
    private final AtomicLong logCount = new AtomicLong();
    private final AtomicLong compressedSize = new AtomicLong();
    private final AtomicLong decompressedSize = new AtomicLong();
    private final AtomicLong totalTime = new AtomicLong();

    public FetchStatsLogger(Logger activeLogger, Logger aggregateLogger) {
      this.activeLogger = activeLogger;
      this.aggregateLogger = aggregateLogger;
    }


    private static StringBuilder toShortString(InputAttemptIdentifier inputAttemptIdentifier, StringBuilder sb) {
      sb.append("{");
      sb.append(inputAttemptIdentifier.getInputIdentifier());
      sb.append(", ").append(inputAttemptIdentifier.getAttemptNumber());
      sb.append(", ").append(inputAttemptIdentifier.getPathComponent());
      if (inputAttemptIdentifier.getFetchTypeInfo()
          != InputAttemptIdentifier.SPILL_INFO.FINAL_MERGE_ENABLED) {
        sb.append(", ").append(inputAttemptIdentifier.getFetchTypeInfo().ordinal());
        sb.append(", ").append(inputAttemptIdentifier.getSpillEventId());
      }
      sb.append("}");
      return sb;
    }
    /**
     * Log individual fetch complete event.
     * This log information would be used by tez-tool/perf-analzyer/shuffle tools for mining
     * - amount of data transferred between source to destination machine
     * - time taken to transfer data between source to destination machine
     * - details on DISK/DISK_DIRECT/MEMORY based shuffles
     *
     * @param millis
     * @param bytesCompressed
     * @param bytesDecompressed
     * @param outputType
     * @param srcAttemptIdentifier
     */
    public void logIndividualFetchComplete(long millis, long bytesCompressed,
        long bytesDecompressed, String outputType, InputAttemptIdentifier srcAttemptIdentifier) {

      if (activeLogger.isInfoEnabled()) {
        long wholeMBs = 0;
        long partialMBs = 0;
        millis = Math.max(1L, millis);
        // fast math is done using integer math to avoid double to string conversion
        // calculate B/s * 100 to preserve MBs precision to two decimal places
        // multiply numerator by 100000 (2^5 * 5^5) and divide denominator by MB (2^20)
        // simply fraction to protect ourselves from overflow by factoring out 2^5
        wholeMBs = (bytesCompressed * 3125) / (millis * 32768);
        partialMBs = wholeMBs % 100;
        wholeMBs /= 100;
        StringBuilder sb = new StringBuilder("Completed fetch for attempt: ");
        toShortString(srcAttemptIdentifier, sb);
        sb.append(" to ");
        sb.append(outputType);
        sb.append(", csize=");
        sb.append(bytesCompressed);
        sb.append(", dsize=");
        sb.append(bytesDecompressed);
        sb.append(", EndTime=");
        sb.append(System.currentTimeMillis());
        sb.append(", TimeTaken=");
        sb.append(millis);
        sb.append(", Rate=");
        sb.append(wholeMBs);
        sb.append(".");
        MBPS_FAST_FORMAT.get().format(partialMBs, sb);
        sb.append(" MB/s");
        activeLogger.info(sb.toString());
      } else {
        long currentCount, currentCompressedSize, currentDecompressedSize, currentTotalTime;
        synchronized (this) {
          currentCount = logCount.incrementAndGet();
          currentCompressedSize = compressedSize.addAndGet(bytesCompressed);
          currentDecompressedSize = decompressedSize.addAndGet(bytesDecompressed);
          currentTotalTime = totalTime.addAndGet(millis);
          if (currentCount % 1000 == 0) {
            compressedSize.set(0);
            decompressedSize.set(0);
            totalTime.set(0);
          }
        }
        if (currentCount % 1000 == 0) {
          double avgRate = currentTotalTime == 0 ? 0
              : currentCompressedSize / (double)currentTotalTime / 1000 / 1024 / 1024;
          aggregateLogger.info("Completed {} fetches, stats for last 1000 fetches: "
              + "avg csize: {}, avg dsize: {}, avgTime: {}, avgRate: {}", currentCount,
              currentCompressedSize / 1000, currentDecompressedSize / 1000, currentTotalTime / 1000,
              MBPS_FORMAT.get().format(avgRate));
        }
      }
    }
  }

  /**
   * Build {@link org.apache.tez.http.HttpConnectionParams} from configuration
   *
   * @param conf
   * @return HttpConnectionParams
   */
  public static HttpConnectionParams getHttpConnectionParams(Configuration conf) {
    return TezRuntimeUtils.getHttpConnectionParams(conf);
  }

  public static boolean isTezShuffleHandler(Configuration config) {
    return config.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT).
        contains("tez");
  }
}

