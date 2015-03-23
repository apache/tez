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
import java.util.List;

import javax.crypto.SecretKey;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.IOUtils;
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
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.shuffle.HttpConnection.HttpConnectionParams;
import org.apache.tez.runtime.library.common.shuffle.HttpConnection.HttpConnectionParamsBuilder;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

public class ShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleUtils.class);
  public static final String SHUFFLE_HANDLER_SERVICE_ID = "mapreduce_shuffle";

  static final ThreadLocal<DecimalFormat> MBPS_FORMAT =
      new ThreadLocal<DecimalFormat>() {
        @Override
        protected DecimalFormat initialValue() {
          return new DecimalFormat("0.00");
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
    DataInputByteBuffer in = new DataInputByteBuffer();
    try {
      in.reset(meta);
      int port = in.readInt();
      return port;
    } finally {
      in.close();
    }
  }

  public static void shuffleToMemory(byte[] shuffleData,
      InputStream input, int decompressedLength, int compressedLength,
      CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
      Logger LOG, String identifier) throws IOException {
    try {
      IFile.Reader.readToMemory(shuffleData, input, compressedLength, codec,
        ifileReadAhead, ifileReadAheadLength);
      // metrics.inputBytes(shuffleData.length);
      LOG.info("Read " + shuffleData.length + " bytes from input for "
          + identifier);
    } catch (IOException ioe) {
      // Close the streams
      ioCleanup(input);
      // Re-throw
      throw ioe;
    }
  }
  
  public static void shuffleToDisk(OutputStream output, String hostIdentifier,
      InputStream input, long compressedLength, Logger LOG, String identifier)
      throws IOException {
    // Copy data to local-disk
    long bytesLeft = compressedLength;
    try {
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

      LOG.info("Read " + (compressedLength - bytesLeft)
          + " bytes from input for " + identifier);

      output.close();
    } catch (IOException ioe) {
      // Close the streams
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
        if (LOG.isDebugEnabled())
          LOG.debug("Exception in closing " + c, e);
      }
    }
  }

  // TODO NEWTEZ handle ssl shuffle
  public static StringBuilder constructBaseURIForShuffleHandler(String host,
      int port, int partition, String appId, boolean sslShuffle) {
    return constructBaseURIForShuffleHandler(host + ":" + String.valueOf(port),
      partition, appId, sslShuffle);
  }
  
  public static StringBuilder constructBaseURIForShuffleHandler(String hostIdentifier,
      int partition, String appId, boolean sslShuffle) {
    final String http_protocol = (sslShuffle) ? "https://" : "http://";
    StringBuilder sb = new StringBuilder(http_protocol);
    sb.append(hostIdentifier);
    sb.append("/");
    sb.append("mapOutput?job=");
    sb.append(appId.replace("application", "job"));
    sb.append("&reduce=");
    sb.append(String.valueOf(partition));
    sb.append("&map=");
    return sb;
  }

  public static URL constructInputURL(String baseURI, 
      List<InputAttemptIdentifier> inputs, boolean keepAlive) throws MalformedURLException {
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

  public static HttpConnectionParams constructHttpShuffleConnectionParams(
      Configuration conf) {
    HttpConnectionParamsBuilder builder = new HttpConnectionParamsBuilder();

    int connectionTimeout =
        conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT,
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT_DEFAULT);

    int readTimeout =
        conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT,
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT_DEFAULT);

    int bufferSize =
        conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE,
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE_DEFAULT);

    boolean keepAlive =
        conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED,
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED_DEFAULT);
    int keepAliveMaxConnections =
        conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS,
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS_DEFAULT);
    if (keepAlive) {
      System.setProperty("sun.net.http.errorstream.enableBuffering", "true");
      System.setProperty("http.maxConnections",
        String.valueOf(keepAliveMaxConnections));
      LOG.info("Set keepAlive max connections: " + keepAliveMaxConnections);
    }

    builder.setTimeout(connectionTimeout, readTimeout)
        .setBufferSize(bufferSize)
        .setKeepAlive(keepAlive, keepAliveMaxConnections);

    boolean sslShuffle = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
      TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT);
    builder.setSSL(sslShuffle, conf);

    return builder.build();
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
    if (dmProto.hasData()) {
      sb.append(", ").append("hasDataInEvent: " + dmProto.hasData());
    }
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
   * @return ByteBuffer
   * @throws IOException
   */
  static ByteBuffer generateDMEPayload(boolean sendEmptyPartitionDetails,
      int numPhysicalOutputs, TezSpillRecord spillRecord, OutputContext context,
      int spillId, boolean finalMergeEnabled, boolean isLastEvent, String pathComponent)
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
                TezUtilsInternal.toByteArray(emptyPartitionDetails));
        payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
        LOG.info("EmptyPartition bitsetSize=" + emptyPartitionDetails.cardinality() + ", numOutputs="
            + numPhysicalOutputs + ", emptyPartitions=" + emptyPartitions
            + ", compressedSize=" + emptyPartitionsBytesString.size());
      }
    }

    if (!sendEmptyPartitionDetails || outputGenerated) {
      String host = context.getExecutionContext().getHostName();
      ByteBuffer shuffleMetadata = context
          .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
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
   * @throws IOException
   */
  public static void generateEventOnSpill(List<Event> eventList, boolean finalMergeEnabled,
      boolean isLastEvent, OutputContext context, int spillId, TezSpillRecord spillRecord,
      int numPhysicalOutputs, boolean sendEmptyPartitionDetails, String pathComponent)
      throws IOException {
    Preconditions.checkArgument(eventList != null, "EventList can't be null");

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
        finalMergeEnabled, isLastEvent, pathComponent);

    if (finalMergeEnabled || isLastEvent) {
      ShuffleUserPayloads.VertexManagerEventPayloadProto.Builder vmBuilder =
          ShuffleUserPayloads.VertexManagerEventPayloadProto.newBuilder();

      long outputSize = context.getCounters().findCounter(TaskCounter.OUTPUT_BYTES).getValue();

      //Set this information only when required.  In pipelined shuffle, multiple events would end
      // up adding up to final outputsize.  This is needed for auto-reduce parallelism to work
      // properly.
      vmBuilder.setOutputSize(outputSize);
      VertexManagerEvent vmEvent = VertexManagerEvent.create(
          context.getDestinationVertexName(), vmBuilder.build().toByteString().asReadOnlyByteBuffer());
      eventList.add(vmEvent);
    }


    CompositeDataMovementEvent csdme =
        CompositeDataMovementEvent.create(0, numPhysicalOutputs, payload);
    eventList.add(csdme);
  }



  /**
   * Log individual fetch complete event.
   * This log information would be used by tez-tool/perf-analzyer/shuffle tools for mining
   * - amount of data transferred between source to destination machine
   * - time taken to transfer data between source to destination machine
   * - details on DISK/DISK_DIRECT/MEMORY based shuffles
   *
   * @param log
   * @param millis
   * @param bytesCompressed
   * @param bytesDecompressed
   * @param outputType
   * @param srcAttemptIdentifier
   */
  public static void logIndividualFetchComplete(Logger log, long millis, long
      bytesCompressed,
      long bytesDecompressed, String outputType, InputAttemptIdentifier srcAttemptIdentifier) {
    double rate = 0;
    if (millis != 0) {
      rate = bytesCompressed / ((double) millis / 1000);
      rate = rate / (1024 * 1024);
    }
    log.info(
        "Completed fetch for attempt: "
            + srcAttemptIdentifier + " to " + outputType +
            ", CompressedSize=" + bytesCompressed + ", DecompressedSize=" + bytesDecompressed +
            ", EndTime=" + System.currentTimeMillis() + ", TimeTaken=" + millis + ", Rate=" +
            MBPS_FORMAT.get().format(rate) + " MB/s");
  }
}

