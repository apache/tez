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

package org.apache.tez.runtime.library.shuffle.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection.HttpConnectionParams;
import org.apache.tez.runtime.library.shuffle.common.HttpConnection.HttpConnectionParamsBuilder;

public class ShuffleUtils {

  private static final Log LOG = LogFactory.getLog(ShuffleUtils.class);
  public static String SHUFFLE_HANDLER_SERVICE_ID = "mapreduce_shuffle";

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
    DataOutputBuffer dob = new DataOutputBuffer();
    jobToken.write(dob);
    ByteBuffer bb = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    return bb;
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

  @SuppressWarnings("resource")
  public static void shuffleToMemory(byte[] shuffleData,
      InputStream input, int decompressedLength, int compressedLength,
      CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
      Log LOG, String identifier) throws IOException {
    try {
      IFile.Reader.readToMemory(shuffleData, input, compressedLength, codec,
        ifileReadAhead, ifileReadAheadLength);
      // metrics.inputBytes(shuffleData.length);
      LOG.info("Read " + shuffleData.length + " bytes from input for "
          + identifier);
    } catch (IOException ioe) {
      // Close the streams
      IOUtils.cleanup(LOG, input);
      // Re-throw
      throw ioe;
    }
  }
  
  public static void shuffleToDisk(OutputStream output, String hostIdentifier,
      InputStream input, long compressedLength, Log LOG, String identifier)
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
      IOUtils.cleanup(LOG, input, output);
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
        conf.getInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT,
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT_DEFAULT);

    int readTimeout =
        conf.getInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT,
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT_DEFAULT);

    int bufferSize =
        conf.getInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE,
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE_DEFAULT);

    boolean keepAlive =
        conf.getBoolean(TezJobConfig.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED,
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED_DEFAULT);
    int keepAliveMaxConnections =
        conf.getInt(
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS,
          TezJobConfig.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS_DEFAULT);
    if (keepAlive) {
      System.setProperty("sun.net.http.errorstream.enableBuffering", "true");
      System.setProperty("http.maxConnections",
        String.valueOf(keepAliveMaxConnections));
      LOG.info("Set keepAlive max connections: " + keepAliveMaxConnections);
    }

    builder.setTimeout(connectionTimeout, readTimeout)
        .setBufferSize(bufferSize)
        .setKeepAlive(keepAlive, keepAliveMaxConnections);

    boolean sslShuffle = conf.getBoolean(TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
      TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT);
    builder.setSSL(sslShuffle, conf);

    return builder.build();
  }
}

