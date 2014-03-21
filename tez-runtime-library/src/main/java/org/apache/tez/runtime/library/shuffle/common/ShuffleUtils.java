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
import java.nio.ByteBuffer;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;

public class ShuffleUtils {

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
  public static void shuffleToMemory(MemoryFetchedInput fetchedInput,
      InputStream input, int decompressedLength, int compressedLength,
      CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
      Log LOG) throws IOException {
    IFileInputStream checksumIn = new IFileInputStream(input, compressedLength,
        ifileReadAhead, ifileReadAheadLength);

    input = checksumIn;

    // Are map-outputs compressed?
    if (codec != null) {
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      decompressor.reset();
      input = codec.createInputStream(input, decompressor);
    }
    // Copy map-output into an in-memory buffer
    byte[] shuffleData = fetchedInput.getBytes();

    try {
      IOUtils.readFully(input, shuffleData, 0, shuffleData.length);
      // metrics.inputBytes(shuffleData.length);
      LOG.info("Read " + shuffleData.length + " bytes from input for "
          + fetchedInput.getInputAttemptIdentifier());
    } catch (IOException ioe) {
      // Close the streams
      IOUtils.cleanup(LOG, input);
      // Re-throw
      throw ioe;
    }
  }
  
  public static void shuffleToDisk(DiskFetchedInput fetchedInput,
      InputStream input, long compressedLength, Log LOG)
      throws IOException {
    // Copy data to local-disk
    OutputStream output = fetchedInput.getOutputStream();
    long bytesLeft = compressedLength;
    try {
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];
      while (bytesLeft > 0) {
        int n = input.read(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
        if (n < 0) {
          throw new IOException("read past end of stream reading "
              + fetchedInput.getInputAttemptIdentifier());
        }
        output.write(buf, 0, n);
        bytesLeft -= n;
        // metrics.inputBytes(n);
      }

      LOG.info("Read " + (compressedLength - bytesLeft)
          + " bytes from input for " + fetchedInput.getInputAttemptIdentifier());

      output.close();
    } catch (IOException ioe) {
      // Close the streams
      IOUtils.cleanup(LOG, input, output);

      // Re-throw
      throw ioe;
    }

    // Sanity check
    if (bytesLeft != 0) {
      throw new IOException("Incomplete input received for "
          + fetchedInput.getInputAttemptIdentifier() + " ("
          + bytesLeft + " bytes missing of " + compressedLength + ")");
    }
  }
  
  // TODO NEWTEZ handle ssl shuffle
  public static StringBuilder constructBaseURIForShuffleHandler(String host, int port, int partition, ApplicationId appId) {
    StringBuilder sb = new StringBuilder("http://");
    sb.append(host);
    sb.append(":");
    sb.append(String.valueOf(port));
    sb.append("/");
    sb.append("mapOutput?job=");
    sb.append(appId.toString().replace("application", "job"));
    sb.append("&reduce=");
    sb.append(String.valueOf(partition));
    sb.append("&map=");
    return sb;
  }
}
