/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;

public class TezUtils {
  
  private static final Log LOG = LogFactory.getLog(TezUtils.class);
  
  public static void addUserSpecifiedTezConfiguration(Configuration conf) 
      throws IOException {
    FileInputStream confPBBinaryStream = null;
    ConfigurationProto.Builder confProtoBuilder = ConfigurationProto.newBuilder();
    try {
      confPBBinaryStream = new FileInputStream(
          TezConfiguration.TEZ_PB_BINARY_CONF_NAME);
      confProtoBuilder.mergeFrom(confPBBinaryStream);
    } finally {
      if (confPBBinaryStream != null) {
        confPBBinaryStream.close();
      }
    }

    ConfigurationProto confProto = confProtoBuilder.build();

    List<PlanKeyValuePair> kvPairList = confProto.getConfKeyValuesList();
    if(kvPairList != null && !kvPairList.isEmpty()) {
      for(PlanKeyValuePair kvPair : kvPairList) {
        conf.set(kvPair.getKey(), kvPair.getValue());
      }
    }
  }
  
  public static ByteString createByteStringFromConf(Configuration conf)
      throws IOException {
    Preconditions.checkNotNull(conf, "Configuration must be specified");
    ByteString.Output os = ByteString.newOutput();
    //SnappyOutputStream compressOs = new SnappyOutputStream(os);
    DeflaterOutputStream compressOs = new DeflaterOutputStream(os, new Deflater(Deflater.BEST_SPEED));
    DataOutputStream dos = new DataOutputStream(compressOs);
    conf.write(dos);
    dos.close();
    return os.toByteString();
  }

  public static byte[] createUserPayloadFromConf(Configuration conf)
      throws IOException {
    Preconditions.checkNotNull(conf, "Configuration must be specified");
    DataOutputBuffer dob = new DataOutputBuffer();
    conf.write(dob);
    return compressBytes(dob.getData());
  }

  public static Configuration createConfFromByteString(ByteString byteString)
      throws IOException {
    Preconditions.checkNotNull(byteString, "ByteString must be specified");
//    SnappyInputStream uncompressIs = new SnappyInputStream(byteString.newInput());
    InflaterInputStream uncompressIs = new InflaterInputStream(byteString.newInput());
    DataInputStream dataInputStream = new DataInputStream(uncompressIs);
    Configuration conf = new Configuration(false);
    conf.readFields(dataInputStream);
    return conf;
  }
  
  public static Configuration createConfFromUserPayload(byte[] bb)
      throws IOException {
    // TODO Avoid copy ?
    Preconditions.checkNotNull(bb, "Bytes must be specified");
    byte[] uncompressed = uncompressBytes(bb);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(uncompressed, 0, uncompressed.length);
    Configuration conf = new Configuration(false);
    conf.readFields(dib);
    return conf;
  }

  public static byte[] compressBytes(byte[] inBytes) throws IOException {
    Stopwatch sw = null;
    if (LOG.isDebugEnabled()) {
      sw = new Stopwatch().start();
    }
    byte[] compressed = compressBytesInflateDeflate(inBytes);
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("UncompressedSize: " + inBytes.length + ", CompressedSize: "
          + compressed.length + ", CompressTime: " + sw.elapsedMillis());
    }
    return compressed;
  }

  public static byte[] uncompressBytes(byte[] inBytes) throws IOException {
    Stopwatch sw = null;
    if (LOG.isDebugEnabled()) {
      sw = new Stopwatch().start();
    }
    byte[] uncompressed = uncompressBytesInflateDeflate(inBytes);
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("CompressedSize: " + inBytes.length + ", UncompressedSize: "
          + uncompressed.length + ", UncompressTimeTaken: "
          + sw.elapsedMillis());
    }
    return uncompressed;
  }
  
//  private static byte[] compressBytesSnappy(byte[] inBytes) throws IOException {
//    return Snappy.compress(inBytes);
//  }
//
//  private static byte[] uncompressBytesSnappy(byte[] inBytes) throws IOException {
//    return Snappy.uncompress(inBytes);
//  }  
  
  private static byte[] compressBytesInflateDeflate(byte[] inBytes) {
    Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    deflater.setInput(inBytes);
    ByteArrayOutputStream bos = new ByteArrayOutputStream(inBytes.length);
    deflater.finish();
    byte[] buffer = new byte[1024 * 8];
    while (!deflater.finished()) {
      int count = deflater.deflate(buffer);
      bos.write(buffer, 0, count);
    }
    byte[] output = bos.toByteArray();
    return output;
  }

  private static byte[] uncompressBytesInflateDeflate(byte[] inBytes) throws IOException {
    Inflater inflater = new Inflater();
    inflater.setInput(inBytes);
    ByteArrayOutputStream bos = new ByteArrayOutputStream(inBytes.length);
    byte[] buffer = new byte[1024 * 8];
    while (!inflater.finished()) {
      int count;
      try {
        count = inflater.inflate(buffer);
      } catch (DataFormatException e) {
        throw new IOException(e);
      }
      bos.write(buffer, 0, count);
    }
    byte[] output = bos.toByteArray();
    return output;
  }
}
