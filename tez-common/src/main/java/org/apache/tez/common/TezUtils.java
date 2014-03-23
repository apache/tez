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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;

public class TezUtils {

  private static final Log LOG = LogFactory.getLog(TezUtils.class);

  public static void addUserSpecifiedTezConfiguration(Configuration conf) throws IOException {
    FileInputStream confPBBinaryStream = null;
    ConfigurationProto.Builder confProtoBuilder = ConfigurationProto.newBuilder();
    try {
      confPBBinaryStream = new FileInputStream(TezConfiguration.TEZ_PB_BINARY_CONF_NAME);
      confProtoBuilder.mergeFrom(confPBBinaryStream);
    } finally {
      if (confPBBinaryStream != null) {
        confPBBinaryStream.close();
      }
    }

    ConfigurationProto confProto = confProtoBuilder.build();

    List<PlanKeyValuePair> kvPairList = confProto.getConfKeyValuesList();
    if (kvPairList != null && !kvPairList.isEmpty()) {
      for (PlanKeyValuePair kvPair : kvPairList) {
        conf.set(kvPair.getKey(), kvPair.getValue());
      }
    }
  }

  /**
   * Convert a Configuration to compressed ByteString using Protocol buffer
   * 
   * @param conf
   *          : Configuration to be converted
   * @return PB ByteString (compressed)
   * @throws IOException
   */
  public static ByteString createByteStringFromConf(Configuration conf) throws IOException {
    Preconditions.checkNotNull(conf, "Configuration must be specified");
    ByteString.Output os = ByteString.newOutput();
    DeflaterOutputStream compressOs = new DeflaterOutputStream(os,
        new Deflater(Deflater.BEST_SPEED));
    try {
      writeConfInPB(compressOs, conf);
    } finally {
      if (compressOs != null) {
        compressOs.close();
      }
    }
    return os.toByteString();
  }

  /**
   * Convert a Configuration to compressed user pay load (i.e. byte[]) using
   * Protocol buffer
   * 
   * @param conf
   *          : Configuration to be converted
   * @return compressed pay load
   * @throws IOException
   */
  public static byte[] createUserPayloadFromConf(Configuration conf) throws IOException {
    return createByteStringFromConf(conf).toByteArray();
  }

  /**
   * Convert compressed byte string to a Configuration object using protocol
   * buffer
   * 
   * @param byteString
   *          :compressed conf in Protocol buffer
   * @return Configuration
   * @throws IOException
   */
  public static Configuration createConfFromByteString(ByteString byteString) throws IOException {
    Preconditions.checkNotNull(byteString, "ByteString must be specified");
    // SnappyInputStream uncompressIs = new
    // SnappyInputStream(byteString.newInput());
    InflaterInputStream uncompressIs = new InflaterInputStream(byteString.newInput());
    ConfigurationProto confProto = ConfigurationProto.parseFrom(uncompressIs);
    Configuration conf = new Configuration(false);
    readConfFromPB(confProto, conf);
    return conf;
  }

  /**
   * Convert compressed pay load in byte[] to a Configuration object using
   * protocol buffer
   * 
   * @param bb
   *          : compressed pay load
   * @return Configuration
   * @throws IOException
   */
  public static Configuration createConfFromUserPayload(byte[] bb) throws IOException {
    return createConfFromByteString(ByteString.copyFrom(bb));
  }

  private static void writeConfInPB(OutputStream dos, Configuration conf) throws IOException {
    ConfigurationProto.Builder confProtoBuilder = ConfigurationProto.newBuilder();
    Iterator<Entry<String, String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Entry<String, String> entry = iter.next();
      PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
      kvp.setKey(entry.getKey());
      kvp.setValue(entry.getValue());
      confProtoBuilder.addConfKeyValues(kvp);
    }
    ConfigurationProto confProto = confProtoBuilder.build();
    confProto.writeTo(dos);
  }

  private static void readConfFromPB(ConfigurationProto confProto, Configuration conf) {
    List<PlanKeyValuePair> settingList = confProto.getConfKeyValuesList();
    for (PlanKeyValuePair setting : settingList) {
      conf.set(setting.getKey(), setting.getValue());
    }
  }

  public static byte[] compressBytes(byte[] inBytes) throws IOException {
    Stopwatch sw = null;
    if (LOG.isDebugEnabled()) {
      sw = new Stopwatch().start();
    }
    byte[] compressed = compressBytesInflateDeflate(inBytes);
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("UncompressedSize: " + inBytes.length + ", CompressedSize: " + compressed.length
          + ", CompressTime: " + sw.elapsedMillis());
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
      LOG.debug("CompressedSize: " + inBytes.length + ", UncompressedSize: " + uncompressed.length
          + ", UncompressTimeTaken: " + sw.elapsedMillis());
    }
    return uncompressed;
  }

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

  public static ByteString compressByteArrayToByteString(byte[] inBytes) throws IOException {
    ByteString.Output os = ByteString.newOutput();
    DeflaterOutputStream compressOs = new DeflaterOutputStream(os, new Deflater(
        Deflater.BEST_COMPRESSION));
    compressOs.write(inBytes);
    compressOs.finish();
    ByteString byteString = os.toByteString();
    return byteString;
  }
  
  public static byte[] decompressByteStringToByteArray(ByteString byteString) throws IOException {
    InflaterInputStream in = new InflaterInputStream(byteString.newInput());
    byte[] bytes = IOUtils.toByteArray(in);
    return bytes;
  }

  public static void updateLoggers(String addend) throws FileNotFoundException {
    String containerLogDir = null;

    LOG.info("Redirecting log files based on addend: " + addend);

    Appender appender = Logger.getRootLogger().getAppender(
        TezConfiguration.TEZ_CONTAINER_LOGGER_NAME);
    if (appender != null) {
      if (appender instanceof TezContainerLogAppender) {
        TezContainerLogAppender claAppender = (TezContainerLogAppender) appender;
        containerLogDir = claAppender.getContainerLogDir();
        claAppender.setLogFileName(constructLogFileName(
            TezConfiguration.TEZ_CONTAINER_LOG_FILE_NAME, addend));
        claAppender.activateOptions();
      } else {
        LOG.warn("Appender is a " + appender.getClass() + "; require an instance of "
            + TezContainerLogAppender.class.getName() + " to reconfigure the logger output");
      }
    } else {
      LOG.warn("Not configured with appender named: " + TezConfiguration.TEZ_CONTAINER_LOGGER_NAME
          + ". Cannot reconfigure logger output");
    }

    if (containerLogDir != null) {
      System.setOut(new PrintStream(new File(containerLogDir, constructLogFileName(
          TezConfiguration.TEZ_CONTAINER_OUT_FILE_NAME, addend))));
      System.setErr(new PrintStream(new File(containerLogDir, constructLogFileName(
          TezConfiguration.TEZ_CONTAINER_ERR_FILE_NAME, addend))));
    }
  }

  private static String constructLogFileName(String base, String addend) {
    if (addend == null || addend.isEmpty()) {
      return base;
    } else {
      return base + "_" + addend;
    }
  }
}
