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
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.TextFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;

import com.google.common.base.Stopwatch;

@Private
public class TezUtilsInternal {

  private static final Log LOG = LogFactory.getLog(TezUtilsInternal.class);
  private static final Random RANDOM = new Random();

  public static void addUserSpecifiedTezConfiguration(String baseDir, Configuration conf) throws
      IOException {
    FileInputStream confPBBinaryStream = null;
    ConfigurationProto.Builder confProtoBuilder = ConfigurationProto.newBuilder();
    try {
      confPBBinaryStream =
          new FileInputStream(new File(baseDir, TezConstants.TEZ_PB_BINARY_CONF_NAME));
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

  private static final Pattern pattern = Pattern.compile("\\W");
  @Private
  public static final int MAX_VERTEX_NAME_LENGTH = 40;

  @Private
  public static String cleanVertexName(String vertexName) {
    return sanitizeString(vertexName).substring(0,
        vertexName.length() > MAX_VERTEX_NAME_LENGTH ? MAX_VERTEX_NAME_LENGTH : vertexName.length());
  }

  private static String sanitizeString(String srcString) {
    Matcher matcher = pattern.matcher(srcString);
    String res = matcher.replaceAll("_");
    return res; // Number starts allowed rightnow
  }

  public static void updateLoggers(String addend) throws FileNotFoundException {

    LOG.info("Redirecting log file based on addend: " + addend);

    Appender appender = Logger.getRootLogger().getAppender(
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);
    if (appender != null) {
      if (appender instanceof TezContainerLogAppender) {
        TezContainerLogAppender claAppender = (TezContainerLogAppender) appender;
        claAppender.setLogFileName(constructLogFileName(
            TezConstants.TEZ_CONTAINER_LOG_FILE_NAME, addend));
        claAppender.activateOptions();
      } else {
        LOG.warn("Appender is a " + appender.getClass() + "; require an instance of "
            + TezContainerLogAppender.class.getName() + " to reconfigure the logger output");
      }
    } else {
      LOG.warn("Not configured with appender named: " + TezConstants.TEZ_CONTAINER_LOGGER_NAME
          + ". Cannot reconfigure logger output");
    }
  }

  private static String constructLogFileName(String base, String addend) {
    if (addend == null || addend.isEmpty()) {
      return base;
    } else {
      return base + "_" + addend;
    }
  }

  public static BitSet fromByteArray(byte[] bytes) {
    if (bytes == null) {
      return new BitSet();
    }
    BitSet bits = new BitSet();
    for (int i = 0; i < bytes.length * 8; i++) {
      if ((bytes[(bytes.length) - (i / 8) - 1] & (1 << (i % 8))) > 0) {
        bits.set(i);
      }
    }
    return bits;
  }

  public static byte[] toByteArray(BitSet bits) {
    if (bits == null) {
      return null;
    }
    byte[] bytes = new byte[bits.length() / 8 + 1];
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i)) {
        bytes[(bytes.length) - (i / 8) - 1] |= 1 << (i % 8);
      }
    }
    return bytes;
  }

  public static String getContainerLogDir() {
    String logDirsStr  = System.getenv(Environment.LOG_DIRS.name());
    if (logDirsStr == null || logDirsStr.isEmpty()) {
      return null;
    }
    String[] logDirs = StringUtils.split(logDirsStr, ',');
    if (logDirs.length == 0) {
      return null;
    }
    int logIndex = RANDOM.nextInt(logDirs.length);
    return logDirs[logIndex];
  }

  /**
   * Convert DAGPlan to text. Skip sensitive informations like credentials.
   *
   * @param dagPlan
   * @return
   */
  public static String convertDagPlanToString(DAGProtos.DAGPlan dagPlan) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : dagPlan.getAllFields().entrySet()) {
      if (entry.getKey().getNumber() != DAGProtos.DAGPlan.CREDENTIALS_BINARY_FIELD_NUMBER) {
        TextFormat.printField(entry.getKey(), entry.getValue(), sb);
      } else {
        Credentials credentials =
            DagTypeConverters.convertByteStringToCredentials(dagPlan.getCredentialsBinary());
        TextFormat.printField(entry.getKey(),
            ByteString.copyFrom(TezCommonUtils.getCredentialsInfo(credentials,"dag").getBytes()), sb);
      }
    }
    return sb.toString();
  }

}
