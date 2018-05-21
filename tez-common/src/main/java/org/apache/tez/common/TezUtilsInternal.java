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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Appender;
import org.apache.tez.common.io.NonSyncByteArrayOutputStream;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tez.dag.records.TaskAttemptTerminationCause;

@Private
public class TezUtilsInternal {

  private static final Logger LOG = LoggerFactory.getLogger(TezUtilsInternal.class);

  public static ConfigurationProto readUserSpecifiedTezConfiguration(String baseDir) throws
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
    return confProto;
  }

  public static void addUserSpecifiedTezConfiguration(Configuration conf,
                                                      List<PlanKeyValuePair> kvPairList) {
    if (kvPairList != null && !kvPairList.isEmpty()) {
      for (PlanKeyValuePair kvPair : kvPairList) {
        conf.set(kvPair.getKey(), kvPair.getValue());
      }
    }
  }
//
//  public static void addUserSpecifiedTezConfiguration(String baseDir, Configuration conf) throws
//      IOException {
//    FileInputStream confPBBinaryStream = null;
//    ConfigurationProto.Builder confProtoBuilder = ConfigurationProto.newBuilder();
//    try {
//      confPBBinaryStream =
//          new FileInputStream(new File(baseDir, TezConstants.TEZ_PB_BINARY_CONF_NAME));
//      confProtoBuilder.mergeFrom(confPBBinaryStream);
//    } finally {
//      if (confPBBinaryStream != null) {
//        confPBBinaryStream.close();
//      }
//    }
//
//    ConfigurationProto confProto = confProtoBuilder.build();
//
//    List<PlanKeyValuePair> kvPairList = confProto.getConfKeyValuesList();
//    if (kvPairList != null && !kvPairList.isEmpty()) {
//      for (PlanKeyValuePair kvPair : kvPairList) {
//        conf.set(kvPair.getKey(), kvPair.getValue());
//      }
//    }
//  }


  public static byte[] compressBytes(byte[] inBytes) throws IOException {
    StopWatch sw = new StopWatch().start();
    byte[] compressed = compressBytesInflateDeflate(inBytes);
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("UncompressedSize: " + inBytes.length + ", CompressedSize: " + compressed.length
          + ", CompressTime: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return compressed;
  }

  public static byte[] uncompressBytes(byte[] inBytes) throws IOException {
    StopWatch sw = new StopWatch().start();
    byte[] uncompressed = uncompressBytesInflateDeflate(inBytes);
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("CompressedSize: " + inBytes.length + ", UncompressedSize: " + uncompressed.length
          + ", UncompressTimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return uncompressed;
  }

  private static byte[] compressBytesInflateDeflate(byte[] inBytes) {
    Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    deflater.setInput(inBytes);
    NonSyncByteArrayOutputStream bos = new NonSyncByteArrayOutputStream(inBytes.length);
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
    NonSyncByteArrayOutputStream bos = new NonSyncByteArrayOutputStream(inBytes.length);
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

    Appender appender = org.apache.log4j.Logger.getRootLogger().getAppender(
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
    byte[] bytes = new byte[(bits.length() + 7) / 8];
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i)) {
        bytes[(bytes.length) - (i / 8) - 1] |= 1 << (i % 8);
      }
    }
    return bytes;
  }

  /**
   * Convert DAGPlan to text. Skip sensitive informations like credentials.
   *
   * @param dagPlan
   * @return a string representation of the dag plan with sensitive information removed
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
            ByteString.copyFrom(TezCommonUtils.getCredentialsInfo(credentials,"dag").getBytes(
                Charset.forName("UTF-8"))), sb);
      }
    }
    return sb.toString();
  }

  public static TaskAttemptTerminationCause fromTaskAttemptEndReason(
      TaskAttemptEndReason taskAttemptEndReason) {
    if (taskAttemptEndReason == null) {
      return null;
    }
    switch (taskAttemptEndReason) {
      case COMMUNICATION_ERROR:
        return TaskAttemptTerminationCause.COMMUNICATION_ERROR;
      case EXECUTOR_BUSY:
        return TaskAttemptTerminationCause.SERVICE_BUSY;
      case INTERNAL_PREEMPTION:
        return TaskAttemptTerminationCause.INTERNAL_PREEMPTION;
      case EXTERNAL_PREEMPTION:
        return TaskAttemptTerminationCause.EXTERNAL_PREEMPTION;
      case APPLICATION_ERROR:
        return TaskAttemptTerminationCause.APPLICATION_ERROR;
      case FRAMEWORK_ERROR:
        return TaskAttemptTerminationCause.FRAMEWORK_ERROR;
      case NODE_FAILED:
        return TaskAttemptTerminationCause.NODE_FAILED;
      case CONTAINER_EXITED:
        return TaskAttemptTerminationCause.CONTAINER_EXITED;
      case OTHER:
        return TaskAttemptTerminationCause.UNKNOWN_ERROR;
      default:
        return TaskAttemptTerminationCause.UNKNOWN_ERROR;
    }
  }

  public static TaskAttemptEndReason toTaskAttemptEndReason(TaskAttemptTerminationCause cause) {
    // TODO Post TEZ-2003. Consolidate these states, and mappings.
    if (cause == null) {
      return null;
    }
    switch (cause) {
      case COMMUNICATION_ERROR:
        return TaskAttemptEndReason.COMMUNICATION_ERROR;
      case SERVICE_BUSY:
        return TaskAttemptEndReason.EXECUTOR_BUSY;
      case INTERNAL_PREEMPTION:
        return TaskAttemptEndReason.INTERNAL_PREEMPTION;
      case EXTERNAL_PREEMPTION:
        return TaskAttemptEndReason.EXTERNAL_PREEMPTION;
      case APPLICATION_ERROR:
        return TaskAttemptEndReason.APPLICATION_ERROR;
      case FRAMEWORK_ERROR:
        return TaskAttemptEndReason.FRAMEWORK_ERROR;
      case NODE_FAILED:
        return TaskAttemptEndReason.NODE_FAILED;
      case CONTAINER_EXITED:
        return TaskAttemptEndReason.CONTAINER_EXITED;
      case INTERRUPTED_BY_SYSTEM:
      case INTERRUPTED_BY_USER:
      case UNKNOWN_ERROR:
      case TERMINATED_BY_CLIENT:
      case TERMINATED_AT_SHUTDOWN:
      case TERMINATED_INEFFECTIVE_SPECULATION:
      case TERMINATED_EFFECTIVE_SPECULATION:
      case TERMINATED_ORPHANED:
      case INPUT_READ_ERROR:
      case OUTPUT_WRITE_ERROR:
      case OUTPUT_LOST:
      case TASK_HEARTBEAT_ERROR:
      case CONTAINER_LAUNCH_FAILED:

      case CONTAINER_STOPPED:
      case NODE_DISK_ERROR:
      default:
        return TaskAttemptEndReason.OTHER;
    }
  }

  public static <T extends Enum<T>> Set<T> getEnums(Configuration conf, String confName,
      Class<T> enumType, String defaultValues) {
    String[] names = conf.getStrings(confName);
    if (names == null) {
      names = StringUtils.getStrings(defaultValues);
    }
    if (names == null) {
      return null;
    }
    Set<T> enums = new HashSet<>();
    for (String name : names) {
      enums.add(Enum.valueOf(enumType, name));
    }
    return enums;
  }

  @Private
  public static void setHadoopCallerContext(HadoopShim hadoopShim, TezTaskAttemptID attemptID) {
    hadoopShim.setHadoopCallerContext("tez_ta:" + attemptID.toString());
  }

  @Private
  public static void setHadoopCallerContext(HadoopShim hadoopShim, TezVertexID vertexID) {
    hadoopShim.setHadoopCallerContext("tez_v:" + vertexID.toString());
  }

  @Private
  public static void setHadoopCallerContext(HadoopShim hadoopShim, TezDAGID dagID) {
    hadoopShim.setHadoopCallerContext("tez_dag:" + dagID.toString());
  }

  @Private
  public static void setHadoopCallerContext(HadoopShim hadoopShim, ApplicationId appID) {
    hadoopShim.setHadoopCallerContext("tez_app:" + appID.toString());
  }

  @Private
  public static void setSecurityUtilConfigration(Logger log, Configuration conf) {
    // Use reflection to invoke SecurityUtil.setConfiguration when available, version 2.6.0 of
    // hadoop does not support it, it is currently available from 2.9.0.
    // Remove this when the minimum supported hadoop version has the above method.
    Class<SecurityUtil> clz = SecurityUtil.class;
    try {
      Method method = clz.getMethod("setConfiguration", Configuration.class);
      method.invoke(null, conf);
    } catch (NoSuchMethodException e) {
      // This is not available, so ignore it.
    } catch (SecurityException | IllegalAccessException | IllegalArgumentException |
        InvocationTargetException e) {
      log.warn("Error invoking SecurityUtil.setConfiguration: ", e);
      throw new TezUncheckedException("Error invoking SecurityUtil.setConfiguration", e);
    }
  }
}
