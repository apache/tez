/*
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

package org.apache.tez.common;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.records.DAGProtos;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


/**
 * Utility methods for setting up a DAG. Has helpers for setting up log4j configuration, converting
 * {@link org.apache.hadoop.conf.Configuration} to {@link org.apache.tez.dag.api.UserPayload} etc.
 */
@InterfaceAudience.Public
public class TezUtils {

  private static final Log LOG = LogFactory.getLog(TezUtils.class);

  /**
   * Allows changing the log level for task / AM logging. </p>
   *
   * Adds the JVM system properties necessary to configure
   * {@link org.apache.hadoop.yarn.ContainerLogAppender}.
   *
   * @param logLevel the desired log level (eg INFO/WARN/DEBUG)
   * @param vargs    the argument list to append to
   */
  public static void addLog4jSystemProperties(String logLevel,
                                              List<String> vargs) {
    TezClientUtils.addLog4jSystemProperties(logLevel, vargs);
  }


  /**
   * Convert a Configuration to compressed ByteString using Protocol buffer
   *
   * @param conf
   *          : Configuration to be converted
   * @return PB ByteString (compressed)
   * @throws java.io.IOException
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
   * Convert a Configuration to a {@link org.apache.tez.dag.api.UserPayload} </p>
   *
   *
   * @param conf configuration to be converted
   * @return an instance of {@link org.apache.tez.dag.api.UserPayload}
   * @throws java.io.IOException
   */
  public static UserPayload createUserPayloadFromConf(Configuration conf) throws IOException {
    return UserPayload.create(createByteStringFromConf(conf).asReadOnlyByteBuffer());
  }

  /**
   * Convert a byte string to a Configuration object
   *
   * @param byteString byteString representation of the conf created using {@link
   *                   #createByteStringFromConf(org.apache.hadoop.conf.Configuration)}
   * @return Configuration
   * @throws java.io.IOException
   */
  public static Configuration createConfFromByteString(ByteString byteString) throws IOException {
    Preconditions.checkNotNull(byteString, "ByteString must be specified");
    // SnappyInputStream uncompressIs = new
    // SnappyInputStream(byteString.newInput());
    InflaterInputStream uncompressIs = new InflaterInputStream(byteString.newInput());
    DAGProtos.ConfigurationProto confProto = DAGProtos.ConfigurationProto.parseFrom(uncompressIs);
    Configuration conf = new Configuration(false);
    readConfFromPB(confProto, conf);
    return conf;
  }

  /**
   * Convert an instance of {@link org.apache.tez.dag.api.UserPayload} to {@link
   * org.apache.hadoop.conf.Configuration}
   *
   * @param payload {@link org.apache.tez.dag.api.UserPayload} created using {@link
   *                #createUserPayloadFromConf(org.apache.hadoop.conf.Configuration)}
   * @return Configuration
   * @throws java.io.IOException
   */
  public static Configuration createConfFromUserPayload(UserPayload payload) throws IOException {
    return createConfFromByteString(ByteString.copyFrom(payload.getPayload()));
  }


  private static void writeConfInPB(OutputStream dos, Configuration conf) throws IOException {
    DAGProtos.ConfigurationProto.Builder confProtoBuilder = DAGProtos.ConfigurationProto
        .newBuilder();
    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> entry = iter.next();
      DAGProtos.PlanKeyValuePair.Builder kvp = DAGProtos.PlanKeyValuePair.newBuilder();
      kvp.setKey(entry.getKey());
      kvp.setValue(entry.getValue());
      confProtoBuilder.addConfKeyValues(kvp);
    }
    DAGProtos.ConfigurationProto confProto = confProtoBuilder.build();
    confProto.writeTo(dos);
  }

  private static void readConfFromPB(DAGProtos.ConfigurationProto confProto, Configuration conf) {
    List<DAGProtos.PlanKeyValuePair> settingList = confProto.getConfKeyValuesList();
    for (DAGProtos.PlanKeyValuePair setting : settingList) {
      conf.set(setting.getKey(), setting.getValue());
    }
  }

  public static String convertToHistoryText(String description, Configuration conf) {
    // Add a version if this serialization is changed
    JSONObject jsonObject = new JSONObject();
    try {
      if (description != null && !description.isEmpty()) {
        jsonObject.put(ATSConstants.DESCRIPTION, description);
      }
      if (conf != null) {
        JSONObject confJson = new JSONObject();
        Iterator<Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
          Entry<String, String> entry = iter.next();
          confJson.put(entry.getKey(), entry.getValue());
        }
        jsonObject.put(ATSConstants.CONFIG, confJson);
      }
    } catch (JSONException e) {
      throw new TezUncheckedException("Error when trying to convert description/conf to JSON", e);
    }
    return jsonObject.toString();
  }

  public static String convertToHistoryText(Configuration conf) {
    return convertToHistoryText(null, conf);
  }


}
