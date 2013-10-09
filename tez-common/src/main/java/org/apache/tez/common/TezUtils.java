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

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class TezUtils {
  
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
    DataOutputStream dos = new DataOutputStream(os);
    conf.write(dos);
    return os.toByteString();
  }
  
  public static byte[] createUserPayloadFromConf(Configuration conf)
      throws IOException {
    Preconditions.checkNotNull(conf, "Configuration must be specified");
    DataOutputBuffer dob = new DataOutputBuffer();
    conf.write(dob);
    return dob.getData();
  }

  public static Configuration createConfFromByteString(ByteString byteString)
      throws IOException {
    Preconditions.checkNotNull(byteString, "ByteString must be specified");
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    dibb.reset(byteString.asReadOnlyByteBuffer());
    Configuration conf = new Configuration(false);
    conf.readFields(dibb);
    return conf;
  }
  
  public static Configuration createConfFromUserPayload(byte[] bb)
      throws IOException {
    // TODO Avoid copy ?
    Preconditions.checkNotNull(bb, "Bytes must be specified");
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(bb, 0, bb.length);
    Configuration conf = new Configuration(false);
    conf.readFields(dib);
    return conf;
  }

}
