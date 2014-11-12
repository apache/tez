/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.runtime.library.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestUnorderedKVEdgeConfig {

  @Test (timeout=2000)
  public void testNullParams() {
    try {
      UnorderedKVEdgeConfig.newBuilder(null, "VALUE");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      UnorderedKVEdgeConfig.newBuilder("KEY", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }
  }

  @Test (timeout=2000)
  public void testDefaultConfigsUsed() {
    UnorderedKVEdgeConfig.Builder builder =
        UnorderedKVEdgeConfig.newBuilder("KEY", "VALUE");
    builder.setKeySerializationClass("SerClass1", null).setValueSerializationClass("SerClass2", null);

    UnorderedKVEdgeConfig configuration = builder.build();


    UnorderedKVOutputConfig rebuiltOutput =
        new UnorderedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    UnorderedKVInputConfig rebuiltInput =
        new UnorderedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    assertEquals(true, outputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals("TestCodec",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
    assertTrue(outputConf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY).startsWith
        ("SerClass2,SerClass1"));

    Configuration inputConf = rebuiltInput.conf;
    assertEquals(true, inputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals("TestCodec",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
    assertTrue(inputConf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY).startsWith
        ("SerClass2,SerClass1"));
  }

  @Test (timeout=2000)
  public void testSpecificIOConfs() {
    // Ensures that Output and Input confs are not mixed.
    UnorderedKVEdgeConfig.Builder builder =
        UnorderedKVEdgeConfig.newBuilder("KEY", "VALUE");

    UnorderedKVEdgeConfig configuration = builder.build();

    UnorderedKVOutputConfig rebuiltOutput =
        new UnorderedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    UnorderedKVInputConfig rebuiltInput =
        new UnorderedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    assertEquals("TestCodec",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, "DEFAULT"));

    Configuration inputConf = rebuiltInput.conf;
    assertEquals("TestCodec",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, "DEFAULT"));
  }

  @Test (timeout=2000)
  public void tetCommonConf() {

    Configuration fromConf = new Configuration(false);
    fromConf.set("test.conf.key.1", "confkey1");
    fromConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD, false);
    fromConf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.11f);
    fromConf.set("io.shouldExist", "io");
    Map<String, String> additionalConfs = new HashMap<String, String>();
    additionalConfs.put("test.key.2", "key2");
    additionalConfs.put(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, "1111");
    additionalConfs.put(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, "0.22f");
    additionalConfs.put("file.shouldExist", "file");

    UnorderedKVEdgeConfig.Builder builder = UnorderedKVEdgeConfig
        .newBuilder("KEY",
            "VALUE")
        .setAdditionalConfiguration("fs.shouldExist", "fs")
        .setAdditionalConfiguration("test.key.1", "key1")
        .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, "3333")
        .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, "0.33f")
        .setAdditionalConfiguration(additionalConfs)
        .setFromConfiguration(fromConf);

    UnorderedKVEdgeConfig configuration = builder.build();

    UnorderedKVOutputConfig rebuiltOutput =
        new UnorderedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    UnorderedKVInputConfig rebuiltInput =
        new UnorderedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    Configuration inputConf = rebuiltInput.conf;

    assertEquals(false, outputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD, true));
    assertEquals(1111, outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(3333, outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT));
    assertEquals("io", outputConf.get("io.shouldExist"));
    assertEquals("file", outputConf.get("file.shouldExist"));
    assertEquals("fs", outputConf.get("fs.shouldExist"));


    assertEquals(false, inputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD, true));
    assertEquals(1111, inputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(3333, inputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertEquals(0.11f,
        inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f,
        inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.33f, inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f),
        0.001f);
    assertEquals("io", inputConf.get("io.shouldExist"));
    assertEquals("file", inputConf.get("file.shouldExist"));
    assertEquals("fs", inputConf.get("fs.shouldExist"));

  }

  private void checkHistoryText(String historyText) {
    assertNotNull(historyText);
    assertTrue(historyText.contains(
        TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT));
  }

  @Test (timeout=2000)
  public void testHistoryText() {
    UnorderedKVEdgeConfig.Builder builder = UnorderedKVEdgeConfig.newBuilder("KEY", "VALUE");
    Configuration fromConf = new Configuration(false);
    fromConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT,
        true);
    builder.setFromConfiguration(fromConf);

    UnorderedKVEdgeConfig kvEdgeConfig = builder.build();

    checkHistoryText(kvEdgeConfig.getInputHistoryText());
    checkHistoryText(kvEdgeConfig.getOutputHistoryText());

    EdgeProperty defaultEdgeProperty = builder.build().createDefaultBroadcastEdgeProperty();
    checkHistoryText(defaultEdgeProperty.getEdgeDestination().getHistoryText());
    checkHistoryText(defaultEdgeProperty.getEdgeSource().getHistoryText());

    defaultEdgeProperty = builder.build().createDefaultOneToOneEdgeProperty();
    checkHistoryText(defaultEdgeProperty.getEdgeDestination().getHistoryText());
    checkHistoryText(defaultEdgeProperty.getEdgeSource().getHistoryText());

    EdgeManagerPluginDescriptor descriptor = mock(EdgeManagerPluginDescriptor.class);
    EdgeProperty edgeProperty = builder.build().createDefaultCustomEdgeProperty(descriptor);
    checkHistoryText(edgeProperty.getEdgeDestination().getHistoryText());
    checkHistoryText(edgeProperty.getEdgeSource().getHistoryText());

  }


}
