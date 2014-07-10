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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.junit.Test;

public class TestUnorderedUnpartitionedKVEdgeConfiguration {

  @Test
  public void testNullParams() {
    try {
      UnorderedUnpartitionedKVEdgeConfiguration.newBuilder(null, "VALUE");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      UnorderedUnpartitionedKVEdgeConfiguration.newBuilder("KEY", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testDefaultConfigsUsed() {
    UnorderedUnpartitionedKVEdgeConfiguration.Builder builder =
        UnorderedUnpartitionedKVEdgeConfiguration.newBuilder("KEY", "VALUE");

    UnorderedUnpartitionedKVEdgeConfiguration configuration = builder.build();

    byte[] outputBytes = configuration.getOutputPayload();
    byte[] inputBytes = configuration.getInputPayload();

    OnFileUnorderedKVOutputConfiguration rebuiltOutput =
        new OnFileUnorderedKVOutputConfiguration();
    rebuiltOutput.fromByteArray(outputBytes);
    ShuffledUnorderedKVInputConfiguration rebuiltInput =
        new ShuffledUnorderedKVInputConfiguration();
    rebuiltInput.fromByteArray(inputBytes);

    Configuration outputConf = rebuiltOutput.conf;
    assertEquals(true, outputConf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals("TestCodec",
        outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, ""));

    Configuration inputConf = rebuiltInput.conf;
    assertEquals(true, inputConf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals("TestCodec",
        inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC, ""));
  }

  @Test
  public void testSpecificIOConfs() {
    // Ensures that Output and Input confs are not mixed.
    UnorderedUnpartitionedKVEdgeConfiguration.Builder builder =
        UnorderedUnpartitionedKVEdgeConfiguration.newBuilder("KEY", "VALUE");

    UnorderedUnpartitionedKVEdgeConfiguration configuration = builder.build();

    byte[] outputBytes = configuration.getOutputPayload();
    byte[] inputBytes = configuration.getInputPayload();

    OnFileUnorderedKVOutputConfiguration rebuiltOutput =
        new OnFileUnorderedKVOutputConfiguration();
    rebuiltOutput.fromByteArray(outputBytes);
    ShuffledUnorderedKVInputConfiguration rebuiltInput =
        new ShuffledUnorderedKVInputConfiguration();
    rebuiltInput.fromByteArray(inputBytes);

    Configuration outputConf = rebuiltOutput.conf;
    assertEquals("DEFAULT",
        outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC, "DEFAULT"));

    Configuration inputConf = rebuiltInput.conf;
    assertEquals("DEFAULT",
        inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, "DEFAULT"));
  }

  @Test
  public void tetCommonConf() {

    Configuration fromConf = new Configuration(false);
    fromConf.set("test.conf.key.1", "confkey1");
    fromConf.setBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD, false);
    fromConf.setFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 0.11f);
    fromConf.set("io.shouldExist", "io");
    Map<String, String> additionalConfs = new HashMap<String, String>();
    additionalConfs.put("test.key.2", "key2");
    additionalConfs.put(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, "1111");
    additionalConfs.put(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, "0.22f");
    additionalConfs.put("file.shouldExist", "file");

    UnorderedUnpartitionedKVEdgeConfiguration.Builder builder = UnorderedUnpartitionedKVEdgeConfiguration
        .newBuilder("KEY",
            "VALUE")
        .setAdditionalConfiguration("fs.shouldExist", "fs")
        .setAdditionalConfiguration("test.key.1", "key1")
        .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, "3333")
        .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, "0.33f")
        .setAdditionalConfiguration(additionalConfs)
        .setFromConfiguration(fromConf);

    UnorderedUnpartitionedKVEdgeConfiguration configuration = builder.build();

    byte[] outputBytes = configuration.getOutputPayload();
    byte[] inputBytes = configuration.getInputPayload();

    OnFileUnorderedKVOutputConfiguration rebuiltOutput =
        new OnFileUnorderedKVOutputConfiguration();
    rebuiltOutput.fromByteArray(outputBytes);
    ShuffledUnorderedKVInputConfiguration rebuiltInput =
        new ShuffledUnorderedKVInputConfiguration();
    rebuiltInput.fromByteArray(inputBytes);

    Configuration outputConf = rebuiltOutput.conf;
    Configuration inputConf = rebuiltInput.conf;

    assertEquals(false, outputConf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD, true));
    assertEquals(1111, outputConf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(3333, outputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT));
    assertEquals("io", outputConf.get("io.shouldExist"));
    assertEquals("file", outputConf.get("file.shouldExist"));
    assertEquals("fs", outputConf.get("fs.shouldExist"));


    assertEquals(false, inputConf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD, true));
    assertEquals(1111, inputConf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(3333, inputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertEquals(0.11f,
        inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f,
        inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.33f, inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f),
        0.001f);
    assertEquals("io", inputConf.get("io.shouldExist"));
    assertEquals("file", inputConf.get("file.shouldExist"));
    assertEquals("fs", inputConf.get("fs.shouldExist"));

  }
}
