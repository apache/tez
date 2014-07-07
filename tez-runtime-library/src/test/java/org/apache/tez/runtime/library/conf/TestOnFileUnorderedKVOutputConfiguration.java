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

public class TestOnFileUnorderedKVOutputConfiguration {

  @Test
  public void testNullParams() {
    try {
      OnFileUnorderedKVOutputConfiguration.newBuilder(
          null, "VALUE");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      OnFileUnorderedKVOutputConfiguration.newBuilder(
          "KEY", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testSetters() {
    Configuration fromConf = new Configuration(false);
    fromConf.set("test.conf.key.1", "confkey1");
    fromConf.setInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 1111);
    fromConf.set("io.shouldExist", "io");
    Map<String, String> additionalConf = new HashMap<String, String>();
    additionalConf.put("test.key.2", "key2");
    additionalConf.put(TezJobConfig.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES, "2222");
    additionalConf.put("file.shouldExist", "file");
    OnFileUnorderedKVOutputConfiguration.Builder builder =
        OnFileUnorderedKVOutputConfiguration.newBuilder("KEY", "VALUE")
            .enableCompression("CustomCodec")
            .setAdditionalConfiguration("fs.shouldExist", "fs")
            .setAdditionalConfiguration("test.key.1", "key1")
            .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
                String.valueOf(false))
            .setAdditionalConfiguration(additionalConf)
            .setFromConfiguration(fromConf);

    OnFileUnorderedKVOutputConfiguration configuration = builder.build();


    byte[] confBytes = configuration.toByteArray();
    OnFileUnorderedKVOutputConfiguration rebuilt =
        new OnFileUnorderedKVOutputConfiguration();
    rebuilt.fromByteArray(confBytes);

    Configuration conf = rebuilt.conf;

    // Verify programmatic API usage
    assertEquals("KEY", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS, ""));
    assertEquals("VALUE", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS, ""));
    assertEquals("CustomCodec",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, ""));
    assertEquals(true, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS,
        false));

    // Verify additional configs
    assertEquals(false, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals(1111, conf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT));
    assertEquals("io", conf.get("io.shouldExist"));
    assertEquals("file", conf.get("file.shouldExist"));
    assertEquals("fs", conf.get("fs.shouldExist"));
    assertNull(conf.get("test.conf.key.1"));
    assertNull(conf.get("test.key.1"));
    assertNull(conf.get("test.key.2"));
  }

  @Test
  public void testDefaultConfigsUsed() {
    OnFileUnorderedKVOutputConfiguration.Builder builder =
        OnFileUnorderedKVOutputConfiguration
            .newBuilder("KEY", "VALUE");
    OnFileUnorderedKVOutputConfiguration configuration = builder.build();

    byte[] confBytes = configuration.toByteArray();
    OnFileUnorderedKVOutputConfiguration rebuilt =
        new OnFileUnorderedKVOutputConfiguration();
    rebuilt.fromByteArray(confBytes);

    Configuration conf = rebuilt.conf;

    assertEquals(true, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));

    // Default Output property present.
    assertEquals("TestCodec",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, ""));
    // Input property should be absent
    assertEquals("DEFAULT",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC, "DEFAULT"));

    // Verify whatever was configured
    assertEquals("KEY", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS, ""));
    assertEquals("VALUE", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS, ""));
  }
}
