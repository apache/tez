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

public class TestShuffledUnorderedKVInputConfiguration {

  @Test
  public void testNullParams() {
    try {
      ShuffledUnorderedKVInputConfiguration.newBuilder(null, "VALUE");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      ShuffledUnorderedKVInputConfiguration.newBuilder("KEY", null);
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
    additionalConf.put(TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, "3");
    additionalConf.put("file.shouldExist", "file");
    ShuffledUnorderedKVInputConfiguration.Builder builder =
        ShuffledUnorderedKVInputConfiguration.newBuilder("KEY", "VALUE")
            .enableCompression("CustomCodec")
            .setMaxSingleMemorySegmentFraction(0.11f)
            .setMergeFraction(0.22f)
            .setShuffleBufferFraction(0.33f)
            .setAdditionalConfiguration("fs.shouldExist", "fs")
            .setAdditionalConfiguration("test.key.1", "key1")
            .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
                String.valueOf(false))
            .setAdditionalConfiguration(additionalConf)
            .setFromConfiguration(fromConf);

    ShuffledUnorderedKVInputConfiguration configuration = builder.build();


    byte[] confBytes = configuration.toByteArray();
    ShuffledUnorderedKVInputConfiguration rebuilt = new ShuffledUnorderedKVInputConfiguration();
    rebuilt.fromByteArray(confBytes);

    Configuration conf = rebuilt.conf;

    // Verify programmatic API usage
    assertEquals("KEY", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS, ""));
    assertEquals("VALUE", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS, ""));
    assertEquals("CustomCodec",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC, ""));
    assertEquals(true, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_IS_COMPRESSED,
        false));
    assertEquals(0.11f, conf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f, conf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f), 0.001f);
    assertEquals(0.33f, conf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 0.00f), 0.001f);

    // Verify additional configs
    assertEquals(false, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals(1111, conf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT));
    assertEquals(3, conf.getInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, -1));
    assertEquals("io", conf.get("io.shouldExist"));
    assertEquals("file", conf.get("file.shouldExist"));
    assertEquals("fs", conf.get("fs.shouldExist"));
    assertNull(conf.get("test.conf.key.1"));
    assertNull(conf.get("test.key.1"));
    assertNull(conf.get("test.key.2"));
  }

  @Test
  public void testDefaultConfigsUsed() {
    ShuffledUnorderedKVInputConfiguration.Builder builder =
        ShuffledUnorderedKVInputConfiguration.newBuilder("KEY", "VALUE");
    ShuffledUnorderedKVInputConfiguration configuration = builder.build();

    byte[] confBytes = configuration.toByteArray();
    ShuffledUnorderedKVInputConfiguration rebuilt = new ShuffledUnorderedKVInputConfiguration();
    rebuilt.fromByteArray(confBytes);

    Configuration conf = rebuilt.conf;

    assertEquals(true, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));

    // Default Output property present.
    assertEquals("TestCodec",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC, ""));
    // Input property should be absent
    assertEquals("DEFAULT",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, "DEFAULT"));

    // Verify whatever was configured
    assertEquals("KEY", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS, ""));
    assertEquals("VALUE", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS, ""));
  }
}
