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
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestUnorderedKVInputConfig {

  @Test(timeout = 5000)
  public void testNullParams() {
    try {
      UnorderedKVInputConfig.newBuilder(null, "VALUE");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      UnorderedKVInputConfig.newBuilder("KEY", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }
  }

  @Test(timeout = 5000)
  public void testSetters() {
    Configuration fromConf = new Configuration(false);
    fromConf.set("test.conf.key.1", "confkey1");
    fromConf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 1111);
    fromConf.set("io.shouldExist", "io");
    fromConf.set("ssl.shouldExist", "ssl");
    Map<String, String> additionalConf = new HashMap<String, String>();
    additionalConf.put("test.key.2", "key2");
    additionalConf.put(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, "3");
    additionalConf.put("file.shouldExist", "file");
    Configuration fromConfUnfiltered = new Configuration(false);
    fromConfUnfiltered.set("test.conf.unfiltered.1", "unfiltered1");
    UnorderedKVInputConfig.Builder builder =
        UnorderedKVInputConfig.newBuilder("KEY", "VALUE")
            .setCompression(true, "CustomCodec", null)
            .setMaxSingleMemorySegmentFraction(0.11f)
            .setMergeFraction(0.22f)
            .setShuffleBufferFraction(0.33f)
            .setAdditionalConfiguration("fs.shouldExist", "fs")
            .setAdditionalConfiguration("test.key.1", "key1")
            .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
                String.valueOf(false))
            .setAdditionalConfiguration(additionalConf)
            .setFromConfiguration(fromConf)
            .setFromConfigurationUnfiltered(fromConfUnfiltered);

    UnorderedKVInputConfig configuration = builder.build();

    UnorderedKVInputConfig rebuilt = new UnorderedKVInputConfig();
    rebuilt.fromUserPayload(configuration.toUserPayload());

    Configuration conf = rebuilt.conf;

    // Verify programmatic API usage
    assertEquals("KEY", conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, ""));
    assertEquals("VALUE", conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, ""));
    assertEquals("CustomCodec",
        conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
    assertEquals(true, conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS,
        false));
    assertEquals(0.11f, conf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f, conf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f), 0.001f);
    assertEquals(0.33f, conf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.00f), 0.001f);

    // Verify additional configs
    assertEquals(false, conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals(1111, conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT));
    assertEquals(3, conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, -1));
    assertEquals("io", conf.get("io.shouldExist"));
    assertEquals("file", conf.get("file.shouldExist"));
    assertEquals("fs", conf.get("fs.shouldExist"));
    assertEquals("ssl", conf.get("ssl.shouldExist"));
    assertNull(conf.get("test.conf.key.1"));
    assertNull(conf.get("test.key.1"));
    assertNull(conf.get("test.key.2"));

    assertEquals("unfiltered1", conf.get("test.conf.unfiltered.1"));
  }

  @Test(timeout = 5000)
  public void testDefaultConfigsUsed() {
    UnorderedKVInputConfig.Builder builder =
        UnorderedKVInputConfig.newBuilder("KEY", "VALUE");
    UnorderedKVInputConfig configuration = builder.build();

    UnorderedKVInputConfig rebuilt = new UnorderedKVInputConfig();
    rebuilt.fromUserPayload(configuration.toUserPayload());

    Configuration conf = rebuilt.conf;

    assertEquals(true, conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));

    // Default property present.
    assertEquals("TestCodec",
        conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));

    // Verify whatever was configured
    assertEquals("KEY", conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, ""));
    assertEquals("VALUE", conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, ""));
  }

  private final void verifySharedFetchConfigs(UserPayload payload) {
    UnorderedKVInputConfig rebuilt = new UnorderedKVInputConfig();
    rebuilt.fromUserPayload(payload);

    Configuration conf = rebuilt.conf;

    assertEquals(
        !TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT,
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT));

    assertEquals(
        !TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT,
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH,
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT));
  }

  @Test
  public void testSharedFetchConfigs() {
    UnorderedKVInputConfig.Builder builder = UnorderedKVInputConfig.newBuilder(
        "KEY", "VALUE");

    UnorderedKVInputConfig configuration = builder
        .setAdditionalConfiguration(
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
            String
                .valueOf(!TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT))
        .setAdditionalConfiguration(
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH,
            String
                .valueOf(!TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT))
        .build();

    verifySharedFetchConfigs(configuration.toUserPayload());
    /* test from configuration */

    Configuration conf = new Configuration();
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
        String
            .valueOf(!TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT));
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH,
        String
            .valueOf(!TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT));
    configuration = UnorderedKVInputConfig.newBuilder("KEY", "VALUE")
        .setFromConfiguration(conf).build();

    verifySharedFetchConfigs(configuration.toUserPayload());
  }
}
