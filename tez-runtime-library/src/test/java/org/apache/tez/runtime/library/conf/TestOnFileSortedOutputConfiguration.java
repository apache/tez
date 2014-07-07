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

public class TestOnFileSortedOutputConfiguration {

  @Test
  public void testNullParams() {
    try {
      OnFileSortedOutputConfiguration.newBuilder(
          null, "VALUE", "PARTITIONER", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      OnFileSortedOutputConfiguration.newBuilder(
          "KEY", null, "PARTITIONER", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      OnFileSortedOutputConfiguration.newBuilder(
          "KEY", "VALUE", null, null);
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
    fromConf.set("fs.shouldExist", "fs");
    Map<String, String> additionalConf = new HashMap<String, String>();
    additionalConf.put("test.key.2", "key2");
    additionalConf.put("io.shouldExist", "io");
    additionalConf.put(TezJobConfig.TEZ_RUNTIME_INTERNAL_SORTER_CLASS, "TestInternalSorter");
    OnFileSortedOutputConfiguration.Builder builder =
        OnFileSortedOutputConfiguration.newBuilder("KEY", "VALUE", "PARTITIONER", null)
            .setKeyComparatorClass("KEY_COMPARATOR")
            .enableCompression("CustomCodec")
            .setSortBufferSize(2048)
            .setAdditionalConfiguration("test.key.1", "key1")
            .setAdditionalConfiguration("file.shouldExist", "file")
            .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
                String.valueOf(false))
            .setAdditionalConfiguration(additionalConf)
            .setFromConfiguration(fromConf);

    OnFileSortedOutputConfiguration configuration = builder.build();


    byte[] confBytes = configuration.toByteArray();
    OnFileSortedOutputConfiguration rebuilt = new OnFileSortedOutputConfiguration();
    rebuilt.fromByteArray(confBytes);

    Configuration conf = rebuilt.conf;

    // Verify programmatic API usage
    assertEquals(2048, conf.getInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_MB, 0));
    assertEquals("KEY", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS, ""));
    assertEquals("VALUE", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS, ""));
    assertEquals("PARTITIONER", conf.get(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS, ""));
    assertEquals("CustomCodec",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, ""));
    assertEquals(true, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS,
        false));
    assertEquals("KEY_COMPARATOR", conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_COMPARATOR_CLASS));

    // Verify additional configs
    assertEquals(false, conf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals(1111, conf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT));
    assertEquals("TestInternalSorter",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERNAL_SORTER_CLASS, ""));
    assertEquals("io", conf.get("io.shouldExist"));
    assertEquals("file", conf.get("file.shouldExist"));
    assertEquals("fs", conf.get("fs.shouldExist"));
    assertNull(conf.get("test.conf.key.1"));
    assertNull(conf.get("test.key.1"));
    assertNull(conf.get("test.key.2"));
  }

  @Test
  public void testDefaultConfigsUsed() {
    OnFileSortedOutputConfiguration.Builder builder =
        OnFileSortedOutputConfiguration.newBuilder("KEY", "VALUE", "PARTITIONER", null);
    OnFileSortedOutputConfiguration configuration = builder.build();

    byte[] confBytes = configuration.toByteArray();
    OnFileSortedOutputConfiguration rebuilt = new OnFileSortedOutputConfiguration();
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
    assertEquals("PARTITIONER", conf.get(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS, ""));
  }

  @Test
  public void testPartitionerConfigs() {
    Configuration partitionerConf = new Configuration(false);
    partitionerConf.set("partitioner.test.key", "PARTITIONERKEY");
    partitionerConf
        .set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, "InvalidKeyOverride");
    OnFileSortedOutputConfiguration.Builder builder =
        OnFileSortedOutputConfiguration.newBuilder("KEY", "VALUE", "PARTITIONER", partitionerConf);

    OnFileSortedOutputConfiguration configuration = builder.build();

    byte[] confBytes = configuration.toByteArray();
    OnFileSortedOutputConfiguration rebuilt = new OnFileSortedOutputConfiguration();
    rebuilt.fromByteArray(confBytes);

    Configuration conf = rebuilt.conf;

    // Default Output property should not be overridden based on partitioner config
    assertEquals("TestCodec",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, ""));

    assertEquals("PARTITIONERKEY", conf.get("partitioner.test.key"));
  }

  @Test
  public void testCombinerConfigs() {
    Configuration combinerConf = new Configuration(false);
    combinerConf.set("combiner.test.key", "COMBINERKEY");
    combinerConf
        .set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, "InvalidKeyOverride");
    OnFileSortedOutputConfiguration.Builder builder =
        OnFileSortedOutputConfiguration.newBuilder("KEY", "VALUE", "PARTITIONER", null)
            .setCombiner("COMBINER", combinerConf);

    OnFileSortedOutputConfiguration configuration = builder.build();

    byte[] confBytes = configuration.toByteArray();
    OnFileSortedOutputConfiguration rebuilt = new OnFileSortedOutputConfiguration();
    rebuilt.fromByteArray(confBytes);

    Configuration conf = rebuilt.conf;

    // Default Output property should not be overridden based on partitioner config
    assertEquals("TestCodec",
        conf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, ""));

    assertEquals("COMBINERKEY", conf.get("combiner.test.key"));
  }
}
