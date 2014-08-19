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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestOrderedPartitionedKVEdgeConfig {

  @Test
  public void testNullParams() {
    try {
      OrderedPartitionedKVEdgeConfig.newBuilder(null, "VALUE", "PARTITIONER");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      OrderedPartitionedKVEdgeConfig.newBuilder("KEY", null, "PARTITIONER");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      OrderedPartitionedKVEdgeConfig.newBuilder("KEY", "VALUE", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testDefaultConfigsUsed() {
    OrderedPartitionedKVEdgeConfig.Builder builder = OrderedPartitionedKVEdgeConfig
        .newBuilder("KEY", "VALUE", "PARTITIONER");

    OrderedPartitionedKVEdgeConfig configuration = builder.build();

    OrderedPartitionedKVOutputConfig
        rebuiltOutput = new OrderedPartitionedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    OrderedGroupedKVInputConfig rebuiltInput = new OrderedGroupedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    assertEquals(true, outputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals("TestCodec",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));

    Configuration inputConf = rebuiltInput.conf;
    assertEquals(true, inputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT));
    assertEquals("TestCodec",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
  }

  @Test
  public void testSpecificIOConfs() {
    // Ensures that Output and Input confs are not mixed.
    OrderedPartitionedKVEdgeConfig.Builder builder = OrderedPartitionedKVEdgeConfig
        .newBuilder("KEY", "VALUE", "PARTITIONER");

    OrderedPartitionedKVEdgeConfig configuration = builder.build();

    OrderedPartitionedKVOutputConfig
        rebuiltOutput = new OrderedPartitionedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    OrderedGroupedKVInputConfig rebuiltInput = new OrderedGroupedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    assertEquals("TestCodec",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, "DEFAULT"));

    Configuration inputConf = rebuiltInput.conf;
    assertEquals("TestCodec",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, "DEFAULT"));
  }

  @Test
  public void tetCommonConf() {

    Configuration fromConf = new Configuration(false);
    fromConf.set("test.conf.key.1", "confkey1");
    fromConf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, 3);
    fromConf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.11f);
    fromConf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 123);
    fromConf.set("io.shouldExist", "io");
    Map<String, String> additionalConfs = new HashMap<String, String>();
    additionalConfs.put("test.key.2", "key2");
    additionalConfs.put(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, "1111");
    additionalConfs.put(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, "0.22f");
    additionalConfs.put(TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS, "CustomSorter");
    additionalConfs.put("file.shouldExist", "file");

    OrderedPartitionedKVEdgeConfig.Builder builder = OrderedPartitionedKVEdgeConfig
        .newBuilder("KEY", "VALUE", "PARTITIONER")
        .setAdditionalConfiguration("fs.shouldExist", "fs")
        .setAdditionalConfiguration("test.key.1", "key1")
        .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, "2222")
        .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, "0.33f")
        .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES, "3333")
        .setAdditionalConfiguration(additionalConfs)
        .setFromConfiguration(fromConf);

    OrderedPartitionedKVEdgeConfig configuration = builder.build();

    OrderedPartitionedKVOutputConfig
        rebuiltOutput = new OrderedPartitionedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    OrderedGroupedKVInputConfig rebuiltInput = new OrderedGroupedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    Configuration inputConf = rebuiltInput.conf;

    assertEquals(3, outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, 0));
    assertEquals(1111, outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(2222, outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT));
    assertEquals(123, outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 0));
    assertEquals("CustomSorter", outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS));
    assertEquals(3333,
        outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES, 0));
    assertEquals("io", outputConf.get("io.shouldExist"));
    assertEquals("file", outputConf.get("file.shouldExist"));
    assertEquals("fs", outputConf.get("fs.shouldExist"));


    assertEquals(3, inputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, 0));
    assertEquals(1111, inputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(2222, inputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertEquals(0.11f,
        inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f,
        inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.33f, inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f),
        0.001f);
    assertNull(inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB));
    assertNull(inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS));
    assertNull(inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES));
    assertEquals("io", inputConf.get("io.shouldExist"));
    assertEquals("file", inputConf.get("file.shouldExist"));
    assertEquals("fs", inputConf.get("fs.shouldExist"));

  }

  @Test
  public void testSetters() {
    Map<String, String> comparatorConf = Maps.newHashMap();
    comparatorConf.put("comparator.test.key", "comparatorValue");
    OrderedPartitionedKVEdgeConfig.Builder builder = OrderedPartitionedKVEdgeConfig
        .newBuilder("KEY", "VALUE", "PARTITIONER")
        .setKeyComparatorClass("KEY_COMPARATOR", comparatorConf)
        .configureOutput().setSortBufferSize(1111).setSorterNumThreads(2).done()
        .configureInput().setMaxSingleMemorySegmentFraction(0.11f).setMergeFraction(0.22f)
        .setPostMergeBufferFraction(0.33f).setShuffleBufferFraction(0.44f).done()
        .setCompression(true, "CustomCodec", null);

    OrderedPartitionedKVEdgeConfig configuration = builder.build();

    OrderedPartitionedKVOutputConfig
        rebuiltOutput = new OrderedPartitionedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    OrderedGroupedKVInputConfig rebuiltInput = new OrderedGroupedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    Configuration inputConf = rebuiltInput.conf;

    assertEquals("KEY", outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, ""));
    assertEquals("VALUE",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, ""));
    assertEquals("PARTITIONER", outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, ""));
    assertEquals(1111, outputConf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 0));
    assertEquals("CustomCodec",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
    assertEquals(true,
        outputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS,
            false));
    assertEquals("KEY_COMPARATOR",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS));
    assertEquals("comparatorValue", outputConf.get("comparator.test.key"));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT));


    assertEquals("KEY_COMPARATOR", inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS));
    assertEquals("comparatorValue", inputConf.get("comparator.test.key"));
    assertEquals("KEY", inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, ""));
    assertEquals("VALUE",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, ""));
    assertEquals("CustomCodec",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
    assertEquals(true,
        inputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS,
            false));

    assertEquals(0.11f,
        inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f, inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f),
        0.001f);
    assertEquals(0.33f, inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, 0.0f),
        0.001f);
    assertEquals(0.44f,
        inputConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.00f), 0.001f);
    assertNull(inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB));

  }

  @Test
  public void testSerialization() {
    OrderedPartitionedKVEdgeConfig.Builder builder = OrderedPartitionedKVEdgeConfig
        .newBuilder("KEY", "VALUE", "PARTITIONER")
        .setCompression(true, "CustomCodec", null)
        .setKeySerializationClass("serClass1", "SomeComparator1", null)
        .setValueSerializationClass("serClass2", null);

    OrderedPartitionedKVEdgeConfig configuration = builder.build();

    OrderedPartitionedKVOutputConfig
        rebuiltOutput = new OrderedPartitionedKVOutputConfig();
    rebuiltOutput.fromUserPayload(configuration.getOutputPayload());
    OrderedGroupedKVInputConfig rebuiltInput = new OrderedGroupedKVInputConfig();
    rebuiltInput.fromUserPayload(configuration.getInputPayload());

    Configuration outputConf = rebuiltOutput.conf;
    Configuration inputConf = rebuiltInput.conf;

    assertEquals("KEY", outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, ""));
    assertEquals("VALUE",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, ""));
    assertEquals("PARTITIONER", outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, ""));
    assertEquals("CustomCodec",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
    assertEquals(true,
        outputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS,
            false));
    assertNull(outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT));
    //verify comparator and serialization class
    assertEquals("SomeComparator1",
        outputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS));
    assertTrue(outputConf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY).trim().startsWith
        ("serClass2,serClass1"));


    //verify comparator and serialization class
    assertEquals("SomeComparator1", inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS));
    assertTrue(inputConf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY).trim().startsWith
        ("serClass2,serClass1"));

    assertEquals("KEY", inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, ""));
    assertEquals("VALUE",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, ""));
    assertEquals("CustomCodec",
        inputConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, ""));
    assertEquals(true,
        inputConf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS,
            false));
  }
}
