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

public class TestOrderedPartitionedKVEdgeConfiguration {

  @Test
  public void testIncompleteParameters() {
    OrderedPartitionedKVEdgeConfiguration.Builder builder =
        OrderedPartitionedKVEdgeConfiguration.newBuilder("KEY", "VALUE");
    try {
      builder.build();
      fail("Should have failed since the partitioner has not been specified");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Output must be configured - partitioner"));
    }
  }

  @Test
  public void testNullParams() {
    try {
      OrderedPartitionedKVEdgeConfiguration.newBuilder(null, "VALUE");
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      OrderedPartitionedKVEdgeConfiguration.newBuilder("KEY", null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }

    try {
      OrderedPartitionedKVEdgeConfiguration.newBuilder("KEY", "VALUE").configureOutput(null, null);
      fail("Expecting a null parameter list to fail");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testDefaultConfigsUsed() {
    OrderedPartitionedKVEdgeConfiguration.Builder builder = OrderedPartitionedKVEdgeConfiguration
        .newBuilder("KEY", "VALUE")
        .configureOutput("PARTITIONER", null).done();

    OrderedPartitionedKVEdgeConfiguration configuration = builder.build();

    byte[] outputBytes = configuration.getOutputPayload();
    byte[] inputBytes = configuration.getInputPayload();

    OnFileSortedOutputConfiguration rebuiltOutput = new OnFileSortedOutputConfiguration();
    rebuiltOutput.fromByteArray(outputBytes);
    ShuffledMergedInputConfiguration rebuiltInput = new ShuffledMergedInputConfiguration();
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
    OrderedPartitionedKVEdgeConfiguration.Builder builder = OrderedPartitionedKVEdgeConfiguration
        .newBuilder("KEY", "VALUE")
        .configureOutput("PARTITIONER", null).done();

    OrderedPartitionedKVEdgeConfiguration configuration = builder.build();

    byte[] outputBytes = configuration.getOutputPayload();
    byte[] inputBytes = configuration.getInputPayload();

    OnFileSortedOutputConfiguration rebuiltOutput = new OnFileSortedOutputConfiguration();
    rebuiltOutput.fromByteArray(outputBytes);
    ShuffledMergedInputConfiguration rebuiltInput = new ShuffledMergedInputConfiguration();
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
    fromConf.setInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, 3);
    fromConf.setFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 0.11f);
    fromConf.setInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_MB, 123);
    fromConf.set("io.shouldExist", "io");
    Map<String, String> additionalConfs = new HashMap<String, String>();
    additionalConfs.put("test.key.2", "key2");
    additionalConfs.put(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, "1111");
    additionalConfs.put(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, "0.22f");
    additionalConfs.put(TezJobConfig.TEZ_RUNTIME_INTERNAL_SORTER_CLASS, "CustomSorter");
    additionalConfs.put("file.shouldExist", "file");

    OrderedPartitionedKVEdgeConfiguration.Builder builder = OrderedPartitionedKVEdgeConfiguration
        .newBuilder("KEY", "VALUE")
        .configureOutput("PARTITIONER", null).done()
        .setAdditionalConfiguration("fs.shouldExist", "fs")
        .setAdditionalConfiguration("test.key.1", "key1")
        .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, "2222")
        .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, "0.33f")
        .setAdditionalConfiguration(TezJobConfig.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES, "3333")
        .setAdditionalConfiguration(additionalConfs)
        .setFromConfiguration(fromConf);

    OrderedPartitionedKVEdgeConfiguration configuration = builder.build();

    byte[] outputBytes = configuration.getOutputPayload();
    byte[] inputBytes = configuration.getInputPayload();

    OnFileSortedOutputConfiguration rebuiltOutput = new OnFileSortedOutputConfiguration();
    rebuiltOutput.fromByteArray(outputBytes);
    ShuffledMergedInputConfiguration rebuiltInput = new ShuffledMergedInputConfiguration();
    rebuiltInput.fromByteArray(inputBytes);

    Configuration outputConf = rebuiltOutput.conf;
    Configuration inputConf = rebuiltInput.conf;

    assertEquals(3, outputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, 0));
    assertEquals(1111, outputConf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(2222, outputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT));
    assertEquals(123, outputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_MB, 0));
    assertEquals("CustomSorter", outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERNAL_SORTER_CLASS));
    assertEquals(3333,
        outputConf.getInt(TezJobConfig.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES, 0));
    assertEquals("io", outputConf.get("io.shouldExist"));
    assertEquals("file", outputConf.get("file.shouldExist"));
    assertEquals("fs", outputConf.get("fs.shouldExist"));


    assertEquals(3, inputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, 0));
    assertEquals(1111, inputConf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 0));
    assertEquals(2222, inputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE, 0));
    assertEquals(0.11f,
        inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f,
        inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.33f, inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f),
        0.001f);
    assertNull(inputConf.get(TezJobConfig.TEZ_RUNTIME_IO_SORT_MB));
    assertNull(inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERNAL_SORTER_CLASS));
    assertNull(inputConf.get(TezJobConfig.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES));
    assertEquals("io", inputConf.get("io.shouldExist"));
    assertEquals("file", inputConf.get("file.shouldExist"));
    assertEquals("fs", inputConf.get("fs.shouldExist"));

  }

  @Test
  public void testSetters() {
    OrderedPartitionedKVEdgeConfiguration.Builder builder = OrderedPartitionedKVEdgeConfiguration
        .newBuilder("KEY", "VALUE")
        .setKeyComparatorClass("KEY_COMPARATOR")
        .configureOutput("PARTITIONER", null).setSortBufferSize(1111).setSorterNumThreads(2).done()
        .configureInput().setMaxSingleMemorySegmentFraction(0.11f).setMergeFraction(0.22f)
        .setPostMergeBufferFraction(0.33f).setShuffleBufferFraction(0.44f).done()
        .enableCompression("CustomCodec");

    OrderedPartitionedKVEdgeConfiguration configuration = builder.build();

    byte[] outputBytes = configuration.getOutputPayload();
    byte[] inputBytes = configuration.getInputPayload();

    OnFileSortedOutputConfiguration rebuiltOutput = new OnFileSortedOutputConfiguration();
    rebuiltOutput.fromByteArray(outputBytes);
    ShuffledMergedInputConfiguration rebuiltInput = new ShuffledMergedInputConfiguration();
    rebuiltInput.fromByteArray(inputBytes);

    Configuration outputConf = rebuiltOutput.conf;
    Configuration inputConf = rebuiltInput.conf;

    assertEquals("KEY", outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS, ""));
    assertEquals("VALUE",
        outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS, ""));
    assertEquals("PARTITIONER", outputConf.get(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS, ""));
    assertEquals(1111, outputConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_MB, 0));
    assertEquals("CustomCodec",
        outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC, ""));
    assertEquals(true,
        outputConf.getBoolean(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS,
            false));
    assertEquals("KEY_COMPARATOR",
        outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_COMPARATOR_CLASS));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_IS_COMPRESSED));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT));
    assertNull(outputConf.get(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT));


    assertEquals("KEY", inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS, ""));
    assertEquals("VALUE",
        inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS, ""));
    assertEquals("CustomCodec",
        inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC, ""));
    assertEquals(true,
        inputConf.getBoolean(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_IS_COMPRESSED,
            false));
    assertEquals(0.11f,
        inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f), 0.001f);
    assertEquals(0.22f, inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.0f),
        0.001f);
    assertEquals(0.33f, inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT, 0.0f),
        0.001f);
    assertEquals(0.44f,
        inputConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 0.00f), 0.001f);
    assertNull(inputConf.get(TezJobConfig.TEZ_RUNTIME_IO_SORT_MB));
    assertNull(inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC));
    assertNull(inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS));
    assertNull(inputConf.get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_COMPARATOR_CLASS));
  }

}
