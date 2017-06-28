/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.sort.impl;

import com.google.common.collect.Maps;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.testutils.RandomTextGenerator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class TestPipelinedSorter {
  private static FileSystem localFs = null;
  private static Path workDir = null;
  private OutputContext outputContext;

  private int numOutputs;
  private long initialAvailableMem;

  //TODO: Need to make it nested structure so that multiple partition cases can be validated
  private static TreeMap<String, String> sortedDataMap = Maps.newTreeMap();

  static {
    Configuration conf = getConf();
    try {
      localFs = FileSystem.getLocal(conf);
      workDir = new Path(
          new Path(System.getProperty("test.build.data", "/tmp")),
          TestPipelinedSorter.class.getName())
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    localFs.delete(workDir, true);
  }

  @Before
  public void setup() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();
    String uniqueId = UUID.randomUUID().toString();
    String auxiliaryService = getConf().get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    this.outputContext = createMockOutputContext(counters, appId, uniqueId, auxiliaryService);
  }

  public static Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    //To enable PipelinedSorter
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, SorterImpl.PIPELINED.name());

    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);

    //Setup localdirs
    if (workDir != null) {
      String localDirs = workDir.toString();
      conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
    }
    return conf;
  }

  @After
  public void reset() throws IOException {
    cleanup();
    localFs.mkdirs(workDir);
  }

  @Test
  public void basicTest() throws IOException {
    //TODO: need to support multiple partition testing later

    //# partition, # of keys, size per key, InitialMem, blockSize
    Configuration conf = getConf();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);
    basicTest(1, 100000, 100, (10 * 1024l * 1024l), 3 << 20);

  }

  @Test
  public void testWithoutPartitionStats() throws IOException {
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS, false);
    //# partition, # of keys, size per key, InitialMem, blockSize
    basicTest(1, 0, 0, (10 * 1024l * 1024l), 3 << 20);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS, true);
  }

  @Test
  public void testWithEmptyData() throws IOException {
    Configuration conf = getConf();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);
    //# partition, # of keys, size per key, InitialMem, blockSize
    basicTest(1, 0, 0, (10 * 1024l * 1024l), 3 << 20);
  }

  @Test
  public void testEmptyDataWithPipelinedShuffle() throws IOException {
    this.numOutputs = 1;
    this.initialAvailableMem = 1 *1024 * 1024;
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    writeData(sorter, 0, 1<<20);

    // final merge is disabled. Final output file would not be populated in this case.
    assertTrue(sorter.finalOutputFile == null);
    TezCounter numShuffleChunks = outputContext.getCounters().findCounter(TaskCounter.SHUFFLE_CHUNK_COUNT);
//    assertTrue(sorter.getNumSpills() == numShuffleChunks.getValue());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);

  }

  @Test
  public void testEmptyPartitionsTwoSpillsNoEmptyEvents() throws Exception {
    testEmptyPartitionsHelper(2, false);
  }

  @Test
  public void testEmptyPartitionsTwoSpillsWithEmptyEvents() throws Exception {
    testEmptyPartitionsHelper(2, true);
  }

  @Test
  public void testEmptyPartitionsNoSpillsNoEmptyEvents() throws Exception {
    testEmptyPartitionsHelper(0, false);
  }

  @Test
  public void testEmptyPartitionsNoSpillsWithEmptyEvents() throws Exception {
    testEmptyPartitionsHelper(0, true);
  }

  public void testEmptyPartitionsHelper(int numKeys, boolean sendEmptyPartitionDetails) throws IOException, InterruptedException {
    int partitions = 50;
    this.numOutputs = partitions;
    this.initialAvailableMem = 1 *1024 * 1024;
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED, sendEmptyPartitionDetails);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, partitions,
        initialAvailableMem);

    writeData(sorter, numKeys, 1000000);
    if (numKeys == 0) {
      assertTrue(sorter.getNumSpills() == 1);
    } else {
      assertTrue(sorter.getNumSpills() == numKeys + 1);
    }
    verifyCounters(sorter, outputContext);
    Path indexFile = sorter.getFinalIndexFile();
    TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
    for (int i = 0; i < partitions; i++) {
      TezIndexRecord tezIndexRecord = spillRecord.getIndex(i);
      if (tezIndexRecord.hasData()) {
        continue;
      }
      if (sendEmptyPartitionDetails) {
        Assert.assertEquals("Unexpected raw length for " + i + "th partition", 0, tezIndexRecord.getRawLength());
      } else {
        Assert.assertEquals("Unexpected raw length for " + i + "th partition", 6, tezIndexRecord.getRawLength());
      }
    }
  }

  @Test
  public void basicTestWithSmallBlockSize() throws IOException {
    //3 MB key & 3 MB value, whereas block size is just 3 MB
    basicTest(1, 5, (3 << 20), (10 * 1024l * 1024l), 3 << 20);
  }

  @Test
  public void testWithLargeKeyValue() throws IOException {
    //15 MB key & 15 MB value, 48 MB sort buffer.  block size is 48MB (or 1 block)
    //meta would be 16 MB
    basicTest(1, 5, (15 << 20), (48 * 1024l * 1024l), 48 << 20);
  }

  @Test
  public void testKVExceedsBuffer() throws IOException {
    // a single block of 1mb, 2KV pair, key 1mb, value 1mb
    basicTest(1, 2, (1 << 20), (1 * 1024l * 1024l), 1<<20);
  }

  @Test
  public void testKVExceedsBuffer2() throws IOException {
    // a list of 4 blocks each 256kb, 2KV pair, key 1mb, value 1mb
    basicTest(1, 2, (1 << 20), (1 * 1024l * 1024l), 256<<20);
  }

  @Test
  public void testExceedsKVWithMultiplePartitions() throws IOException {
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    this.numOutputs = 5;
    this.initialAvailableMem = 1 * 1024 * 1024;
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    writeData(sorter, 100, 1<<20);
    verifyCounters(sorter, outputContext);
  }

  @Test
  public void testExceedsKVWithPipelinedShuffle() throws IOException {
    this.numOutputs = 1;
    this.initialAvailableMem = 1 *1024 * 1024;
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    writeData(sorter, 5, 1<<20);

    // final merge is disabled. Final output file would not be populated in this case.
    assertTrue(sorter.finalOutputFile == null);
    TezCounter numShuffleChunks = outputContext.getCounters().findCounter(TaskCounter.SHUFFLE_CHUNK_COUNT);
    assertTrue(sorter.getNumSpills() == numShuffleChunks.getValue());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
  }

  @Test
  public void test_TEZ_2602_50mb() throws IOException {
    this.numOutputs = 1;
    this.initialAvailableMem = 1 *1024 * 1024;
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    Text value = new Text("1");
    long size = 50 * 1024 * 1024;
    while(size > 0) {
      Text key = RandomTextGenerator.generateSentence();
      sorter.write(key, value);
      size -= key.getLength();
    }

    sorter.flush();
    sorter.close();
  }

  //@Test
  public void testLargeDataWithMixedKV() throws IOException {
    this.numOutputs = 1;
    this.initialAvailableMem = 48 *1024 * 1024;
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    //write 10 MB KV
    Text key = new Text(RandomStringUtils.randomAlphanumeric(10 << 20));
    Text value = new Text(RandomStringUtils.randomAlphanumeric(10 << 20));
    sorter.write(key, value);

    //write 24 MB KV. This should cause single record spill
    key = new Text(RandomStringUtils.randomAlphanumeric(24 << 20));
    value = new Text(RandomStringUtils.randomAlphanumeric(24 << 20));
    sorter.write(key, value);

    //write 10 MB KV
    key = new Text(RandomStringUtils.randomAlphanumeric(10 << 20));
    value = new Text(RandomStringUtils.randomAlphanumeric(10 << 20));
    sorter.write(key, value);

    sorter.flush();
    sorter.close();
  }


  @Test
  // first write a KV which dosnt fit into span, this will spill to disk
  // next write smaller keys, which will update the hint
  public void testWithVariableKVLength1() throws IOException {
    int numkeys[] = {2, 2};
    int keylens[] = {32 << 20, 7 << 20};
    basicTest2(1, numkeys, keylens, 64 << 20, 32 << 20);
  }

  @Test
  // first write a kv pair which fits into buffer,
  // next try to write a kv pair which doesnt fit into remaining buffer
  public void testWithVariableKVLength() throws IOException {
    //2 KVpairs of 2X2mb, 2 KV of 2X7mb
    int numkeys[] = {2, 2};
    int keylens[] = {2 << 20, 7<<20};
    basicTest2(1, numkeys, keylens, 64 << 20, 32 << 20);
  }

  @Test
  // first write KV which fits into span
  // then write KV which doesnot fit in buffer. this will be spilled to disk
  // all keys should be merged properly
  public void testWithVariableKVLength2() throws IOException {
    // 20 KVpairs of 2X10kb, 10 KV of 2X200kb, 20KV of 2X10kb
    int numkeys[] = {20, 10, 20};
    int keylens[] = {10<<10, 200<<10, 10<<10};
    basicTest2(1, numkeys, keylens, (10 * 1024l * 1024l), 2);
  }

  @Test
  public void testWithCustomComparator() throws IOException {
    //Test with custom comparator
    Configuration conf = getConf();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
        CustomComparator.class.getName());
    basicTest(1, 100000, 100, (10 * 1024l * 1024l), 3 << 20);
  }

  @Test
  public void testWithPipelinedShuffle() throws IOException {
    this.numOutputs = 1;
    this.initialAvailableMem = 5 *1024 * 1024;
    Configuration conf = getConf();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    //Write 100 keys each of size 10
    writeData(sorter, 10000, 100);

    //final merge is disabled. Final output file would not be populated in this case.
    assertTrue(sorter.finalOutputFile == null);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    verify(outputContext, times(1)).sendEvents(anyListOf(Event.class));
  }

  @Test
  public void testCountersWithMultiplePartitions() throws IOException {
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    this.numOutputs = 5;
    this.initialAvailableMem = 5 * 1024 * 1024;
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    writeData(sorter, 10000, 100);
    verifyCounters(sorter, outputContext);
  }

  @Test
  public void testMultipleSpills() throws IOException {
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    this.numOutputs = 5;
    this.initialAvailableMem = 5 * 1024 * 1024;
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 3);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    writeData(sorter, 25000, 1000);
    assertFalse("Expecting needsRLE to be false", sorter.needsRLE());
    verifyCounters(sorter, outputContext);
  }

  @Test
  public void testMultipleSpills_WithRLE() throws IOException {
    Configuration conf = getConf();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    this.numOutputs = 5;
    this.initialAvailableMem = 5 * 1024 * 1024;
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 3);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    writeSimilarKeys(sorter, 25000, 1000, true);
    assertTrue("Expecting needsRLE to be true", sorter.needsRLE());
    verifyCounters(sorter, outputContext);
  }

  public void basicTest2(int partitions, int[] numkeys, int[] keysize,
      long initialAvailableMem, int  blockSize) throws IOException {
    this.numOutputs = partitions; // single output
    Configuration conf = getConf();
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 100);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);
    writeData2(sorter, numkeys, keysize);
    verifyCounters(sorter, outputContext);
  }

  private void writeData2(ExternalSorter sorter,
      int[] numKeys, int[] keyLen) throws IOException {
    sortedDataMap.clear();
    int counter = 0;
    for (int numkey : numKeys) {
      int curKeyLen = keyLen[counter];
      for (int i = 0; i < numkey; i++) {
        Text key = new Text(RandomStringUtils.randomAlphanumeric(curKeyLen));
        Text value = new Text(RandomStringUtils.randomAlphanumeric(curKeyLen));
        sorter.write(key, value);
      }
      counter++;
    }
    sorter.flush();
    sorter.close();
  }

  public void basicTest(int partitions, int numKeys, int keySize,
      long initialAvailableMem, int minBlockSize) throws IOException {
    this.numOutputs = partitions; // single output
    Configuration conf = getConf();
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, minBlockSize >> 20);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);

    writeData(sorter, numKeys, keySize);

    //partition stats;
    ReportPartitionStats partitionStats =
        ReportPartitionStats.fromString(conf.get(
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));
    if (partitionStats.isEnabled()) {
      assertTrue(sorter.getPartitionStats() != null);
    }

    verifyCounters(sorter, outputContext);
    Path outputFile = sorter.finalOutputFile;
    FileSystem fs = outputFile.getFileSystem(conf);
    TezCounter finalOutputBytes =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    if (finalOutputBytes.getValue() > 0) {
      IFile.Reader reader = new IFile.Reader(fs, outputFile, null, null, null, false, -1, 4096);
      verifyData(reader);
      reader.close();
    }
    //Verify dataset
    verify(outputContext, atLeastOnce()).notifyProgress();
  }

  private void verifyCounters(PipelinedSorter sorter, OutputContext context) {
    TezCounter numShuffleChunks = context.getCounters().findCounter(TaskCounter.SHUFFLE_CHUNK_COUNT);
    TezCounter additionalSpills =
        context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
    TezCounter additionalSpillBytesWritten =
        context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    TezCounter additionalSpillBytesRead =
        context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);

    if (sorter.isFinalMergeEnabled()) {
      assertTrue(additionalSpills.getValue() == (sorter.getNumSpills() - 1));
      //Number of files served by shuffle-handler
      assertTrue(1 == numShuffleChunks.getValue());
      if (sorter.getNumSpills() > 1) {
        assertTrue(additionalSpillBytesRead.getValue() > 0);
        assertTrue(additionalSpillBytesWritten.getValue() > 0);
      }
    } else {
      assertTrue(0 == additionalSpills.getValue());
      //Number of files served by shuffle-handler
      assertTrue(sorter.getNumSpills() == numShuffleChunks.getValue());
      assertTrue(additionalSpillBytesRead.getValue() == 0);
      assertTrue(additionalSpillBytesWritten.getValue() == 0);
    }

    TezCounter finalOutputBytes =
        context.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    assertTrue(finalOutputBytes.getValue() >= 0);

    TezCounter outputBytesWithOverheadCounter = context.getCounters().findCounter
        (TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    assertTrue(outputBytesWithOverheadCounter.getValue() >= 0);
  }


  @Test
  //Intentionally not having timeout
  //Its not possible to allocate > 2 GB in test environment.  Carry out basic checks here.
  public void memTest() throws IOException {
    //Verify if > 2 GB can be set via config
    Configuration conf = getConf();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 3076);
    long size = ExternalSorter.getInitialMemoryRequirement(conf, 4096 * 1024 * 1024l);
    Assert.assertTrue(size == (3076l << 20));

    //Verify number of block buffers allocated
    this.initialAvailableMem = 10 * 1024 * 1024;
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 1);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);
    Assert.assertTrue(sorter.maxNumberOfBlocks == 10);

    //10 MB available, request for 3 MB chunk. Last 1 MB gets added to previous chunk.
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 3);
    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);
    Assert.assertTrue(sorter.maxNumberOfBlocks == 3);

    //10 MB available, request for 10 MB min chunk.  Would get 1 block.
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 10);
    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem);
    Assert.assertTrue(sorter.maxNumberOfBlocks == 1);

    //Verify block sizes (10 MB min chunk size), but available mem is zero.
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 10);
    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs, initialAvailableMem);
    Assert.assertTrue(sorter.maxNumberOfBlocks == 1);
    int blockSize = sorter.computeBlockSize(0, (10 << 20));
    //available is zero. Can't allocate any more buffer.
    Assert.assertTrue(blockSize == 0);

    //300 MB available. Request for 200 MB min block size. It would allocate a block with 200 MB,
    // but last 100 would get clubbed. Hence, it would return 300 MB block.
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 200);
    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs, (300 << 20));
    Assert.assertTrue(sorter.maxNumberOfBlocks == 1);
    blockSize = sorter.computeBlockSize((300 << 20), (300 << 20));
    Assert.assertTrue(blockSize == (300 << 20));

    //300 MB available. Request for 3500 MB min block size. throw exception
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 3500);
    try {
      sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
          (300 << 20));
    } catch(IllegalArgumentException iae ) {
      assertTrue(iae.getMessage().contains("positive value between 0 and 2047"));
    }

    //64 MB available. Request for 32 MB min block size.
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, false);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 32);
    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs, (64 << 20));
    Assert.assertTrue(sorter.maxNumberOfBlocks == 2);
    blockSize = sorter.computeBlockSize((64 << 20), (64 << 20));
    Assert.assertTrue(blockSize == (32 << 20));

    blockSize = sorter.computeBlockSize((32 << 20), (64 << 20));
    Assert.assertTrue(blockSize == (32 << 20));

    blockSize = sorter.computeBlockSize((48 << 20), (64 << 20));
    Assert.assertTrue(blockSize == (48 << 20));

    //64 MB available. Request for 8 MB min block size.
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 8);
    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs, (64 << 20));
    Assert.assertTrue(sorter.maxNumberOfBlocks == 8);
    blockSize = sorter.computeBlockSize((64 << 20), (64 << 20));
    //Should return 16 instead of 8 which is min block size.
    Assert.assertTrue(blockSize == (8 << 20));
  }

  @Test
  //Intentionally not having timeout
  public void test_without_lazyMemAllocation() throws IOException {
    this.numOutputs = 10;
    Configuration conf = getConf();

    //128 MB. Pre-allocate. Request for default block size. Get 1 buffer
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 128);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB_DEFAULT);
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, false);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf,
        numOutputs, (128l << 20));
    assertTrue("Expected 1 sort buffers. current len=" + sorter.buffers.size(),
        sorter.buffers.size() == 1);

    //128 MB. Pre-allocate. Get 2 buffer
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 128);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 62);
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, false);
    sorter = new PipelinedSorter(this.outputContext, conf,
        numOutputs, (128l << 20));
    assertTrue("Expected 2 sort buffers. current len=" + sorter.buffers.size(),
        sorter.buffers.size() == 2);

    //48 MB. Pre-allocate. But request for lesser block size (62). Get 2 buffer
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 48);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 62);
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, false);
    sorter = new PipelinedSorter(this.outputContext, conf,
        numOutputs, (48l << 20));
    assertTrue("Expected 1 sort buffers. current len=" + sorter.buffers.size(),
        sorter.buffers.size() == 1);
  }

  @Test
  //Intentionally not having timeout
  public void test_with_lazyMemAllocation() throws IOException {
    this.numOutputs = 10;
    Configuration conf = getConf();

    //128 MB. Do not pre-allocate.
    // Get 32 MB buffer first and the another buffer with 96 on filling up
    // the 32 MB buffer.
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 128);
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, true);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf,
        numOutputs, (128l << 20));
    assertTrue("Expected 1 sort buffers. current len=" + sorter.buffers.size(),
        sorter.buffers.size() == 1);
    assertTrue(sorter.buffers.get(0).capacity() == 32 * 1024 * 1024 - 64);
    writeData(sorter, 100, 1024*1024, false); //100 1 MB KV. Will spill

    //Now it should have created 2 buffers, 32 & 96 MB buffers.
    assertTrue(sorter.buffers.size() == 2);
    assertTrue(sorter.buffers.get(0).capacity() == 32 * 1024 * 1024 - 64);
    assertTrue(sorter.buffers.get(1).capacity() == 96 * 1024 * 1024 + 64);
    closeSorter(sorter);
    verifyCounters(sorter, outputContext);

    //TODO: Not sure if this would fail in build machines due to mem
    //300 MB. Do not pre-allocate.
    // Get 1 buffer with 62 MB. But grow to 2 buffers as data is written
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 300);
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, true);
    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs, (300l << 20));
    assertTrue(sorter.buffers.size() == 1);
    assertTrue(sorter.buffers.get(0).capacity() == 32 * 1024 * 1024 - 64);

    writeData(sorter, 50, 1024*1024, false); //50 1 MB KV to allocate 2nd buf
    assertTrue(sorter.buffers.size() == 2);
    assertTrue(sorter.buffers.get(0).capacity() == 32 * 1024 * 1024 - 64);
    assertTrue(sorter.buffers.get(1).capacity() == 268 * 1024 * 1024 + 64);

    //48 MB. Do not pre-allocate.
    // Get 32 MB buffer first invariably and proceed with the rest.
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 48);
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, true);
    sorter = new PipelinedSorter(this.outputContext, conf,
        numOutputs, (48l << 20));
    assertTrue("Expected 1 sort buffers. current len=" + sorter.buffers.size(),
        sorter.buffers.size() == 1);
    assertTrue(sorter.buffers.get(0).capacity() == 32 * 1024 * 1024 - 64);
    writeData(sorter, 20, 1024*1024, false); //100 1 MB KV. Will spill

    //Now it should have created 2 buffers, 32 & 96 MB buffers.
    assertTrue(sorter.buffers.size() == 2);
    assertTrue(sorter.buffers.get(0).capacity() == 32 * 1024 * 1024 - 64);
    assertTrue(sorter.buffers.get(1).capacity() == 16 * 1024 * 1024 + 64);
    closeSorter(sorter);
  }

  @Test
  //Intentionally not having timeout
  public void testLazyAllocateMem() throws IOException {
    this.numOutputs = 10;
    Configuration conf = getConf();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 128);
    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, false);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, 4500);
    try {
      PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf,
          numOutputs, (4500l << 20));
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains(TezRuntimeConfiguration
          .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB));
      assertTrue(iae.getMessage().contains("value between 0 and 2047"));
    }

    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, -1);
    try {
      PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf,
          numOutputs, (4500l << 20));
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains(TezRuntimeConfiguration
          .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB));
      assertTrue(iae.getMessage().contains("value between 0 and 2047"));
    }

    conf.setBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY, true);
    conf.setInt(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, -1);
    try {
      PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf,
          numOutputs, (4500l << 20));
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains(TezRuntimeConfiguration
          .TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB));
      assertTrue(iae.getMessage().contains("value between 0 and 2047"));
    }

  }

  @Test
  //Intentionally not having timeout
  public void testWithLargeKeyValueWithMinBlockSize() throws IOException {
    //2 MB key & 2 MB value, 48 MB sort buffer.  block size is 16MB
    basicTest(1, 5, (2 << 20), (48 * 1024l * 1024l), 16 << 20);
  }

  private void writeData(ExternalSorter sorter, int numKeys, int keyLen) throws IOException {
    writeData(sorter, numKeys, keyLen, true);
  }

  // duplicate some of the keys
  private void writeSimilarKeys(ExternalSorter sorter, int numKeys, int keyLen,
      boolean autoClose) throws IOException {
    sortedDataMap.clear();
    String keyStr = RandomStringUtils.randomAlphanumeric(keyLen);
    for (int i = 0; i < numKeys; i++) {
      if (i % 4 == 0) {
        keyStr = RandomStringUtils.randomAlphanumeric(keyLen);
      }
      Text key = new Text(keyStr);
      Text value = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
      sorter.write(key, value);
      sortedDataMap.put(key.toString(), value.toString()); //for verifying data later
    }
    if (autoClose) {
      closeSorter(sorter);
    }
  }

  private void writeData(ExternalSorter sorter, int numKeys, int keyLen,
      boolean autoClose) throws IOException {
    sortedDataMap.clear();
    for (int i = 0; i < numKeys; i++) {
      Text key = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
      Text value = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
      sorter.write(key, value);
      sortedDataMap.put(key.toString(), value.toString()); //for verifying data later
    }
    if (autoClose) {
      closeSorter(sorter);
    }
  }

  private void closeSorter(ExternalSorter sorter) throws IOException {
    if (sorter != null) {
      sorter.flush();
      sorter.close();
    }
  }

  private void verifyData(IFile.Reader reader)
      throws IOException {
    Text readKey = new Text();
    Text readValue = new Text();
    DataInputBuffer keyIn = new DataInputBuffer();
    DataInputBuffer valIn = new DataInputBuffer();
    Configuration conf = getConf();
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    Deserializer<Text> keyDeserializer = serializationFactory.getDeserializer(Text.class);
    Deserializer<Text> valDeserializer = serializationFactory.getDeserializer(Text.class);
    keyDeserializer.open(keyIn);
    valDeserializer.open(valIn);

    int numRecordsRead = 0;

    for (Map.Entry<String, String> entry : sortedDataMap.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      if (reader.nextRawKey(keyIn)) {
        reader.nextRawValue(valIn);
        readKey = keyDeserializer.deserialize(readKey);
        readValue = valDeserializer.deserialize(readValue);
        Assert.assertTrue(key.equalsIgnoreCase(readKey.toString()));
        Assert.assertTrue(val.equalsIgnoreCase(readValue.toString()));
        numRecordsRead++;
      }
    }
    Assert.assertTrue(numRecordsRead == sortedDataMap.size());
  }

  private static OutputContext createMockOutputContext(TezCounters counters, ApplicationId appId,
      String uniqueId, String auxiliaryService) throws IOException {
    OutputContext outputContext = mock(OutputContext.class);

    ExecutionContext execContext = new ExecutionContextImpl("localhost");

    DataOutputBuffer serviceProviderMetaData = new DataOutputBuffer();
    serviceProviderMetaData.writeInt(80);
    doReturn(ByteBuffer.wrap(serviceProviderMetaData.getData())).when(outputContext)
        .getServiceProviderMetaData(auxiliaryService);

    doReturn(execContext).when(outputContext).getExecutionContext();
    doReturn(mock(OutputStatisticsReporter.class)).when(outputContext).getStatisticsReporter();
    doReturn(counters).when(outputContext).getCounters();
    doReturn(appId).when(outputContext).getApplicationId();
    doReturn(1).when(outputContext).getDAGAttemptNumber();
    doReturn("dagName").when(outputContext).getDAGName();
    doReturn("destinationVertexName").when(outputContext).getDestinationVertexName();
    doReturn(1).when(outputContext).getOutputIndex();
    doReturn(1).when(outputContext).getTaskAttemptNumber();
    doReturn(1).when(outputContext).getTaskIndex();
    doReturn(1).when(outputContext).getTaskVertexIndex();
    doReturn("vertexName").when(outputContext).getTaskVertexName();
    doReturn(uniqueId).when(outputContext).getUniqueIdentifier();
    Path outDirBase = new Path(workDir, "outDir_" + uniqueId);
    String[] outDirs = new String[] { outDirBase.toString() };
    doReturn(outDirs).when(outputContext).getWorkDirs();
    return outputContext;
  }

  /**
   * E.g Hive uses TezBytesComparator which internally makes use of WritableComparator's comparison.
   * Any length mismatches are handled there.
   *
   * However, custom comparators can handle this differently and might throw
   * IndexOutOfBoundsException in case of invalid lengths.
   *
   * This comparator (similar to comparator in BinInterSedes of pig) would thrown exception when
   * wrong lengths are mentioned.
   */
  public static class CustomComparator extends WritableComparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      //wrapping is done so that it would throw exceptions on wrong lengths
      ByteBuffer bb1 = ByteBuffer.wrap(b1, s1, l1);
      ByteBuffer bb2 = ByteBuffer.wrap(b2, s2, l2);

      return bb1.compareTo(bb2);
    }

  }
}
