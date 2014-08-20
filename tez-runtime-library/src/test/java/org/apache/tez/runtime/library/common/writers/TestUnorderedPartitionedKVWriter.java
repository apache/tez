/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.runtime.library.common.writers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@RunWith(value = Parameterized.class)
public class TestUnorderedPartitionedKVWriter {

  private static final Log LOG = LogFactory.getLog(TestUnorderedPartitionedKVWriter.class);

  private static final String HOST_STRING = "localhost";
  private static final int SHUFFLE_PORT = 4000;

  private static String testTmpDir = System.getProperty("test.build.data", "/tmp");
  private static final Path TEST_ROOT_DIR = new Path(testTmpDir,
      TestUnorderedPartitionedKVWriter.class.getSimpleName());
  private static FileSystem localFs;

  private boolean shouldCompress;

  public TestUnorderedPartitionedKVWriter(boolean shouldCompress) {
    this.shouldCompress = shouldCompress;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { false }, { true } };
    return Arrays.asList(data);
  }

  @Before
  public void setup() throws IOException {
    LOG.info("Setup. Using test dir: " + TEST_ROOT_DIR);
    localFs = FileSystem.getLocal(new Configuration());
    localFs.delete(TEST_ROOT_DIR, true);
    localFs.mkdirs(TEST_ROOT_DIR);
  }

  @After
  public void cleanup() throws IOException {
    LOG.info("CleanUp");
    localFs.delete(TEST_ROOT_DIR, true);
  }

  @Test(timeout = 10000)
  public void testBufferSizing() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();
    String uniqueId = UUID.randomUUID().toString();
    OutputContext outputContext = createMockOutputContext(counters, appId, uniqueId);

    int maxSingleBufferSizeBytes = 2047;
    Configuration conf = createConfiguration(outputContext, IntWritable.class, LongWritable.class,
        false, maxSingleBufferSizeBytes);

    int numOutputs = 10;

    UnorderedPartitionedKVWriter kvWriter = null;

    kvWriter = new UnorderedPartitionedKVWriterForTest(outputContext, conf, numOutputs, 2048);
    assertEquals(2, kvWriter.numBuffers);
    assertEquals(1024, kvWriter.sizePerBuffer);
    assertEquals(1, kvWriter.numInitializedBuffers);

    kvWriter = new UnorderedPartitionedKVWriterForTest(outputContext, conf, numOutputs,
        maxSingleBufferSizeBytes * 3);
    assertEquals(3, kvWriter.numBuffers);
    assertEquals(maxSingleBufferSizeBytes - maxSingleBufferSizeBytes % 4, kvWriter.sizePerBuffer);
    assertEquals(1, kvWriter.numInitializedBuffers);

    kvWriter = new UnorderedPartitionedKVWriterForTest(outputContext, conf, numOutputs,
        maxSingleBufferSizeBytes * 2 + 1);
    assertEquals(3, kvWriter.numBuffers);
    assertEquals(1364, kvWriter.sizePerBuffer);
    assertEquals(1, kvWriter.numInitializedBuffers);

    kvWriter = new UnorderedPartitionedKVWriterForTest(outputContext, conf, numOutputs, 10240);
    assertEquals(6, kvWriter.numBuffers);
    assertEquals(1704, kvWriter.sizePerBuffer);
    assertEquals(1, kvWriter.numInitializedBuffers);
  }

  @Test(timeout = 10000)
  public void testNoSpill() throws IOException, InterruptedException {
    baseTest(10, 10, null, shouldCompress);
  }

  @Test(timeout = 10000)
  public void testSingleSpill() throws IOException, InterruptedException {
    baseTest(50, 10, null, shouldCompress);
  }

  @Test(timeout = 10000)
  public void testMultipleSpills() throws IOException, InterruptedException {
    baseTest(200, 10, null, shouldCompress);
  }

  @Test(timeout = 10000)
  public void testNoRecords() throws IOException, InterruptedException {
    baseTest(0, 10, null, shouldCompress);
  }

  @Test(timeout = 10000)
  public void testSkippedPartitions() throws IOException, InterruptedException {
    baseTest(200, 10, Sets.newHashSet(2, 5), shouldCompress);
  }

  @Test(timeout = 10000)
  public void testRandomText() throws IOException, InterruptedException {
    textTest(100, 10, 2048, 0, 0, 0);
  }

  @Test(timeout = 10000)
  public void testLargeKeys() throws IOException, InterruptedException {
    textTest(0, 10, 2048, 10, 0, 0);
  }

  @Test(timeout = 10000)
  public void testLargevalues() throws IOException, InterruptedException {
    textTest(0, 10, 2048, 0, 10, 0);
  }

  @Test(timeout = 10000)
  public void testLargeKvPairs() throws IOException, InterruptedException {
    textTest(0, 10, 2048, 0, 0, 10);
  }

  @Test(timeout = 10000)
  public void testTextMixedRecords() throws IOException, InterruptedException {
    textTest(100, 10, 2048, 10, 10, 10);
  }

  public void textTest(int numRegularRecords, int numPartitions, long availableMemory,
      int numLargeKeys, int numLargevalues, int numLargeKvPairs) throws IOException,
      InterruptedException {
    Partitioner partitioner = new HashPartitioner();
    ApplicationId appId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();
    String uniqueId = UUID.randomUUID().toString();
    OutputContext outputContext = createMockOutputContext(counters, appId, uniqueId);
    Random random = new Random();

    Configuration conf = createConfiguration(outputContext, Text.class, Text.class, shouldCompress,
        -1, HashPartitioner.class);
    CompressionCodec codec = null;
    if (shouldCompress) {
      codec = new DefaultCodec();
      ((Configurable) codec).setConf(conf);
    }

    int numRecordsWritten = 0;

    Map<Integer, Multimap<String, String>> expectedValues = new HashMap<Integer, Multimap<String, String>>();
    for (int i = 0; i < numPartitions; i++) {
      expectedValues.put(i, LinkedListMultimap.<String, String> create());
    }

    UnorderedPartitionedKVWriter kvWriter = new UnorderedPartitionedKVWriterForTest(outputContext,
        conf, numPartitions, availableMemory);

    int sizePerBuffer = kvWriter.sizePerBuffer;

    BitSet partitionsWithData = new BitSet(numPartitions);
    Text keyText = new Text();
    Text valText = new Text();
    for (int i = 0; i < numRegularRecords; i++) {
      String key = createRandomString(Math.abs(random.nextInt(10)));
      String val = createRandomString(Math.abs(random.nextInt(20)));
      keyText.set(key);
      valText.set(val);
      int partition = partitioner.getPartition(keyText, valText, numPartitions);
      partitionsWithData.set(partition);
      expectedValues.get(partition).put(key, val);
      kvWriter.write(keyText, valText);
      numRecordsWritten++;
    }

    // Write Large key records
    for (int i = 0; i < numLargeKeys; i++) {
      String key = createRandomString(sizePerBuffer + Math.abs(random.nextInt(100)));
      String val = createRandomString(Math.abs(random.nextInt(20)));
      keyText.set(key);
      valText.set(val);
      int partition = partitioner.getPartition(keyText, valText, numPartitions);
      partitionsWithData.set(partition);
      expectedValues.get(partition).put(key, val);
      kvWriter.write(keyText, valText);
      numRecordsWritten++;
    }

    // Write Large val records
    for (int i = 0; i < numLargevalues; i++) {
      String key = createRandomString(Math.abs(random.nextInt(10)));
      String val = createRandomString(sizePerBuffer + Math.abs(random.nextInt(100)));
      keyText.set(key);
      valText.set(val);
      int partition = partitioner.getPartition(keyText, valText, numPartitions);
      partitionsWithData.set(partition);
      expectedValues.get(partition).put(key, val);
      kvWriter.write(keyText, valText);
      numRecordsWritten++;
    }

    // Write records where key + val are large (but both can fit in the buffer individually)
    for (int i = 0; i < numLargeKvPairs; i++) {
      String key = createRandomString(sizePerBuffer / 2 + Math.abs(random.nextInt(100)));
      String val = createRandomString(sizePerBuffer / 2 + Math.abs(random.nextInt(100)));
      keyText.set(key);
      valText.set(val);
      int partition = partitioner.getPartition(keyText, valText, numPartitions);
      partitionsWithData.set(partition);
      expectedValues.get(partition).put(key, val);
      kvWriter.write(keyText, valText);
      numRecordsWritten++;
    }

    List<Event> events = kvWriter.close();
    verify(outputContext, never()).fatalError(any(Throwable.class), any(String.class));

    TezCounter outputLargeRecordsCounter = counters.findCounter(TaskCounter.OUTPUT_LARGE_RECORDS);
    assertEquals(numLargeKeys + numLargevalues + numLargeKvPairs,
        outputLargeRecordsCounter.getValue());

    // Validate the event
    assertEquals(1, events.size());
    assertTrue(events.get(0) instanceof CompositeDataMovementEvent);
    CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) events.get(0);
    assertEquals(0, cdme.getSourceIndexStart());
    assertEquals(numPartitions, cdme.getCount());
    DataMovementEventPayloadProto eventProto = DataMovementEventPayloadProto.parseFrom(
        ByteString.copyFrom(cdme
            .getUserPayload()));
    assertFalse(eventProto.hasData());
    BitSet emptyPartitionBits = null;
    if (partitionsWithData.cardinality() != numPartitions) {
      assertTrue(eventProto.hasEmptyPartitions());
      byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(eventProto
          .getEmptyPartitions());
      emptyPartitionBits = TezUtilsInternal.fromByteArray(emptyPartitions);
      assertEquals(numPartitions - partitionsWithData.cardinality(),
          emptyPartitionBits.cardinality());
    } else {
      assertFalse(eventProto.hasEmptyPartitions());
      emptyPartitionBits = new BitSet(numPartitions);
    }
    assertEquals(HOST_STRING, eventProto.getHost());
    assertEquals(SHUFFLE_PORT, eventProto.getPort());
    assertEquals(uniqueId, eventProto.getPathComponent());

    // Verify the data
    // Verify the actual data
    TezTaskOutput taskOutput = new TezTaskOutputFiles(conf, uniqueId);
    Path outputFilePath = null;
    Path spillFilePath = null;
    try {
      outputFilePath = taskOutput.getOutputFile();
    } catch (DiskErrorException e) {
      if (numRecordsWritten > 0) {
        fail();
      } else {
        // Record checking not required.
        return;
      }
    }
    try {
      spillFilePath = taskOutput.getOutputIndexFile();
    } catch (DiskErrorException e) {
      if (numRecordsWritten > 0) {
        fail();
      } else {
        // Record checking not required.
        return;
      }
    }

    // Special case for 0 records.
    TezSpillRecord spillRecord = new TezSpillRecord(spillFilePath, conf);
    DataInputBuffer keyBuffer = new DataInputBuffer();
    DataInputBuffer valBuffer = new DataInputBuffer();
    Text keyDeser = new Text();
    Text valDeser = new Text();
    for (int i = 0; i < numPartitions; i++) {
      if (emptyPartitionBits.get(i)) {
        continue;
      }
      TezIndexRecord indexRecord = spillRecord.getIndex(i);
      FSDataInputStream inStream = FileSystem.getLocal(conf).open(outputFilePath);
      inStream.seek(indexRecord.getStartOffset());
      IFile.Reader reader = new IFile.Reader(inStream, indexRecord.getPartLength(), codec, null,
          null, false, 0, -1);
      while (reader.nextRawKey(keyBuffer)) {
        reader.nextRawValue(valBuffer);
        keyDeser.readFields(keyBuffer);
        valDeser.readFields(valBuffer);
        int partition = partitioner.getPartition(keyDeser, valDeser, numPartitions);
        assertTrue(expectedValues.get(partition).remove(keyDeser.toString(), valDeser.toString()));
      }
      inStream.close();
    }
    for (int i = 0; i < numPartitions; i++) {
      assertEquals(0, expectedValues.get(i).size());
      expectedValues.remove(i);
    }
    assertEquals(0, expectedValues.size());
  }

  private void baseTest(int numRecords, int numPartitions, Set<Integer> skippedPartitions,
      boolean shouldCompress) throws IOException, InterruptedException {
    PartitionerForTest partitioner = new PartitionerForTest();
    ApplicationId appId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();
    String uniqueId = UUID.randomUUID().toString();
    OutputContext outputContext = createMockOutputContext(counters, appId, uniqueId);

    Configuration conf = createConfiguration(outputContext, IntWritable.class, LongWritable.class,
        shouldCompress, -1);
    CompressionCodec codec = null;
    if (shouldCompress) {
      codec = new DefaultCodec();
      ((Configurable) codec).setConf(conf);
    }

    int numOutputs = numPartitions;
    long availableMemory = 2048;
    int numRecordsWritten = 0;

    Map<Integer, Multimap<Integer, Long>> expectedValues = new HashMap<Integer, Multimap<Integer, Long>>();
    for (int i = 0; i < numOutputs; i++) {
      expectedValues.put(i, LinkedListMultimap.<Integer, Long> create());
    }

    UnorderedPartitionedKVWriter kvWriter = new UnorderedPartitionedKVWriterForTest(outputContext,
        conf, numOutputs, availableMemory);

    int sizePerBuffer = kvWriter.sizePerBuffer;
    int sizePerRecord = 4 + 8; // IntW + LongW
    int sizePerRecordWithOverhead = sizePerRecord + 12; // Record + META_OVERHEAD

    IntWritable intWritable = new IntWritable();
    LongWritable longWritable = new LongWritable();
    for (int i = 0; i < numRecords; i++) {
      intWritable.set(i);
      longWritable.set(i);
      int partition = partitioner.getPartition(intWritable, longWritable, numOutputs);
      if (skippedPartitions != null && skippedPartitions.contains(partition)) {
        continue;
      }
      expectedValues.get(partition).put(intWritable.get(), longWritable.get());
      kvWriter.write(intWritable, longWritable);
      numRecordsWritten++;
    }
    List<Event> events = kvWriter.close();

    int recordsPerBuffer = sizePerBuffer / sizePerRecordWithOverhead;
    int numExpectedSpills = numRecordsWritten / recordsPerBuffer;

    verify(outputContext, never()).fatalError(any(Throwable.class), any(String.class));

    // Verify the status of the buffers
    if (numExpectedSpills == 0) {
      assertEquals(1, kvWriter.numInitializedBuffers);
    } else {
      assertTrue(kvWriter.numInitializedBuffers > 1);
    }
    assertNull(kvWriter.currentBuffer);
    assertEquals(0, kvWriter.availableBuffers.size());

    // Verify the counters
    TezCounter outputRecordBytesCounter = counters.findCounter(TaskCounter.OUTPUT_BYTES);
    TezCounter outputRecordsCounter = counters.findCounter(TaskCounter.OUTPUT_RECORDS);
    TezCounter outputBytesWithOverheadCounter = counters
        .findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    TezCounter fileOutputBytesCounter = counters.findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    TezCounter spilledRecordsCounter = counters.findCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter additionalSpillBytesWritternCounter = counters
        .findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    TezCounter additionalSpillBytesReadCounter = counters
        .findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);
    TezCounter numAdditionalSpillsCounter = counters
        .findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
    assertEquals(numRecordsWritten * sizePerRecord, outputRecordBytesCounter.getValue());
    assertEquals(numRecordsWritten, outputRecordsCounter.getValue());
    assertEquals(numRecordsWritten * sizePerRecordWithOverhead,
        outputBytesWithOverheadCounter.getValue());
    long fileOutputBytes = fileOutputBytesCounter.getValue();
    if (numRecordsWritten > 0) {
      assertTrue(fileOutputBytes > 0);
      if (!shouldCompress) {
        assertTrue(fileOutputBytes > outputRecordBytesCounter.getValue());
      }
    } else {
      assertEquals(0, fileOutputBytes);
    }
    assertEquals(recordsPerBuffer * numExpectedSpills, spilledRecordsCounter.getValue());
    long additionalSpillBytesWritten = additionalSpillBytesWritternCounter.getValue();
    long additionalSpillBytesRead = additionalSpillBytesReadCounter.getValue();
    if (numExpectedSpills == 0) {
      assertEquals(0, additionalSpillBytesWritten);
      assertEquals(0, additionalSpillBytesRead);
    } else {
      assertTrue(additionalSpillBytesWritten > 0);
      assertTrue(additionalSpillBytesRead > 0);
      if (!shouldCompress) {
        assertTrue(additionalSpillBytesWritten > (recordsPerBuffer * numExpectedSpills * sizePerRecord));
        assertTrue(additionalSpillBytesRead > (recordsPerBuffer * numExpectedSpills * sizePerRecord));
      }
    }
    assertTrue(additionalSpillBytesWritten == additionalSpillBytesRead);
    assertEquals(numExpectedSpills, numAdditionalSpillsCounter.getValue());

    BitSet emptyPartitionBits = null;
    // Verify the event returned
    assertEquals(1, events.size());
    assertTrue(events.get(0) instanceof CompositeDataMovementEvent);
    CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) events.get(0);
    assertEquals(0, cdme.getSourceIndexStart());
    assertEquals(numOutputs, cdme.getCount());
    DataMovementEventPayloadProto eventProto =
        DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(
            cdme.getUserPayload()));
    assertFalse(eventProto.hasData());
    if (skippedPartitions == null && numRecordsWritten > 0) {
      assertFalse(eventProto.hasEmptyPartitions());
      emptyPartitionBits = new BitSet(numPartitions);
    } else {
      assertTrue(eventProto.hasEmptyPartitions());
      byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(eventProto
          .getEmptyPartitions());
      emptyPartitionBits = TezUtilsInternal.fromByteArray(emptyPartitions);
      if (numRecordsWritten == 0) {
        assertEquals(numPartitions, emptyPartitionBits.cardinality());
      } else {
        for (Integer e : skippedPartitions) {
          assertTrue(emptyPartitionBits.get(e));
        }
        assertEquals(skippedPartitions.size(), emptyPartitionBits.cardinality());
      }
    }
    if (emptyPartitionBits.cardinality() != numPartitions) {
      assertEquals(HOST_STRING, eventProto.getHost());
      assertEquals(SHUFFLE_PORT, eventProto.getPort());
      assertEquals(uniqueId, eventProto.getPathComponent());
    } else {
      assertFalse(eventProto.hasHost());
      assertFalse(eventProto.hasPort());
      assertFalse(eventProto.hasPathComponent());
    }

    // Verify the actual data
    TezTaskOutput taskOutput = new TezTaskOutputFiles(conf, uniqueId);
    Path outputFilePath = null;
    Path spillFilePath = null;
    try {
      outputFilePath = taskOutput.getOutputFile();
    } catch (DiskErrorException e) {
      if (numRecordsWritten > 0) {
        fail();
      } else {
        // Record checking not required.
        return;
      }
    }
    try {
      spillFilePath = taskOutput.getOutputIndexFile();
    } catch (DiskErrorException e) {
      if (numRecordsWritten > 0) {
        fail();
      } else {
        // Record checking not required.
        return;
      }
    }

    // Special case for 0 records.
    TezSpillRecord spillRecord = new TezSpillRecord(spillFilePath, conf);
    DataInputBuffer keyBuffer = new DataInputBuffer();
    DataInputBuffer valBuffer = new DataInputBuffer();
    IntWritable keyDeser = new IntWritable();
    LongWritable valDeser = new LongWritable();
    for (int i = 0; i < numOutputs; i++) {
      if (skippedPartitions != null && skippedPartitions.contains(i)) {
        continue;
      }
      TezIndexRecord indexRecord = spillRecord.getIndex(i);
      FSDataInputStream inStream = FileSystem.getLocal(conf).open(outputFilePath);
      inStream.seek(indexRecord.getStartOffset());
      IFile.Reader reader = new IFile.Reader(inStream, indexRecord.getPartLength(), codec, null,
          null, false, 0, -1);
      while (reader.nextRawKey(keyBuffer)) {
        reader.nextRawValue(valBuffer);
        keyDeser.readFields(keyBuffer);
        valDeser.readFields(valBuffer);
        int partition = partitioner.getPartition(keyDeser, valDeser, numOutputs);
        assertTrue(expectedValues.get(partition).remove(keyDeser.get(), valDeser.get()));
      }
      inStream.close();
    }
    for (int i = 0; i < numOutputs; i++) {
      assertEquals(0, expectedValues.get(i).size());
      expectedValues.remove(i);
    }
    assertEquals(0, expectedValues.size());
  }

  private static String createRandomString(int size) {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < size; i++) {
      int r = Math.abs(random.nextInt()) % 26;
      sb.append((char) (65 + r));
    }
    return sb.toString();
  }

  private OutputContext createMockOutputContext(TezCounters counters, ApplicationId appId,
      String uniqueId) {
    OutputContext outputContext = mock(OutputContext.class);
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
    ByteBuffer portBuffer = ByteBuffer.allocate(4);
    portBuffer.mark();
    portBuffer.putInt(SHUFFLE_PORT);
    portBuffer.reset();
    doReturn(portBuffer).when(outputContext).getServiceProviderMetaData(
        eq(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID));
    Path outDirBase = new Path(TEST_ROOT_DIR, "outDir_" + uniqueId);
    String[] outDirs = new String[] { outDirBase.toString() };
    doReturn(outDirs).when(outputContext).getWorkDirs();
    return outputContext;
  }

  private Configuration createConfiguration(OutputContext outputContext,
      Class<? extends Writable> keyClass, Class<? extends Writable> valClass,
      boolean shouldCompress, int maxSingleBufferSizeBytes) {
    return createConfiguration(outputContext, keyClass, valClass, shouldCompress,
        maxSingleBufferSizeBytes, PartitionerForTest.class);
  }

  private Configuration createConfiguration(OutputContext outputContext,
      Class<? extends Writable> keyClass, Class<? extends Writable> valClass,
      boolean shouldCompress, int maxSingleBufferSizeBytes,
      Class<? extends Partitioner> partitionerClass) {
    Configuration conf = new Configuration(false);
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, outputContext.getWorkDirs());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, keyClass.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, valClass.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, partitionerClass.getName());
    if (maxSingleBufferSizeBytes >= 0) {
      conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES,
          maxSingleBufferSizeBytes);
    }
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, shouldCompress);
    if (shouldCompress) {
      conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC,
          DefaultCodec.class.getName());
    }
    return conf;
  }

  public static class PartitionerForTest implements Partitioner {
    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
      if (key instanceof IntWritable) {
        return ((IntWritable) key).get() % numPartitions;
      } else {
        throw new UnsupportedOperationException(
            "Test partitioner expected to be called with IntWritable only");
      }
    }
  }

  private static class UnorderedPartitionedKVWriterForTest extends UnorderedPartitionedKVWriter {

    public UnorderedPartitionedKVWriterForTest(OutputContext outputContext, Configuration conf,
        int numOutputs, long availableMemoryBytes) throws IOException {
      super(outputContext, conf, numOutputs, availableMemoryBytes);
    }

    @Override
    String getHost() {
      return HOST_STRING;
    }
  }
}
