/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.library.common.sort.impl.dflt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.tez.util.StringInterner;

import com.google.protobuf.ByteString;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestDefaultSorter {

  private static final int PORT = 80;
  private static final String UniqueID = "UUID";

  private static FileSystem localFs = null;
  private static Path workingDir = null;

  private Configuration conf;
  private LocalDirAllocator dirAllocator;

  @BeforeEach
  public void setup() throws IOException {
    conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, SorterImpl.LEGACY.name()); // DefaultSorter
    conf.set("fs.defaultFS", "file:///");
    localFs = FileSystem.getLocal(conf);

    workingDir = new Path(
        new Path(System.getProperty("test.build.data", "/tmp")),
        TestDefaultSorter.class.getName())
        .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    String localDirs = workingDir.toString();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
        HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
    dirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
  }

  @AfterAll
  public static void cleanup() throws IOException {
    localFs.delete(workingDir, true);
  }

  @AfterEach
  public void reset() throws IOException {
    cleanup();
    localFs.mkdirs(workingDir);
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSortSpillPercent() throws Exception {
    OutputContext context = createTezOutputContext();

    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT, 0.0f);
    try {
      new DefaultSorter(context, conf, 10, (10 * 1024 * 1024l));
      fail();
    } catch(IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT));
    }

    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT, 1.1f);
    try {
      new DefaultSorter(context, conf, 10, (10 * 1024 * 1024l));
      fail();
    } catch(IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT));
    }
  }


  @Test
  @Disabled
  /**
   * Disabling this, as this would need 2047 MB sort mb for testing.
   * Set DefaultSorter.MAX_IO_SORT_MB = 20467 for running this.
   */
  public void testSortLimitsWithSmallRecord() throws IOException {
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, NullWritable.class.getName());
    OutputContext context = createTezOutputContext();

    doReturn(2800 * 1024 * 1024l).when(context).getTotalMemoryAvailableToTask();

    //Setting IO_SORT_MB to 2047 MB
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 2047);
    context.requestInitialMemory(
        ExternalSorter.getInitialMemoryRequirement(conf,
            context.getTotalMemoryAvailableToTask()), new MemoryUpdateCallbackHandler());

    DefaultSorter sorter = new DefaultSorter(context, conf, 2, 2047 << 20);

    //Reset key/value in conf back to Text for other test cases
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());

    int i = 0;
    /**
     * If io.sort.mb is not capped to 1800, this would end up throwing
     * "java.lang.ArrayIndexOutOfBoundsException" after many spills.
     * Intentionally made it as infinite loop.
     */
    while (true) {
      //test for the avg record size 2 (in lower spectrum)
      Text key = new Text(i + "");
      sorter.write(key, NullWritable.get());
      i = (i + 1) % 10;
    }
  }

  @Test
  @Disabled
  /**
   * Disabling this, as this would need 2047 MB io.sort.mb for testing.
   * Provide > 2GB to JVM when running this test to avoid OOM in string generation.
   *
   * Set DefaultSorter.MAX_IO_SORT_MB = 2047 for running this.
   */
  public void testSortLimitsWithLargeRecords() throws IOException {
    OutputContext context = createTezOutputContext();

    doReturn(2800 * 1024 * 1024l).when(context).getTotalMemoryAvailableToTask();

    //Setting IO_SORT_MB to 2047 MB
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 2047);
    context.requestInitialMemory(
        ExternalSorter.getInitialMemoryRequirement(conf,
            context.getTotalMemoryAvailableToTask()), new MemoryUpdateCallbackHandler());

    DefaultSorter sorter = new DefaultSorter(context, conf, 2, 2047 << 20);

    int i = 0;
    /**
     * If io.sort.mb is not capped to 1800, this would end up throwing
     * "java.lang.ArrayIndexOutOfBoundsException" after many spills.
     * Intentionally made it as infinite loop.
     */
    while (true) {
      Text key = new Text(i + "");
      //Generate random size between 1 MB to 100 MB.
      int valSize = ThreadLocalRandom.current().nextInt(1 * 1024 * 1024, 100 * 1024 * 1024);
      String val = StringInterner.intern(StringUtils.repeat("v", valSize));
      sorter.write(key, new Text(val));
      i = (i + 1) % 10;
    }
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSortMBLimits() throws Exception {

    assertEquals(DefaultSorter.MAX_IO_SORT_MB, DefaultSorter.computeSortBufferSize(4096, ""),
        "Expected " + DefaultSorter.MAX_IO_SORT_MB);
    assertEquals(DefaultSorter.MAX_IO_SORT_MB, DefaultSorter.computeSortBufferSize(2047, ""),
        "Expected " + DefaultSorter.MAX_IO_SORT_MB);
    assertEquals(1024, DefaultSorter.computeSortBufferSize(1024, ""), "Expected 1024");

    try {
      DefaultSorter.computeSortBufferSize(0, "");
      fail("Should have thrown error for setting buffer size to 0");
    } catch (RuntimeException re) {
    }

    try {
      DefaultSorter.computeSortBufferSize(-100, "");
      fail("Should have thrown error for setting buffer size to negative value");
    } catch (RuntimeException re) {
    }
  }

  @Test
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  //Test TEZ-1977
  public void basicTest() throws IOException {
    OutputContext context = createTezOutputContext();

    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    try {
      //Setting IO_SORT_MB to greater than available mem limit (should throw exception)
      conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 300);
      context.requestInitialMemory(
          ExternalSorter.getInitialMemoryRequirement(conf,
              context.getTotalMemoryAvailableToTask()), new MemoryUpdateCallbackHandler());
      fail();
    } catch(IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB));
    }

    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 1);
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    SorterWrapper sorterWrapper = new SorterWrapper(context, conf, 5, handler.getMemoryAssigned());
    DefaultSorter sorter = sorterWrapper.getSorter();

    //Write 1000 keys each of size 1000, (> 1 spill should happen)
    try {
      Text[] keys = generateData(1000, 1000);
      Text[] values = generateData(1000, 1000);
      for (int i = 0; i < keys.length; i++) {
        sorterWrapper.writeKeyValue(keys[i], values[i]);
      }
      sorterWrapper.close();
      assertTrue(sorter.getNumSpills() > 2);
      verifyCounters(sorter, context);
    } catch(IOException ioe) {
      fail(ioe.getMessage());
    }

    verifyOutputPermissions(context.getUniqueIdentifier());
  }

  @Test
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testEmptyCaseFileLengths() throws IOException {
    testEmptyCaseFileLengthsHelper(50, new String[] {"a", "b"}, new String[] {"1", "2"});
    testEmptyCaseFileLengthsHelper(50, new String[] {"a", "a"}, new String[] {"1", "2"});
    testEmptyCaseFileLengthsHelper(50, new String[] {"aaa", "bbb", "aaa"}, new String[] {"1", "2", "3"});
    testEmptyCaseFileLengthsHelper(1, new String[] {"abcdefghij"}, new String[] {"1234567890"});
  }

  public void testEmptyCaseFileLengthsHelper(int numPartitions, String[] keys, String[] values)
      throws IOException {
    OutputContext context = createTezOutputContext();

    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 1);
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    String auxService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID, TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    SorterWrapper sorterWrapper = new SorterWrapper(context, conf, numPartitions, handler.getMemoryAssigned());
    DefaultSorter sorter = sorterWrapper.getSorter();
    assertEquals(keys.length, values.length, "Key and Values must have the same number of elements");
    BitSet keyRLEs = new BitSet(keys.length);
    for (int i = 0; i < keys.length; i++) {
      boolean isRLE = sorterWrapper.writeKeyValue(new Text(keys[i]), new Text(values[i]));
      keyRLEs.set(i, isRLE);
    }
    sorterWrapper.close();

    List<Event> events = new ArrayList<>();
    String pathComponent = (context.getUniqueIdentifier() + "_" + 0);
    ShuffleUtils.generateEventOnSpill(events, true, true, context, 0,
        sorter.indexCacheList.get(0), 0, true, pathComponent, sorter.getPartitionStats(),
        sorter.reportDetailedPartitionStats(), auxService, TezCommonUtils.newBestCompressionDeflater());

    CompositeDataMovementEvent compositeDataMovementEvent =
        (CompositeDataMovementEvent) events.get(1);
    ByteBuffer bb = compositeDataMovementEvent.getUserPayload();
    ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload =
        ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(bb));

    if (shufflePayload.hasEmptyPartitions()) {
      byte[] emptyPartitionsBytesString =
          TezCommonUtils.decompressByteStringToByteArray(
              shufflePayload.getEmptyPartitions());
      BitSet emptyPartitionBitSet = TezUtilsInternal.fromByteArray(emptyPartitionsBytesString);
      assertEquals(emptyPartitionBitSet.cardinality(), sorterWrapper.getEmptyPartitionsCount(),
          "Number of empty partitions did not match!");
    } else {
      assertEquals(0, sorterWrapper.getEmptyPartitionsCount());
    }
    // Each non-empty partition adds 4 bytes for header, 2 bytes for EOF_MARKER, 4 bytes for checksum
    int expectedFileOutLength = sorterWrapper.getNonEmptyPartitionsCount() * 10;
    for (int i = 0; i < keys.length; i++) {
      // Each Record adds 1 byte for key length, 1 byte Text overhead (length), key.length bytes for key
      expectedFileOutLength += keys[i].length() + 2;
      // Each Record adds 1 byte for value length, 1 byte Text overhead (length), value.length bytes for value
      expectedFileOutLength += values[i].length() + 2;
    }
    assertEquals(localFs.getFileStatus(sorter.getFinalOutputFile()).getLen(), expectedFileOutLength,
        "Unexpected Output File Size!");
    assertEquals(1, sorter.getNumSpills());
    verifyCounters(sorter, context);
  }

  @Test
  public void testWithEmptyData() throws IOException {
    OutputContext context = createTezOutputContext();

    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 1);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    DefaultSorter sorter = new DefaultSorter(context, conf, 1, handler.getMemoryAssigned());

    //no data written. Empty
    try {
      sorter.flush();
      sorter.close();
      assertTrue(sorter.isClosed());
      assertTrue(sorter.getFinalOutputFile().getParent().getName().equalsIgnoreCase(UniqueID));
      verifyCounters(sorter, context);
    } catch(Exception e) {
      fail();
    }
  }

  @Test
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testWithEmptyDataWithFinalMergeDisabled() throws IOException {
    OutputContext context = createTezOutputContext();

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 1);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    DefaultSorter sorter = new DefaultSorter(context, conf, 5, handler.getMemoryAssigned());

    //no data written. Empty
    try {
      sorter.flush();
      sorter.close();
      assertTrue(sorter.isClosed());
      assertTrue(sorter.getFinalOutputFile().getParent().getName().equalsIgnoreCase(UniqueID +
          "_0"));
      verifyCounters(sorter, context);
    } catch(Exception e) {
      fail();
    }
  }

  @Test
  public void testEmptyPartitions() throws Exception {
    testEmptyPartitionsHelper(2, false);
    testEmptyPartitionsHelper(2, true);
    testEmptyPartitionsHelper(0, true);
    testEmptyPartitionsHelper(0, true);
  }

  public void testEmptyPartitionsHelper(int numKeys, boolean sendEmptyPartitionDetails) throws IOException {
    OutputContext context = createTezOutputContext();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED, sendEmptyPartitionDetails);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 1);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    int partitions = 50;
    SorterWrapper sorterWrapper = new SorterWrapper(context, conf, partitions, handler.getMemoryAssigned());
    DefaultSorter sorter = sorterWrapper.getSorter();

    Text[] keys = generateData(numKeys, 1000000);
    Text[] values = generateData(numKeys, 1000000);
    for (int i = 0; i < keys.length; i++) {
      sorterWrapper.writeKeyValue(keys[i], values[i]);
    }
    sorterWrapper.close();
    if (numKeys == 0) {
      assertEquals(1, sorter.getNumSpills());
    } else {
      assertEquals(sorter.getNumSpills(), numKeys);
    }
    verifyCounters(sorter, context);
    verifyOutputPermissions(context.getUniqueIdentifier());
    if (sorter.indexCacheList.size() != 0) {
      for (int i = 0; i < sorter.getNumSpills(); i++) {
        TezSpillRecord record = sorter.indexCacheList.get(i);
        for (int j = 0; j < partitions; j++) {
          TezIndexRecord tezIndexRecord = record.getIndex(j);
          if (tezIndexRecord.hasData()) {
            continue;
          }
          if (sendEmptyPartitionDetails) {
            assertEquals(0, tezIndexRecord.getRawLength(), "Unexpected raw length for " + i + "th partition");
          } else {
            assertEquals(6, tezIndexRecord.getRawLength(), "");
          }
        }
      }
    }
    Path indexFile = sorter.getFinalIndexFile();
    TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
    for (int i = 0; i < partitions; i++) {
      TezIndexRecord tezIndexRecord = spillRecord.getIndex(i);
      if (tezIndexRecord.hasData()) {
        continue;
      }
      if (sendEmptyPartitionDetails) {
        assertEquals(0, tezIndexRecord.getRawLength(), "Unexpected raw length for " + i + "th partition");
      } else {
        assertEquals(6, tezIndexRecord.getRawLength(), "Unexpected raw length for " + i + "th partition");
      }
    }
  }

  void testPartitionStats(boolean withStats) throws IOException {
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS, withStats);
    OutputContext context = createTezOutputContext();

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 4);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    SorterWrapper sorterWrapper = new SorterWrapper(context, conf, 1, handler.getMemoryAssigned());
    DefaultSorter sorter = sorterWrapper.getSorter();

    Text[] keys = generateData(1000, 10);
    Text[] values = generateData(1000, 10);
    for (int i = 0; i < keys.length; i++) {
      sorterWrapper.writeKeyValue(keys[i], values[i]);
    }
    sorterWrapper.close();
    assertEquals(1, sorter.getNumSpills());
    verifyCounters(sorter, context);

    if (withStats) {
      assertNotNull(sorter.getPartitionStats());
    } else {
      assertNull(sorter.getPartitionStats());
    }
  }

  @Test
  @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
  public void testWithPartitionStats() throws IOException {
    testPartitionStats(true);
  }

  @Test
  @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
  public void testWithoutPartitionStats() throws IOException {
    testPartitionStats(false);
  }

  @Test
  @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
  @SuppressWarnings("unchecked")
  public void testWithSingleSpillWithFinalMergeDisabled() throws IOException {
    OutputContext context = createTezOutputContext();

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 4);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);

    SorterWrapper sorterWrapper = new SorterWrapper(context, conf, 1, handler.getMemoryAssigned());
    DefaultSorter sorter = sorterWrapper.getSorter();

    Text[] keys = generateData(1000, 10);
    Text[] values = generateData(1000, 10);
    for (int i = 0; i < keys.length; i++) {
      sorterWrapper.writeKeyValue(keys[i], values[i]);
    }
    sorterWrapper.close();
    assertEquals(1, sorter.getNumSpills());
    ArgumentCaptor<List> eventCaptor = ArgumentCaptor.forClass(List.class);
    verify(context, times(1)).sendEvents(eventCaptor.capture());
    List<Event> events = eventCaptor.getValue();
    for(Event event : events) {
      if (event instanceof CompositeDataMovementEvent) {
        CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) event;
        ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload = ShuffleUserPayloads
            .DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(cdme.getUserPayload()));
        assertTrue(shufflePayload.getPathComponent().equalsIgnoreCase(UniqueID + "_0"));
        verifyOutputPermissions(shufflePayload.getPathComponent());
      }
    }

    verifyCounters(sorter, context);
  }

  @Test
  @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
  @SuppressWarnings("unchecked")
  public void testWithMultipleSpillsWithFinalMergeDisabled() throws IOException {
    OutputContext context = createTezOutputContext();

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 4);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES, 1);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    SorterWrapper sorterWrapper = new SorterWrapper(context, conf, 1, handler.getMemoryAssigned());
    DefaultSorter sorter = sorterWrapper.getSorter();

    Text[] keys = generateData(10000, 1000);
    Text[] values = generateData(10000, 1000);
    for (int i = 0; i < keys.length; i++) {
      sorterWrapper.writeKeyValue(keys[i], values[i]);
    }
    sorterWrapper.close();

    int spillCount = sorter.getNumSpills();
    ArgumentCaptor<List> eventCaptor = ArgumentCaptor.forClass(List.class);
    verify(context, times(1)).sendEvents(eventCaptor.capture());
    List<Event> events = eventCaptor.getValue();
    int spillIndex = 0;
    for(Event event : events) {
      if (event instanceof CompositeDataMovementEvent) {
        CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) event;
        ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload = ShuffleUserPayloads
            .DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(cdme.getUserPayload()));
        assertTrue(shufflePayload.getPathComponent().equalsIgnoreCase(UniqueID + "_" + spillIndex));
        verifyOutputPermissions(shufflePayload.getPathComponent());
        spillIndex++;
      }
    }
    assertEquals(spillIndex, spillCount);
    verifyCounters(sorter, context);
  }

  private void verifyOutputPermissions(String spillId) throws IOException {
    String subpath = Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR + "/" + spillId
        + "/" + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING;
    Path outputPath = dirAllocator.getLocalPathToRead(subpath, conf);
    Path indexPath = dirAllocator.getLocalPathToRead(subpath + Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING, conf);
    assertEquals((short)0640, localFs.getFileStatus(outputPath).getPermission().toShort(), "Incorrect output permissions");
    assertEquals((short)0640, localFs.getFileStatus(indexPath).getPermission().toShort(), "Incorrect index permissions");
  }

  private void verifyCounters(DefaultSorter sorter, OutputContext context) {
    TezCounter numShuffleChunks = context.getCounters().findCounter(TaskCounter.SHUFFLE_CHUNK_COUNT);
    TezCounter additionalSpills = context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
    TezCounter additionalSpillBytesWritten = context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    TezCounter additionalSpillBytesRead = context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);

    if (sorter.isFinalMergeEnabled()) {
      assertEquals(additionalSpills.getValue(), (sorter.getNumSpills() - 1));
      //Number of files served by shuffle-handler
      assertEquals(1, numShuffleChunks.getValue());
      if (sorter.getNumSpills() > 1) {
        assertTrue(additionalSpillBytesRead.getValue() > 0);
        assertTrue(additionalSpillBytesWritten.getValue() > 0);
      }
    } else {
      assertEquals(0, additionalSpills.getValue());
      //Number of files served by shuffle-handler
      assertEquals(sorter.getNumSpills(), numShuffleChunks.getValue());
      assertEquals(0, additionalSpillBytesRead.getValue());
      assertEquals(0, additionalSpillBytesWritten.getValue());
    }

    TezCounter finalOutputBytes = context.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    assertTrue(finalOutputBytes.getValue() >= 0);

    TezCounter outputBytesWithOverheadCounter = context.getCounters().findCounter
        (TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    assertTrue(outputBytesWithOverheadCounter.getValue() >= 0);
    verify(context, atLeastOnce()).notifyProgress();
  }

  private static class SorterWrapper {

    private final DefaultSorter sorter;
    private final Partitioner partitioner;
    private final BitSet nonEmptyPartitions;
    private final Object[] lastKeys;
    private final int numPartitions;


    public SorterWrapper(OutputContext context, Configuration conf, int numPartitions, long memoryAssigned) throws IOException {
      sorter = new DefaultSorter(context, conf, numPartitions, memoryAssigned);
      partitioner = TezRuntimeUtils.instantiatePartitioner(conf);
      nonEmptyPartitions = new BitSet(numPartitions);
      lastKeys = new Object[numPartitions];
      this.numPartitions = numPartitions;
    }

    public boolean writeKeyValue(Object key, Object value) throws IOException {
      int partition = partitioner.getPartition(key, value, this.numPartitions);
      nonEmptyPartitions.set(partition);
      sorter.write(key, value);

      boolean isRLE = key.equals(lastKeys[partition]);
      lastKeys[partition] = key;
      return isRLE;
    }

    public int getNonEmptyPartitionsCount() {
      return nonEmptyPartitions.cardinality();
    }

    public int getEmptyPartitionsCount() {
      return numPartitions - nonEmptyPartitions.cardinality();
    }

    public void close () throws IOException {
      sorter.flush();
      sorter.close();
    }

    public DefaultSorter getSorter() {
      return sorter;
    }
  }

  private static Text[] generateData(int numKeys, int keyLen) {
    Text[] ret = new Text[numKeys];
    for (int i = 0; i < numKeys; i++) {
      ret[i] = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
    }
    return ret;
  }

  private OutputContext createTezOutputContext() throws IOException {
    String[] workingDirs = { workingDir.toString() };
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    DataOutputBuffer serviceProviderMetaData = new DataOutputBuffer();
    serviceProviderMetaData.writeInt(PORT);

    TezCounters counters = new TezCounters();

    OutputContext context = mock(OutputContext.class);
    ExecutionContext execContext = new ExecutionContextImpl("localhost");
    doReturn(mock(OutputStatisticsReporter.class)).when(context).getStatisticsReporter();
    doReturn(execContext).when(context).getExecutionContext();
    doReturn(counters).when(context).getCounters();
    doReturn(workingDirs).when(context).getWorkDirs();
    doReturn(payLoad).when(context).getUserPayload();
    doReturn(5 * 1024 * 1024l).when(context).getTotalMemoryAvailableToTask();
    doReturn(UniqueID).when(context).getUniqueIdentifier();
    doReturn("v1").when(context).getDestinationVertexName();
    doReturn(ByteBuffer.wrap(serviceProviderMetaData.getData())).when(context)
        .getServiceProviderMetaData
            (conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
                TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT));
    doAnswer(new Answer() {
      @Override public Object answer(InvocationOnMock invocation) throws Throwable {
        long requestedSize = (Long) invocation.getArguments()[0];
        MemoryUpdateCallbackHandler callback = (MemoryUpdateCallbackHandler) invocation
            .getArguments()[1];
        callback.memoryAssigned(requestedSize);
        return null;
      }
    }).when(context).requestInitialMemory(anyLong(), any());
    return context;
  }
}
