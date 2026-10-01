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
package org.apache.tez.runtime.library.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;

import com.google.protobuf.ByteString;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@SuppressWarnings({ "rawtypes", "unchecked", "checkstyle:HiddenField" })
public class TestOnFileSortedOutput {
  private static final Random rnd = new Random();
  private static final String UniqueID = "UUID";
  private static final String HOST = "localhost";
  private static final int PORT = 80;

  private Configuration conf;
  private FileSystem fs;
  private Path workingDir;
  //no of outputs
  private int partitions;
  //For sorter (pipelined / Default)
  private SorterImpl sorterImpl;
  private int sorterThreads;

  final AtomicLong outputSize = new AtomicLong();
  final AtomicLong numRecords = new AtomicLong();

  private KeyValuesWriter writer;
  private OrderedPartitionedKVOutput sortedOutput;
  private boolean sendEmptyPartitionViaEvent;
  //Partition index for which data should not be written to.
  private int emptyPartitionIdx;
  private ReportPartitionStats reportPartitionStats;

  public void setupInit(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                        int emptyPartitionIdx, ReportPartitionStats reportPartitionStats) throws Exception {
    this.sendEmptyPartitionViaEvent = sendEmptyPartitionViaEvent;
    this.emptyPartitionIdx = emptyPartitionIdx;
    this.sorterImpl = sorterImpl;
    this.sorterThreads = sorterThreads;
    this.reportPartitionStats = reportPartitionStats;
    conf = new Configuration();

    workingDir = new Path(".", this.getClass().getName());
    String localDirs = workingDir.toString();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
    fs = FileSystem.getLocal(conf);

    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, sorterImpl.name());
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS, sorterThreads);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);

    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
        HashPartitioner.class.getName());

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        sendEmptyPartitionViaEvent);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        reportPartitionStats.getType());
    outputSize.set(0);
    numRecords.set(0);
    fs.mkdirs(workingDir);
    this.partitions = Math.max(1, rnd.nextInt(10));
  }

  @AfterEach
  public void cleanup() throws IOException {
    fs.delete(workingDir, true);
  }

  @SuppressWarnings("deprecation")
  public static Stream<Arguments> getParameters() {
    return Stream.of(
        //empty_partition_via_events_enabled, noOfSortThreads, partitionToBeEmpty, reportPartitionStats
        Arguments.of(false, SorterImpl.LEGACY, 1, -1, ReportPartitionStats.ENABLED),
        Arguments.of(false, SorterImpl.LEGACY, 1, 0, ReportPartitionStats.ENABLED),
        Arguments.of(true, SorterImpl.LEGACY, 1, -1, ReportPartitionStats.ENABLED),
        Arguments.of(true, SorterImpl.LEGACY, 1, 0, ReportPartitionStats.ENABLED),
        Arguments.of(true, SorterImpl.LEGACY, 1, 0, ReportPartitionStats.PRECISE),

        //Pipelined sorter
        Arguments.of(false, SorterImpl.PIPELINED, 2, -1, ReportPartitionStats.ENABLED),
        Arguments.of(false, SorterImpl.PIPELINED, 2, 0, ReportPartitionStats.ENABLED),
        Arguments.of(true, SorterImpl.PIPELINED, 2, -1, ReportPartitionStats.ENABLED),
        Arguments.of(true, SorterImpl.PIPELINED, 2, 0, ReportPartitionStats.ENABLED),
        Arguments.of(true, SorterImpl.PIPELINED, 2, 0, ReportPartitionStats.PRECISE));
  }

  private void startSortedOutput(int partitions) throws Exception {
    OutputContext context = createTezOutputContext();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 4);
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    doReturn(payLoad).when(context).getUserPayload();
    sortedOutput = new OrderedPartitionedKVOutput(context, partitions);
    sortedOutput.initialize();
    sortedOutput.start();
    writer = sortedOutput.getWriter();
  }

  private void _testPipelinedShuffle(String sorterName) throws Exception {
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 3);

    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, sorterName);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, true);
    OutputContext context = createTezOutputContext();
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    doReturn(payLoad).when(context).getUserPayload();
    sortedOutput = new OrderedPartitionedKVOutput(context, partitions);

    sortedOutput.initialize();
    sortedOutput.start();

    assertFalse(sortedOutput.finalMergeEnabled);
    assertTrue(sortedOutput.pipelinedShuffle);
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testPipelinedShuffle(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                                   int emptyPartitionId, ReportPartitionStats reportPartitionStats) throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    _testPipelinedShuffle(SorterImpl.PIPELINED.name());
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testPipelinedShuffleWithFinalMerge(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl,
                                                 int sorterThreads, int emptyPartitionId,
                                                 ReportPartitionStats reportPartitionStats) throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 3);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, SorterImpl.PIPELINED.name());

    //wrong setting for final merge enable in output
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, true);
    OutputContext context = createTezOutputContext();
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    doReturn(payLoad).when(context).getUserPayload();
    sortedOutput = new OrderedPartitionedKVOutput(context, partitions);

    sortedOutput.initialize();
    sortedOutput.start();
    assertFalse(sortedOutput.finalMergeEnabled); //should be disabled as pipelining is on
    assertTrue(sortedOutput.pipelinedShuffle);
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  public void testPipelinedSettingsWithDefaultSorter(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl,
                                                     int sorterThreads, int emptyPartitionId,
                                                     ReportPartitionStats reportPartitionStats) throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 3);
    //negative. with default sorter
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, SorterImpl.LEGACY.name());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, true);

    OutputContext context = createTezOutputContext();
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    doReturn(payLoad).when(context).getUserPayload();
    sortedOutput = new OrderedPartitionedKVOutput(context, partitions);

    sortedOutput.initialize();
    try {
      sortedOutput.start();
      fail("Should have thrown illegal argument exception as pipelining is enabled with "
          + "DefaultSorter");
    } catch(IllegalArgumentException ie) {
      assertTrue(ie.getMessage().contains("works with PipelinedSorter"));
    }

  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSortBufferSize(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                                 int emptyPartitionId, ReportPartitionStats reportPartitionStats) throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    OutputContext context = createTezOutputContext();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 2048);
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    doReturn(payLoad).when(context).getUserPayload();
    sortedOutput = new OrderedPartitionedKVOutput(context, partitions);
    try {
      //Memory limit checks are done in sorter impls. For e.g, defaultsorter does not support > 2GB
      sortedOutput.initialize();
      DefaultSorter sorter = new DefaultSorter(context, conf, 100, 3500 * 1024 * 1024L);
      fail();
    } catch(IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB));
    }
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 0);
    payLoad = TezUtils.createUserPayloadFromConf(conf);
    doReturn(payLoad).when(context).getUserPayload();
    sortedOutput = new OrderedPartitionedKVOutput(context, partitions);
    try {
      sortedOutput.initialize();
      fail();
    } catch(IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB));
    }
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void baseTest(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                       int emptyPartitionId, ReportPartitionStats reportPartitionStats) throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    startSortedOutput(partitions);

    //Write random set of keys
    long recordsWritten = numRecords.get();
    for (int i = 0; i < Math.max(1, rnd.nextInt(50)); i++) {
      Text key = new Text(new BigInteger(256, rnd).toString());
      LinkedList values = new LinkedList();
      for (int j = 0; j < Math.max(2, rnd.nextInt(10)); j++) {
        recordsWritten++;
        values.add(new Text(new BigInteger(256, rnd).toString()));
      }
      writer.write(key, values);
    }

    List<Event> eventList = sortedOutput.close();
    assertTrue(eventList != null && eventList.size() == 2);
    assertEquals(recordsWritten, numRecords.get());
    ShuffleUserPayloads.DataMovementEventPayloadProto
        payload = ShuffleUserPayloads.DataMovementEventPayloadProto
        .parseFrom(
            ByteString.copyFrom(((CompositeDataMovementEvent) eventList.get(1)).getUserPayload()));

    ShuffleUserPayloads.VertexManagerEventPayloadProto
        vmPayload = ShuffleUserPayloads.VertexManagerEventPayloadProto
        .parseFrom(
            ByteString.copyFrom(((VertexManagerEvent) eventList.get(0)).getUserPayload()));

    if (reportPartitionStats.isPrecise()) {
      assertTrue(vmPayload.hasDetailedPartitionStats());
    } else {
      assertTrue(vmPayload.hasPartitionStats());
    }
    assertEquals(HOST, payload.getHost());
    assertEquals(PORT, payload.getPort());
    assertEquals(UniqueID, payload.getPathComponent());
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testWithSomeEmptyPartition(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                                         int emptyPartitionId, ReportPartitionStats reportPartitionStats)
      throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    //ensure atleast 2 partitions are available
    partitions = Math.max(2, partitions);
    startSortedOutput(partitions);

    //write random data
    for (int i = 0; i < 2 * partitions; i++) {
      Text key = new Text(new BigInteger(256, rnd).toString());
      Text value = new Text(new BigInteger(256, rnd).toString());
      //skip writing to certain partitions
      if (i % partitions != emptyPartitionIdx) {
        writer.write(key, value);
      }
    }

    List<Event> eventList = sortedOutput.close();
    assertTrue(eventList != null && eventList.size() == 2);

    ShuffleUserPayloads.DataMovementEventPayloadProto
        payload = ShuffleUserPayloads.DataMovementEventPayloadProto
        .parseFrom(ByteString.copyFrom(((CompositeDataMovementEvent) eventList.get(1)).getUserPayload()));

    assertEquals(HOST, payload.getHost());
    assertEquals(PORT, payload.getPort());
    assertEquals(UniqueID, payload.getPathComponent());
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testAllEmptyPartition(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                                    int emptyPartitionId, ReportPartitionStats reportPartitionStats) throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    startSortedOutput(partitions);

    //Close output without writing any data to it.
    List<Event> eventList = sortedOutput.close();
    assertTrue(eventList != null && eventList.size() == 2);

    ShuffleUserPayloads.DataMovementEventPayloadProto
        payload = ShuffleUserPayloads.DataMovementEventPayloadProto
        .parseFrom(ByteString.copyFrom(((CompositeDataMovementEvent) eventList.get(1)).getUserPayload()));
    if (sendEmptyPartitionViaEvent) {
      assertEquals("", payload.getHost());
      assertEquals(0, payload.getPort());
      assertEquals("", payload.getPathComponent());
    } else {
      assertEquals(HOST, payload.getHost());
      assertEquals(PORT, payload.getPort());
      assertEquals(UniqueID, payload.getPathComponent());
    }
  }

  private OutputContext createTezOutputContext() throws IOException {
    String[] workingDirs = { workingDir.toString() };
    Configuration localConf = new Configuration(false);
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    DataOutputBuffer serviceProviderMetaData = new DataOutputBuffer();
    serviceProviderMetaData.writeInt(PORT);

    TezCounters counters = new TezCounters();

    OutputStatisticsReporter reporter = mock(OutputStatisticsReporter.class);
    doAnswer(new Answer() {
      @Override public Object answer(InvocationOnMock invocation) throws Throwable {
        outputSize.set((Long) invocation.getArguments()[0]);
        return null;
      }
    }).when(reporter).reportDataSize(anyLong());
    doAnswer(new Answer() {
      @Override public Object answer(InvocationOnMock invocation) throws Throwable {
        numRecords.set((Long) invocation.getArguments()[0]);
        return null;
      }
    }).when(reporter).reportItemsProcessed(anyLong());


    OutputContext context = mock(OutputContext.class);
    doReturn(localConf).when(context).getContainerConfiguration();
    doReturn(counters).when(context).getCounters();
    doReturn(workingDirs).when(context).getWorkDirs();
    doReturn(payLoad).when(context).getUserPayload();
    doReturn(5 * 1024 * 1024L).when(context).getTotalMemoryAvailableToTask();
    doReturn(UniqueID).when(context).getUniqueIdentifier();
    doReturn("v0").when(context).getTaskVertexName();
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
    ExecutionContext ExecutionContext = mock(ExecutionContext.class);
    doReturn(HOST).when(ExecutionContext).getHostName();
    doReturn(reporter).when(context).getStatisticsReporter();
    doReturn(ExecutionContext).when(context).getExecutionContext();
    return context;
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInvalidSorter(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                                int emptyPartitionId, ReportPartitionStats reportPartitionStats) throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    try {
      _testPipelinedShuffle("Foo");
      fail("Expected start to fail due to invalid sorter");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @ParameterizedTest(name = "test[{0}, {1}, {2}, {3}, {4}]")
  @MethodSource("getParameters")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testLowerCaseNamedSorter(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl, int sorterThreads,
                                       int emptyPartitionId, ReportPartitionStats reportPartitionStats)
      throws Exception {
    setupInit(sendEmptyPartitionViaEvent, sorterImpl, sorterThreads, emptyPartitionId, reportPartitionStats);
    _testPipelinedShuffle("Pipelined");
  }


}
