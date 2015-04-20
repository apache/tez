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

package org.apache.tez.runtime.library.output;

import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


@RunWith(Parameterized.class)
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

  private KeyValuesWriter writer;
  private OrderedPartitionedKVOutput sortedOutput;
  private boolean sendEmptyPartitionViaEvent;
  //Partition index for which data should not be written to.
  private int emptyPartitionIdx;

  /**
   * Constructor
   *
   * @param sendEmptyPartitionViaEvent
   * @param sorterImpl Which sorter impl ( pipeline/legacy )
   * @param sorterThreads number of threads needed for sorter (required only for pipelined sorter)
   * @param emptyPartitionIdx for which data should not be generated
   */
  public TestOnFileSortedOutput(boolean sendEmptyPartitionViaEvent, SorterImpl sorterImpl,
      int sorterThreads, int emptyPartitionIdx) throws IOException {
    this.sendEmptyPartitionViaEvent = sendEmptyPartitionViaEvent;
    this.emptyPartitionIdx = emptyPartitionIdx;
    this.sorterImpl = sorterImpl;
    this.sorterThreads = sorterThreads;

    conf = new Configuration();

    workingDir = new Path(".", this.getClass().getName());
    String localDirs = workingDir.toString();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
    fs = FileSystem.getLocal(conf);
  }

  @Before
  public void setup() throws Exception {
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, sorterImpl.name());
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS, sorterThreads);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);

    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
        HashPartitioner.class.getName());

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        sendEmptyPartitionViaEvent);

    outputSize.set(0);
    fs.mkdirs(workingDir);
    this.partitions = Math.max(1, rnd.nextInt(10));
  }

  @After
  public void cleanup() throws IOException {
    fs.delete(workingDir, true);
  }

  @Parameterized.Parameters(name = "test[{0}, {1}, {2}, {3}]")
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<Object[]>();
    //empty_partition_via_events_enabled, noOfSortThreads, partitionToBeEmpty
    parameters.add(new Object[] { false, SorterImpl.LEGACY, 1, -1 });
    parameters.add(new Object[] { false, SorterImpl.LEGACY, 1, 0 });
    parameters.add(new Object[] { true, SorterImpl.LEGACY, 1, -1 });
    parameters.add(new Object[] { true, SorterImpl.LEGACY, 1, 0 });

    //Pipelined sorter
    parameters.add(new Object[] { false, SorterImpl.PIPELINED, 2, -1 });
    parameters.add(new Object[] { false, SorterImpl.PIPELINED, 2, 0 });
    parameters.add(new Object[] { true, SorterImpl.PIPELINED, 2, -1 });
    parameters.add(new Object[] { true, SorterImpl.PIPELINED, 2, 0 });

    return parameters;
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

  @Test (timeout = 5000)
  public void testPipelinedShuffle() throws Exception {
    _testPipelinedShuffle(SorterImpl.PIPELINED.name());
  }

  @Test (timeout = 5000)
  public void testPipelinedShuffleWithFinalMerge() throws Exception {
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

  @Test
  public void testPipelinedSettingsWithDefaultSorter() throws Exception {
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

  @Test (timeout = 5000)
  public void testSortBufferSize() throws Exception{
    OutputContext context = createTezOutputContext();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 2048);
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    doReturn(payLoad).when(context).getUserPayload();
    sortedOutput = new OrderedPartitionedKVOutput(context, partitions);
    try {
      //Memory limit checks are done in sorter impls. For e.g, defaultsorter does not support > 2GB
      sortedOutput.initialize();
      DefaultSorter sorter = new DefaultSorter(context, conf, 100, 3500*1024*1024l);
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

  @Test(timeout = 5000)
  public void baseTest() throws Exception {
    startSortedOutput(partitions);

    //Write random set of keys
    for (int i = 0; i < Math.max(1, rnd.nextInt(50)); i++) {
      Text key = new Text(new BigInteger(256, rnd).toString());
      LinkedList values = new LinkedList();
      for (int j = 0; j < Math.max(2, rnd.nextInt(10)); j++) {
        values.add(new Text(new BigInteger(256, rnd).toString()));
      }
      writer.write(key, values);
    }

    List<Event> eventList = sortedOutput.close();
    assertTrue(eventList != null && eventList.size() == 2);

    ShuffleUserPayloads.DataMovementEventPayloadProto
        payload = ShuffleUserPayloads.DataMovementEventPayloadProto
        .parseFrom(
            ByteString.copyFrom(((CompositeDataMovementEvent) eventList.get(1)).getUserPayload()));

    assertEquals(HOST, payload.getHost());
    assertEquals(PORT, payload.getPort());
    assertEquals(UniqueID, payload.getPathComponent());
  }

  @Test(timeout = 5000)
  public void testWithSomeEmptyPartition() throws Exception {
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

  @Test(timeout = 5000)
  public void testAllEmptyPartition() throws Exception {
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
    
    OutputContext context = mock(OutputContext.class);
    doReturn(counters).when(context).getCounters();
    doReturn(workingDirs).when(context).getWorkDirs();
    doReturn(payLoad).when(context).getUserPayload();
    doReturn(5 * 1024 * 1024l).when(context).getTotalMemoryAvailableToTask();
    doReturn(UniqueID).when(context).getUniqueIdentifier();
    doReturn("v1").when(context).getDestinationVertexName();
    doReturn(ByteBuffer.wrap(serviceProviderMetaData.getData())).when(context)
        .getServiceProviderMetaData
            (ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    doAnswer(new Answer() {
      @Override public Object answer(InvocationOnMock invocation) throws Throwable {
        long requestedSize = (Long) invocation.getArguments()[0];
        MemoryUpdateCallbackHandler callback = (MemoryUpdateCallbackHandler) invocation
            .getArguments()[1];
        callback.memoryAssigned(requestedSize);
        return null;
      }
    }).when(context).requestInitialMemory(anyLong(), any(MemoryUpdateCallback.class));
    ExecutionContext ExecutionContext = mock(ExecutionContext.class);
    doReturn(HOST).when(ExecutionContext).getHostName();
    doReturn(reporter).when(context).getStatisticsReporter();
    doReturn(ExecutionContext).when(context).getExecutionContext();
    return context;
  }

  @Test(timeout=5000)
  public void testInvalidSorter() throws Exception {
    try {
      _testPipelinedShuffle("Foo");
      Assert.fail("Expected start to fail due to invalid sorter");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test(timeout=5000)
  public void testLowerCaseNamedSorter() throws Exception {
    _testPipelinedShuffle("Pipelined");
  }


}
