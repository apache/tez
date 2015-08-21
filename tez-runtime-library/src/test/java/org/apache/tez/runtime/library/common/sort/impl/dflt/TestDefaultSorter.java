/*
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

package org.apache.tez.runtime.library.common.sort.impl.dflt;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.google.protobuf.ByteString;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringInterner;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestDefaultSorter {

  private Configuration conf;
  private static final int PORT = 80;
  private static final String UniqueID = "UUID";

  private static FileSystem localFs = null;
  private static Path workingDir = null;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
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
  }

  @AfterClass
  public static void cleanup() throws IOException {
    localFs.delete(workingDir, true);
  }

  @After
  public void reset() throws IOException {
    cleanup();
    localFs.mkdirs(workingDir);
  }

  @Test(timeout = 5000)
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
  @Ignore
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
  @Ignore
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
      String val = StringInterner.weakIntern(StringUtils.repeat("v", valSize));
      sorter.write(key, new Text(val));
      i = (i + 1) % 10;
    }
  }


  @Test(timeout = 5000)
  public void testSortMBLimits() throws Exception {

    assertTrue("Expected " + DefaultSorter.MAX_IO_SORT_MB,
        DefaultSorter.computeSortBufferSize(4096) == DefaultSorter.MAX_IO_SORT_MB);
    assertTrue("Expected " + DefaultSorter.MAX_IO_SORT_MB,
        DefaultSorter.computeSortBufferSize(2047) == DefaultSorter.MAX_IO_SORT_MB);
    assertTrue("Expected 1024", DefaultSorter.computeSortBufferSize(1024) == 1024);

    try {
      DefaultSorter.computeSortBufferSize(0);
      fail("Should have thrown error for setting buffer size to 0");
    } catch(RuntimeException re) {
    }

    try {
      DefaultSorter.computeSortBufferSize(-100);
      fail("Should have thrown error for setting buffer size to negative value");
    } catch(RuntimeException re) {
    }
  }

  @Test(timeout = 30000)
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
    DefaultSorter sorter = new DefaultSorter(context, conf, 1, handler.getMemoryAssigned());

    //Write 1000 keys each of size 1000, (> 1 spill should happen)
    try {
      writeData(sorter, 1000, 1000);
      assertTrue(sorter.getNumSpills() > 2);
    } catch(IOException ioe) {
      fail(ioe.getMessage());
    }
  }

  @Test(timeout = 30000)
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
      assertTrue(sorter.getFinalOutputFile().getParent().getName().equalsIgnoreCase(UniqueID));
    } catch(Exception e) {
      fail();
    }
  }

  @Test(timeout = 30000)
  public void testWithEmptyDataWithFinalMergeDisabled() throws IOException {
    OutputContext context = createTezOutputContext();

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 1);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    DefaultSorter sorter = new DefaultSorter(context, conf, 1, handler.getMemoryAssigned());

    //no data written. Empty
    try {
      sorter.flush();
      sorter.close();
      assertTrue(sorter.getFinalOutputFile().getParent().getName().equalsIgnoreCase(UniqueID +
          "_0"));
      assertTrue(context.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL).getValue() > 0);
    } catch(Exception e) {
      fail();
    }
  }

  @Test(timeout = 30000)
  @SuppressWarnings("unchecked")
  public void testWithSingleSpillWithFinalMergeDisabled() throws IOException {
    OutputContext context = createTezOutputContext();

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 4);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    DefaultSorter sorter = new DefaultSorter(context, conf, 1, handler.getMemoryAssigned());

    writeData(sorter, 1000, 10);
    assertTrue(sorter.getNumSpills() == 1);
    ArgumentCaptor<List> eventCaptor = ArgumentCaptor.forClass(List.class);
    verify(context, times(1)).sendEvents(eventCaptor.capture());
    List<Event> events = eventCaptor.getValue();
    for(Event event : events) {
      if (event instanceof CompositeDataMovementEvent) {
        CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) event;
        ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload = ShuffleUserPayloads
            .DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(cdme.getUserPayload()));
        assertTrue(shufflePayload.getPathComponent().equalsIgnoreCase(UniqueID + "_0"));
      }
    }

    assertTrue(context.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL).getValue() > 0);
  }

  @Test(timeout = 30000)
  @SuppressWarnings("unchecked")
  public void testWithMultipleSpillsWithFinalMergeDisabled() throws IOException {
    OutputContext context = createTezOutputContext();

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    conf.setLong(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 4);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES, 1);
    MemoryUpdateCallbackHandler handler = new MemoryUpdateCallbackHandler();
    context.requestInitialMemory(ExternalSorter.getInitialMemoryRequirement(conf,
        context.getTotalMemoryAvailableToTask()), handler);
    DefaultSorter sorter = new DefaultSorter(context, conf, 1, handler.getMemoryAssigned());

    writeData(sorter, 10000, 1000);
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
        spillIndex++;
      }
    }
    assertTrue(spillIndex == spillCount);
    assertTrue(context.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL).getValue() > 0);
  }

  private void writeData(ExternalSorter sorter, int numKeys, int keyLen) throws IOException {
    for (int i = 0; i < numKeys; i++) {
      Text key = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
      Text value = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
      sorter.write(key, value);
    }
    sorter.flush();
    sorter.close();
  }

  private OutputContext createTezOutputContext() throws IOException {
    String[] workingDirs = { workingDir.toString() };
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    DataOutputBuffer serviceProviderMetaData = new DataOutputBuffer();
    serviceProviderMetaData.writeInt(PORT);

    TezCounters counters = new TezCounters();

    OutputContext context = mock(OutputContext.class);
    ExecutionContext execContext = new ExecutionContextImpl("localhost");
    doReturn(execContext).when(context).getExecutionContext();
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
    return context;
  }
}