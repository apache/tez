package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class TestShuffleInputEventHandlerOrderedGrouped {
  private static final String HOST = "localhost";
  private static final int PORT = 8080;
  private static final String PATH_COMPONENT = "attempt";
  private ShuffleInputEventHandlerOrderedGrouped handler;
  private ShuffleScheduler scheduler;
  private ShuffleScheduler realScheduler;
  private MergeManager mergeManager;

  private TezExecutors sharedExecutor;

  private InputContext createTezInputContext() throws IOException {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    InputContext inputContext = mock(InputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn("sourceVertex").when(inputContext).getSourceVertexName();
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    ExecutionContext executionContext = new ExecutionContextImpl("localhost");
    doReturn(executionContext).when(inputContext).getExecutionContext();
    ByteBuffer shuffleBuffer = ByteBuffer.allocate(4).putInt(0, 4);
    doReturn(shuffleBuffer).when(inputContext).getServiceProviderMetaData(anyString());
    Token<JobTokenIdentifier> sessionToken = new Token<JobTokenIdentifier>(new JobTokenIdentifier(new Text("text")),
        new JobTokenSecretManager());
    ByteBuffer tokenBuffer = TezCommonUtils.serializeServiceData(sessionToken);
    doReturn(tokenBuffer).when(inputContext).getServiceConsumerMetaData(anyString());
    when(inputContext.createTezFrameworkExecutorService(anyInt(), anyString())).thenAnswer(
        new Answer<ExecutorService>() {
          @Override
          public ExecutorService answer(InvocationOnMock invocation) throws Throwable {
            return sharedExecutor.createExecutorService(
                invocation.getArgument(0, Integer.class),
                invocation.getArgument(1, String.class));
          }
        });
    return inputContext;
  }

  private Event createDataMovementEvent(int srcIndex, int targetIndex,
      ByteString emptyPartitionByteString, boolean allPartitionsEmpty) {
    return createDataMovementEvent(srcIndex, targetIndex, emptyPartitionByteString,
        allPartitionsEmpty, false, false, 0);
  }

  private Event createDataMovementEvent(int srcIndex, int targetIndex,
      ByteString emptyPartitionByteString, boolean allPartitionsEmpty, boolean
      finalMergeDisabled, boolean incrementalEvent, int spillId) {
    return createDataMovementEvent(srcIndex, targetIndex, emptyPartitionByteString,
        allPartitionsEmpty, finalMergeDisabled, incrementalEvent, spillId, HOST, PORT);
  }

  private Event createDataMovementEvent(int srcIndex, int targetIndex,
      ByteString emptyPartitionByteString, boolean allPartitionsEmpty, boolean
      finalMergeDisabled, boolean incrementalEvent, int spillId, int attemptNum) {
    return createDataMovementEvent(srcIndex, targetIndex, emptyPartitionByteString,
        allPartitionsEmpty, finalMergeDisabled, incrementalEvent, spillId, HOST, PORT, attemptNum);
  }

  private Event createDataMovementEvent(int srcIndex, int targetIndex,
      ByteString emptyPartitionByteString, boolean allPartitionsEmpty, boolean
      finalMergeDisabled, boolean incrementalEvent, int spillId, String host, int port) {
    return createDataMovementEvent(srcIndex, targetIndex, emptyPartitionByteString,
        allPartitionsEmpty, finalMergeDisabled, incrementalEvent, spillId, host, port, 0);
  }

  private Event createDataMovementEvent(int srcIndex, int targetIndex,
      ByteString emptyPartitionByteString, boolean allPartitionsEmpty, boolean
      finalMergeDisabled, boolean incrementalEvent, int spillId, String host, int port, int attemptNum) {
    ShuffleUserPayloads.DataMovementEventPayloadProto.Builder builder =
        ShuffleUserPayloads.DataMovementEventPayloadProto
            .newBuilder();
    if (!allPartitionsEmpty) {
      builder.setHost(host);
      builder.setPort(port);
      builder.setPathComponent(PATH_COMPONENT);
    }
    if (finalMergeDisabled) {
      builder.setLastEvent(incrementalEvent ? false : true);
      builder.setSpillId(spillId);
    }
    builder.setRunDuration(10);
    if (emptyPartitionByteString != null) {
      builder.setEmptyPartitions(emptyPartitionByteString);
    }
    return DataMovementEvent
        .create(srcIndex, targetIndex, attemptNum, builder.build().toByteString().asReadOnlyByteBuffer());
  }

  @Before
  public void setup() throws Exception {
    sharedExecutor = new TezSharedExecutor(new Configuration());
    setupScheduler(2);
  }

  @After
  public void cleanup() {
    sharedExecutor.shutdownNow();
  }

  private void setupScheduler(int numInputs) throws Exception {
    InputContext inputContext = createTezInputContext();
    Configuration config = new Configuration();
    realScheduler = new ShuffleScheduler(
        inputContext,
        config,
        numInputs,
        mock(Shuffle.class),
        mock(MergeManager.class),
        mock(MergeManager.class),
        System.currentTimeMillis(),
        null,
        false,
        0,
        "src vertex");
    scheduler = spy(realScheduler);
    handler = new ShuffleInputEventHandlerOrderedGrouped(inputContext, scheduler, ShuffleUtils.isTezShuffleHandler(config));
    mergeManager = mock(MergeManager.class);
  }

  @Test (timeout = 10000)
  public void testPiplinedShuffleEvents() throws IOException, InterruptedException {
    //test with 2 events per input (2 inputs)
    int attemptNum = 0;
    int inputIdx = 0;
    Event dme1 = createDataMovementEvent(attemptNum, inputIdx, null, false, true, true, 0);
    CompositeInputAttemptIdentifier id1 =
        new CompositeInputAttemptIdentifier(inputIdx, attemptNum,
            PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0, 1);
    handler.handleEvents(Collections.singletonList(dme1));
    int partitionId = attemptNum;
    verify(scheduler).addKnownMapOutput(eq(HOST), eq(PORT), eq(partitionId), eq(id1));
    verify(scheduler).pipelinedShuffleInfoEventsMap.containsKey(id1.getInputIdentifier());

    //Send final_update event.
    Event dme2 = createDataMovementEvent(attemptNum, inputIdx, null, false, true, false, 1);
    CompositeInputAttemptIdentifier id2 =
        new CompositeInputAttemptIdentifier(inputIdx, attemptNum,
            PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE, 1, 1);
    handler.handleEvents(Collections.singletonList(dme2));
    partitionId = attemptNum;
    assertTrue(scheduler.pipelinedShuffleInfoEventsMap.containsKey(id2.getInputIdentifier()));
    verify(scheduler).addKnownMapOutput(eq(HOST), eq(PORT), eq(partitionId), eq(id2));
    assertTrue(scheduler.pipelinedShuffleInfoEventsMap.containsKey(id2.getInputIdentifier()));

    MapHost host = scheduler.getHost();
    assertTrue(host != null);
    List<InputAttemptIdentifier> list = scheduler.getMapsForHost(host);
    assertTrue(!list.isEmpty());
    //Let the final_update event pass
    MapOutput output = MapOutput.createMemoryMapOutput(id2, mergeManager, 1000, true);
    scheduler.copySucceeded(id2, host, 1000, 10000, 10000, output, false);
    assertTrue(!scheduler.isDone()); //we haven't downloaded id1 yet
    output = MapOutput.createMemoryMapOutput(id1, mergeManager, 1000, true);
    scheduler.copySucceeded(id1, host, 1000, 10000, 10000, output, false);
    assertTrue(!scheduler.isDone()); //we haven't downloaded another source yet

    //Send events for source 2
    attemptNum = 0;
    inputIdx = 1;
    Event dme3 = createDataMovementEvent(attemptNum, inputIdx, null, false, true,
        true, 1);
    InputAttemptIdentifier id3 = new InputAttemptIdentifier(inputIdx,
        attemptNum, PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE,
        0);
    handler.handleEvents(Collections.singletonList(dme3));
    //Send final_update event (empty partition directly invoking copySucceeded).
    InputAttemptIdentifier id4 = new InputAttemptIdentifier(inputIdx,
        attemptNum, PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE, 1);
    assertTrue(!scheduler.isInputFinished(id4.getInputIdentifier()));
    scheduler.copySucceeded(id4, null, 0, 0, 0, null, false);
    assertTrue(!scheduler.isDone()); //we haven't downloaded another id yet
    //Let the incremental event pass
    output = MapOutput.createMemoryMapOutput(id3, mergeManager, 1000, true);
    scheduler.copySucceeded(id3, host, 1000, 10000, 10000, output, false);
    assertTrue(scheduler.isDone());
  }

  @Test (timeout = 5000)
  public void testPiplinedShuffleEvents_WithOutofOrderAttempts() throws IOException, InterruptedException {
    //Process attempt #1 first
    int attemptNum = 1;
    int inputIdx = 1;

    Event dme1 = createDataMovementEvent(attemptNum, inputIdx, null, false, true, true, 0, attemptNum);
    handler.handleEvents(Collections.singletonList(dme1));

    CompositeInputAttemptIdentifier id1 =
        new CompositeInputAttemptIdentifier(inputIdx, attemptNum,
            PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0, 1);

    verify(scheduler, times(1)).addKnownMapOutput(eq(HOST), eq(PORT), eq(1), eq(id1));
    assertTrue("Shuffle info events should not be empty for pipelined shuffle",
        !scheduler.pipelinedShuffleInfoEventsMap.isEmpty());

    int valuesInMapLocations = scheduler.mapLocations.values().size();
    assertTrue("Maplocations should have values. current size: " + valuesInMapLocations,
        valuesInMapLocations > 0);

    // start scheduling for download
    scheduler.getMapsForHost(scheduler.mapLocations.values().iterator().next());

    //Attempt #0 comes up. When processing this, it should report exception
    attemptNum = 0;
    inputIdx = 1;
    Event dme2 = createDataMovementEvent(attemptNum, inputIdx, null, false, true, true, 0, attemptNum);
    handler.handleEvents(Collections.singletonList(dme2));

    // task should issue kill request
    verify(scheduler, times(1)).killSelf(any(), any());
  }

  @Test (timeout = 5000)
  public void testPipelinedShuffle_WithObsoleteEvents() throws IOException, InterruptedException {
    //Process attempt #1 first
    int attemptNum = 1;
    int inputIdx = 1;

    Event dme1 = createDataMovementEvent(attemptNum, inputIdx, null, false, true, true, 0, attemptNum);
    handler.handleEvents(Collections.singletonList(dme1));

    CompositeInputAttemptIdentifier id1 =
        new CompositeInputAttemptIdentifier(inputIdx, attemptNum,
            PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0, 1);

    verify(scheduler, times(1)).addKnownMapOutput(eq(HOST), eq(PORT), eq(1), eq(id1));
    assertTrue("Shuffle info events should not be empty for pipelined shuffle",
        !scheduler.pipelinedShuffleInfoEventsMap.isEmpty());

    int valuesInMapLocations = scheduler.mapLocations.values().size();
    assertTrue("Maplocations should have values. current size: " + valuesInMapLocations,
        valuesInMapLocations > 0);

    // start scheduling for download. Sets up scheduledForDownload in eventInfo.
    scheduler.getMapsForHost(scheduler.mapLocations.values().iterator().next());

    // send input failed event.
    List<Event> events = new LinkedList<Event>();
    int targetIdx = 1;
    InputFailedEvent failedEvent = InputFailedEvent.create(targetIdx, 0);
    events.add(failedEvent);
    handler.handleEvents(events);

    // task should issue kill request, as inputs are scheduled for download already.
    verify(scheduler, times(1)).killSelf(any(), any());
  }

  @Test(timeout = 5000)
  public void basicTest() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int targetIdx = 1;
    Event dme = createDataMovementEvent(srcIdx, targetIdx, null, false);
    events.add(dme);
    handler.handleEvents(events);
    CompositeInputAttemptIdentifier expectedIdentifier = new CompositeInputAttemptIdentifier(targetIdx, 0,
        PATH_COMPONENT, 1);
    int partitionId = srcIdx;
    verify(scheduler).addKnownMapOutput(eq(HOST), eq(PORT), eq(partitionId),
        eq(expectedIdentifier));
    assertTrue("Shuffle info events should be empty for regular shuffle codepath",
        scheduler.pipelinedShuffleInfoEventsMap.isEmpty());
  }

  @Test(timeout = 5000)
  public void testFailedEvent() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int targetIdx = 1;
    InputFailedEvent failedEvent = InputFailedEvent.create(targetIdx, 0);
    events.add(failedEvent);
    handler.handleEvents(events);
    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(targetIdx, 0);
    verify(scheduler).obsoleteInput(eq(expectedIdentifier));
  }

  @Test(timeout = 5000)
  public void testAllPartitionsEmpty() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int targetIdx = 1;
    Event dme = createDataMovementEvent(srcIdx, targetIdx, createEmptyPartitionByteString(srcIdx)
        , true);
    events.add(dme);
    handler.handleEvents(events);
    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(targetIdx, 0);
    verify(scheduler).copySucceeded(eq(expectedIdentifier), any(), eq(0L),
            eq(0L), eq(0L), any(), eq(true));
  }

  @Test(timeout = 5000)
  public void testCurrentPartitionEmpty() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int targetIdx = 1;
    Event dme = createDataMovementEvent(srcIdx, targetIdx, createEmptyPartitionByteString(srcIdx)
        , false);
    events.add(dme);
    handler.handleEvents(events);
    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(targetIdx, 0);
    verify(scheduler).copySucceeded(eq(expectedIdentifier), any(), eq(0L),
        eq(0L), eq(0L), any(), eq(true));
  }

  @Test(timeout = 5000)
  public void testOtherPartitionEmpty() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int taskIndex = 1;
    Event dme = createDataMovementEvent(srcIdx, taskIndex, createEmptyPartitionByteString(100),
        false);
    events.add(dme);
    handler.handleEvents(events);
    int partitionId = srcIdx;
    CompositeInputAttemptIdentifier expectedIdentifier =
        new CompositeInputAttemptIdentifier(taskIndex, 0, PATH_COMPONENT, 1);
    verify(scheduler).addKnownMapOutput(eq(HOST), eq(PORT), eq(partitionId), eq(expectedIdentifier));
  }

  private ByteString createEmptyPartitionByteString(int... emptyPartitions) throws IOException {
    BitSet bitSet = new BitSet();
    for (int i : emptyPartitions) {
      bitSet.set(i);
    }
    return TezCommonUtils.compressByteArrayToByteString(TezUtilsInternal.toByteArray(bitSet));
  }
}
