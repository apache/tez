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

package org.apache.tez.runtime.library.common.shuffle.impl;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestShuffleInputEventHandlerImpl {

  private static final String HOST = "localhost";
  private static final int PORT = 8080;
  private static final String PATH_COMPONENT = "attempttmp";
  private final Configuration conf = new Configuration();

  @Test(timeout = 5000)
  public void testSimple() throws IOException {
    InputContext inputContext = mock(InputContext.class);
    ShuffleManager shuffleManager = mock(ShuffleManager.class);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
        shuffleManager, inputAllocator, null, false, 0);

    int taskIndex = 1;
    Event dme = createDataMovementEvent(0, taskIndex, null);

    List<Event> eventList = new LinkedList<Event>();
    eventList.add(dme);
    handler.handleEvents(eventList);

    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(taskIndex, 0,
        PATH_COMPONENT);

    verify(shuffleManager).addKnownInput(eq(HOST), eq(PORT), eq(expectedIdentifier), eq(0));
  }

  @Test(timeout = 5000)
  public void testCurrentPartitionEmpty() throws IOException {
    InputContext inputContext = mock(InputContext.class);
    ShuffleManager shuffleManager = mock(ShuffleManager.class);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
        shuffleManager, inputAllocator, null, false, 0);

    int taskIndex = 1;
    Event dme = createDataMovementEvent(0, taskIndex, createEmptyPartitionByteString(0));

    List<Event> eventList = new LinkedList<Event>();
    eventList.add(dme);
    handler.handleEvents(eventList);

    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(taskIndex, 0);

    verify(shuffleManager).addCompletedInputWithNoData(eq(expectedIdentifier));
  }

  @Test(timeout = 5000)
  public void testOtherPartitionEmpty() throws IOException {
    InputContext inputContext = mock(InputContext.class);
    ShuffleManager shuffleManager = mock(ShuffleManager.class);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
        shuffleManager, inputAllocator, null, false, 0);

    int taskIndex = 1;
    Event dme = createDataMovementEvent(0, taskIndex, createEmptyPartitionByteString(1));
    List<Event> eventList = new LinkedList<Event>();
    eventList.add(dme);
    handler.handleEvents(eventList);

    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(taskIndex, 0, PATH_COMPONENT);

    verify(shuffleManager).addKnownInput(eq(HOST), eq(PORT), eq(expectedIdentifier), eq(0));
  }

  @Test(timeout = 5000)
  public void testMultipleEvents1() throws IOException {
    InputContext inputContext = mock(InputContext.class);
    ShuffleManager shuffleManager = mock(ShuffleManager.class);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
        shuffleManager, inputAllocator, null, false, 0);

    int taskIndex1 = 1;
    Event dme1 = createDataMovementEvent(0, taskIndex1, createEmptyPartitionByteString(0));
    int taskIndex2 = 2;
    Event dme2 = createDataMovementEvent(0, taskIndex2, null);
    
    List<Event> eventList = new LinkedList<Event>();
    eventList.add(dme1);
    eventList.add(dme2);
    handler.handleEvents(eventList);

    InputAttemptIdentifier expectedIdentifier1 = new InputAttemptIdentifier(taskIndex1, 0);
    InputAttemptIdentifier expectedIdentifier2 = new InputAttemptIdentifier(taskIndex2, 0, PATH_COMPONENT);

    verify(shuffleManager).addCompletedInputWithNoData(eq(expectedIdentifier1));
    verify(shuffleManager).addKnownInput(eq(HOST), eq(PORT), eq(expectedIdentifier2), eq(0));
  }


  private InputContext createInputContext() {
    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn("sourceVertex").when(inputContext).getSourceVertexName();
    return inputContext;
  }

  @SuppressWarnings("unchecked")
  private ShuffleManager createShuffleManager(InputContext inputContext) throws IOException {
    Path outDirBase = new Path(".", "outDir");
    String[] outDirs = new String[] { outDirBase.toString() };
    doReturn(outDirs).when(inputContext).getWorkDirs();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, inputContext.getWorkDirs());

    DataOutputBuffer out = new DataOutputBuffer();
    Token<JobTokenIdentifier> token = new Token(new JobTokenIdentifier(), new JobTokenSecretManager(null));
    token.write(out);
    doReturn(ByteBuffer.wrap(out.getData())).when(inputContext).getServiceConsumerMetaData(
        TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID);

    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);
    ShuffleManager realShuffleManager = new ShuffleManager(inputContext, conf, 2,
        1024, false, -1, null, inputAllocator);
    ShuffleManager shuffleManager = spy(realShuffleManager);
    return shuffleManager;
  }

  /**
   * In pipelined shuffle, check if multiple attempt numbers are processed and
   * exceptions are reported properly.
   *
   * @throws IOException
   */
  @Test(timeout = 5000)
  public void testPipelinedShuffleEvents() throws IOException {

    InputContext inputContext = createInputContext();
    ShuffleManager shuffleManager = createShuffleManager(inputContext);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
        shuffleManager, inputAllocator, null, false, 0);

    //0--> 1 with spill id 0 (attemptNum 0)
    Event dme = createDataMovementEvent(true, 0, 1, 0, false, new BitSet(), 4, 0);
    handler.handleEvents(Collections.singletonList(dme));

    InputAttemptIdentifier expectedId1 = new InputAttemptIdentifier(new InputIdentifier(1), 0,
        PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0);
    verify(shuffleManager, times(1)).addKnownInput(eq(HOST), eq(PORT), eq(expectedId1), eq(0));

    //0--> 1 with spill id 1 (attemptNum 0)
    dme = createDataMovementEvent(true, 0, 1, 1, false, new BitSet(), 4, 0);
    handler.handleEvents(Collections.singletonList(dme));

    InputAttemptIdentifier expectedId2 = new InputAttemptIdentifier(new InputIdentifier(1), 0,
        PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 1);
    verify(shuffleManager, times(2)).addKnownInput(eq(HOST), eq(PORT), eq(expectedId2), eq(0));

    //0--> 1 with spill id 1 (attemptNum 1).  This should report exception
    dme = createDataMovementEvent(true, 0, 1, 1, false, new BitSet(), 4, 1);
    handler.handleEvents(Collections.singletonList(dme));
    verify(inputContext).fatalError(any(Throwable.class), anyString());
  }

  /**
   * In pipelined shuffle, check if processing & exceptions are done correctly when attempts are
   * received in out of order fashion (e.g attemptNum 1 arrives before attemptNum 0)
   *
   * @throws IOException
   */
  @Test(timeout = 5000)
  public void testPipelinedShuffleEvents_WithOutOfOrderAttempts() throws IOException {
    InputContext inputContext = createInputContext();
    ShuffleManager shuffleManager = createShuffleManager(inputContext);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
        shuffleManager, inputAllocator, null, false, 0);

    //0--> 1 with spill id 0 (attemptNum 1).  attemptNum 0 is not sent.
    Event dme = createDataMovementEvent(true, 0, 1, 0, false, new BitSet(), 4, 1);
    handler.handleEvents(Collections.singletonList(dme));

    InputAttemptIdentifier expected = new InputAttemptIdentifier(new InputIdentifier(1), 1,
        PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 1);
    verify(shuffleManager, times(1)).addKnownInput(eq(HOST), eq(PORT), eq(expected), eq(0));

    //Now send attemptNum 0.  This should throw exception, because attempt #1 is already added
    dme = createDataMovementEvent(true, 0, 1, 0, false, new BitSet(), 4, 0);
    handler.handleEvents(Collections.singletonList(dme));
    verify(inputContext).fatalError(any(Throwable.class), anyString());
  }

  /**
   * In pipelined shuffle, check if processing & exceptions are done correctly when empty
   * partitions are sent
   *
   * @throws IOException
   */
  @Test(timeout = 5000)
  public void testPipelinedShuffleEvents_WithEmptyPartitions() throws IOException {
    InputContext inputContext = createInputContext();
    ShuffleManager shuffleManager = createShuffleManager(inputContext);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
        shuffleManager, inputAllocator, null, false, 0);

    //0--> 1 with spill id 0 (attemptNum 0) with empty partitions
    BitSet bitSet = new BitSet(4);
    bitSet.flip(0, 4);
    Event dme = createDataMovementEvent(true, 0, 1, 0, false, bitSet, 4, 0);
    handler.handleEvents(Collections.singletonList(dme));

    InputAttemptIdentifier expected = new InputAttemptIdentifier(new InputIdentifier(1), 0,
        PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0);
    verify(shuffleManager, times(1)).addCompletedInputWithNoData(expected);

    //0--> 1 with spill id 1 (attemptNum 0)
    handler.handleEvents(Collections.singletonList(dme));
    dme = createDataMovementEvent(true, 0, 1, 1, false, new BitSet(), 4, 0);
    expected = new InputAttemptIdentifier(new InputIdentifier(1), 0,
        PATH_COMPONENT, false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 1);
    verify(shuffleManager, times(2)).addCompletedInputWithNoData(expected);


    //Now send attemptNum 1.  This should throw exception, because attempt #1 is already added
    dme = createDataMovementEvent(true, 0, 1, 0, false, new BitSet(), 4, 1);
    handler.handleEvents(Collections.singletonList(dme));
    verify(inputContext).fatalError(any(Throwable.class), anyString());
  }

  private Event createDataMovementEvent(boolean addSpillDetails, int srcIdx, int targetIdx,
      int spillId, boolean isLastSpill, BitSet emptyPartitions, int numPartitions, int attemptNum)
      throws IOException {

    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();

    if (emptyPartitions.cardinality() != 0) {
      // Empty partitions exist
      ByteString emptyPartitionsByteString =
          TezCommonUtils.compressByteArrayToByteString(TezUtilsInternal.toByteArray(emptyPartitions));
      payloadBuilder.setEmptyPartitions(emptyPartitionsByteString);
    }

    if (emptyPartitions.cardinality() != numPartitions) {
      // Populate payload only if at least 1 partition has data
      payloadBuilder.setHost(HOST);
      payloadBuilder.setPort(PORT);
      payloadBuilder.setPathComponent("attemptPath");
    }

    if (addSpillDetails) {
      payloadBuilder.setSpillId(spillId);
      payloadBuilder.setLastEvent(isLastSpill);
    }

    ByteBuffer payload = payloadBuilder.build().toByteString().asReadOnlyByteBuffer();
    return  DataMovementEvent.create(srcIdx, targetIdx, attemptNum, payload);
  }
  
  private Event createDataMovementEvent(int srcIndex, int targetIndex,
      ByteString emptyPartitionByteString) {
    DataMovementEventPayloadProto.Builder builder = DataMovementEventPayloadProto.newBuilder();
    builder.setHost(HOST);
    builder.setPort(PORT);
    builder.setPathComponent("attempttmp");
    if (emptyPartitionByteString != null) {
      builder.setEmptyPartitions(emptyPartitionByteString);
    }
    Event dme = DataMovementEvent
        .create(srcIndex, targetIndex, 0, builder.build().toByteString().asReadOnlyByteBuffer());
    return dme;
  }

  private ByteString createEmptyPartitionByteString(int... emptyPartitions) throws IOException {
    BitSet bitSet = new BitSet();
    for (int i : emptyPartitions) {
      bitSet.set(i);
    }
    ByteString emptyPartitionsBytesString = TezCommonUtils.compressByteArrayToByteString(
        TezUtilsInternal
        .toByteArray(bitSet));
    return emptyPartitionsBytesString;
  }

}
