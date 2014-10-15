package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import com.google.protobuf.ByteString;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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

public class TestShuffleInputEventHandler {

  private static final String HOST = "localhost";
  private static final int PORT = 8080;
  private static final String PATH_COMPONENT = "attempt";

  private ShuffleInputEventHandlerOrderedGrouped handler;
  private ShuffleScheduler scheduler;

  private InputContext createTezInputContext() {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    InputContext inputContext = mock(InputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    return inputContext;
  }

  private Event createDataMovementEvent(int srcIndex, int targetIndex,
      ByteString emptyPartitionByteString, boolean allPartitionsEmpty) {
    ShuffleUserPayloads.DataMovementEventPayloadProto.Builder builder =
        ShuffleUserPayloads.DataMovementEventPayloadProto
            .newBuilder();
    if (!allPartitionsEmpty) {
      builder.setHost(HOST);
      builder.setPort(PORT);
      builder.setPathComponent(PATH_COMPONENT);
    }
    builder.setRunDuration(10);
    if (emptyPartitionByteString != null) {
      builder.setEmptyPartitions(emptyPartitionByteString);
    }
    return DataMovementEvent
        .create(srcIndex, targetIndex, 0, builder.build().toByteString().asReadOnlyByteBuffer());
  }

  @Before
  public void setup() throws Exception {
    InputContext inputContext = createTezInputContext();
    scheduler = mock(ShuffleScheduler.class);
    handler = new ShuffleInputEventHandlerOrderedGrouped(inputContext, scheduler, false);
  }

  @Test
  public void basicTest() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int targetIdx = 1;
    Event dme = createDataMovementEvent(srcIdx, targetIdx, null, false);
    events.add(dme);
    handler.handleEvents(events);
    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(targetIdx, 0,
        PATH_COMPONENT);

    String baseUri = handler.getBaseURI(HOST, PORT, srcIdx).toString();
    int partitionId = srcIdx;
    verify(scheduler).addKnownMapOutput(eq(HOST), eq(PORT), eq(partitionId),
        eq(baseUri), eq(expectedIdentifier));
  }

  @Test
  public void testFailedEvent() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int targetIdx = 1;
    InputFailedEvent failedEvent = InputFailedEvent.create(targetIdx, 0);
    events.add(failedEvent);
    handler.handleEvents(events);
    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(targetIdx, 0);
    verify(scheduler).obsoleteInput(eq(expectedIdentifier));
  }

  @Test
  public void testAllPartitionsEmpty() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int targetIdx = 1;
    Event dme = createDataMovementEvent(srcIdx, targetIdx, createEmptyPartitionByteString(srcIdx)
        , true);
    events.add(dme);
    handler.handleEvents(events);

    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(targetIdx, 0);
    verify(scheduler).copySucceeded(eq(expectedIdentifier), any(MapHost.class), eq(0l),
        eq(0l), eq(0l), any(MapOutput.class));
  }

  @Test
  public void testCurrentPartitionEmpty() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int targetIdx = 1;
    Event dme = createDataMovementEvent(srcIdx, targetIdx, createEmptyPartitionByteString(srcIdx)
        , false);
    events.add(dme);
    handler.handleEvents(events);

    InputAttemptIdentifier expectedIdentifier = new InputAttemptIdentifier(targetIdx, 0);

    verify(scheduler).copySucceeded(eq(expectedIdentifier), any(MapHost.class), eq(0l),
        eq(0l), eq(0l), any(MapOutput.class));
  }

  @Test
  public void testOtherPartitionEmpty() throws IOException {
    List<Event> events = new LinkedList<Event>();
    int srcIdx = 0;
    int taskIndex = 1;
    Event dme = createDataMovementEvent(srcIdx, taskIndex, createEmptyPartitionByteString(100),
        false);
    events.add(dme);
    handler.handleEvents(events);

    String baseUri = handler.getBaseURI(HOST, PORT, srcIdx).toString();
    int partitionId = srcIdx;
    InputAttemptIdentifier expectedIdentifier =
        new InputAttemptIdentifier(taskIndex, 0, PATH_COMPONENT);

    verify(scheduler).addKnownMapOutput(eq(HOST), eq(PORT), eq(partitionId), eq(baseUri),
        eq(expectedIdentifier));
  }

  private ByteString createEmptyPartitionByteString(int... emptyPartitions) throws IOException {
    BitSet bitSet = new BitSet();
    for (int i : emptyPartitions) {
      bitSet.set(i);
    }
    return TezCommonUtils.compressByteArrayToByteString(TezUtilsInternal.toByteArray(bitSet));
  }

}
