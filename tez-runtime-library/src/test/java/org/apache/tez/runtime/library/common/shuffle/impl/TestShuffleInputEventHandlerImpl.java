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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestShuffleInputEventHandlerImpl {

  private static final String HOST = "localhost";
  private static final int PORT = 8080;
  private static final String PATH_COMPONENT = "attempttmp";

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
