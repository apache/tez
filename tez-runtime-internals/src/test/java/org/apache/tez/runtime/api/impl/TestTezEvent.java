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
package org.apache.tez.runtime.api.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

public class TestTezEvent {

  @Test
  public void testSerialization() throws IOException {

    ArrayList<TezEvent> events = new ArrayList<TezEvent>();

    Configuration conf = new Configuration(true);
    String confVal = RandomStringUtils.random(10000, true, true);
    conf.set("testKey", confVal);
    UserPayload payload = TezUtils.createUserPayloadFromConf(conf);
    TezTaskAttemptID srcTAID = TezTaskAttemptID.getInstance(
        TezTaskID.fromString("task_1454468251169_866787_1_02_000000"), 1000);
    TezTaskAttemptID destTAID = TezTaskAttemptID.getInstance(
        TezTaskID.fromString("task_1454468251169_866787_1_02_000000"), 2000);
    EventMetaData srcInfo = new EventMetaData(EventProducerConsumerType.OUTPUT,
        "v1", "v2", srcTAID);
    EventMetaData destInfo = new EventMetaData(EventProducerConsumerType.OUTPUT,
        "v3", "v4", destTAID);

    // Case of size less than 4K and parsing skipped during deserialization
    events.add(new TezEvent(new TaskAttemptCompletedEvent(), new EventMetaData(
        EventProducerConsumerType.PROCESSOR, "v1", "v2", srcTAID)));
    TezEvent dmeEvent = new TezEvent(DataMovementEvent.create(1000, 3, 1,
        payload.getPayload()), srcInfo, System.currentTimeMillis());
    dmeEvent.setDestinationInfo(destInfo);
    events.add(dmeEvent);
    // Different code path
    events.add(new TezEvent(new TaskStatusUpdateEvent(null, 0.1f, null, false),
        new EventMetaData(EventProducerConsumerType.PROCESSOR, "v5", "v6",
            srcTAID)));

    // Serialize to different types of DataOutput
    // One that implements OutputStream and one that does not
    DataOutputBuffer dataout = new DataOutputBuffer();
    ByteArrayDataOutput bout = ByteStreams.newDataOutput();
    serializeEvents(events, dataout);
    serializeEvents(events, bout);

    // Deserialize from different types of DataInput
    // One with DataInputBuffer and another different implementation
    DataInputBuffer datain = new DataInputBuffer();
    datain.reset(dataout.getData(), dataout.getLength());
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dataout.getData(), 0, dataout.getLength()));
    ArrayList<TezEvent> actual1 = deserializeEvents(datain);
    ArrayList<TezEvent> actual2 = deserializeEvents(dis);
    assertEventEquals(events, actual1);
    assertEventEquals(events, actual2);

    byte[] serializedBytes = bout.toByteArray();
    datain.reset(serializedBytes, serializedBytes.length);
    dis = new DataInputStream(new ByteArrayInputStream(serializedBytes));
    actual1 = deserializeEvents(datain);
    actual2 = deserializeEvents(dis);
    assertEventEquals(events, actual1);
    assertEventEquals(events, actual2);

  }

  private void serializeEvents(ArrayList<TezEvent> events, DataOutput out) throws IOException {
    out.writeInt(events.size());
    for (TezEvent e : events) {
      e.write(out);
    }
  }

  private ArrayList<TezEvent> deserializeEvents(DataInput in) throws IOException {
    int eventsCount = in.readInt();
    ArrayList<TezEvent> events = new ArrayList<TezEvent>(eventsCount);
    for (int i = 0; i < eventsCount; ++i) {
      TezEvent e = new TezEvent();
      e.readFields(in);
      events.add(e);
    }
    return events;
  }

  private void assertEventEquals(ArrayList<TezEvent> expectedList, ArrayList<TezEvent> actualList) {
    Assert.assertEquals(expectedList.size(), actualList.size());
    for (int i = 0; i < expectedList.size(); i++) {
      TezEvent expected = expectedList.get(i);
      TezEvent actual = actualList.get(i);
      Assert.assertEquals(expected.getEventReceivedTime(), actual.getEventReceivedTime());
      Assert.assertEquals(expected.getSourceInfo(), actual.getSourceInfo());
      Assert.assertEquals(expected.getDestinationInfo(), actual.getDestinationInfo());
      Assert.assertEquals(expected.getEventType(), actual.getEventType());
      // Doing this instead of implementing equals methods for events
      if (i == 0) {
        Assert.assertTrue(actual.getEvent() instanceof TaskAttemptCompletedEvent);
      } else if (i == 1) {
        DataMovementEvent dmeExpected = (DataMovementEvent) expected.getEvent();
        DataMovementEvent dmeActual = (DataMovementEvent) actual.getEvent();
        Assert.assertEquals(dmeExpected.getSourceIndex(), dmeActual.getSourceIndex());
        Assert.assertEquals(dmeExpected.getTargetIndex(), dmeActual.getTargetIndex());
        Assert.assertEquals(dmeExpected.getVersion(), dmeActual.getVersion());
        Assert.assertEquals(dmeExpected.getUserPayload(), dmeActual.getUserPayload());
      } else {
        TaskStatusUpdateEvent tsuExpected = (TaskStatusUpdateEvent) expected.getEvent();
        TaskStatusUpdateEvent tsuActual = (TaskStatusUpdateEvent) actual.getEvent();
        Assert.assertEquals(tsuExpected.getCounters(), tsuActual.getCounters());
        Assert.assertEquals(tsuExpected.getProgress(), tsuActual.getProgress(), 0);
        Assert.assertEquals(tsuExpected.getProgressNotified(), tsuActual.getProgressNotified());
        Assert.assertEquals(tsuExpected.getStatistics(), tsuActual.getStatistics());
      }
    }
  }

}
