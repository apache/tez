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

package org.apache.tez.mapreduce.common;

import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.lib.MRInputUtils;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputUpdatePayloadEvent;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestMRInputSplitDistributor {

  @Test(timeout = 5000)
  public void testSerializedPayload() throws IOException {

    Configuration conf = new Configuration(false);
    conf.setBoolean(MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD, true);
    ByteString confByteString = TezUtils.createByteStringFromConf(conf);
    InputSplit split1 = new InputSplitForTest(1);
    InputSplit split2 = new InputSplitForTest(2);
    MRSplitProto proto1 = MRInputHelpers.createSplitProto(split1);
    MRSplitProto proto2 = MRInputHelpers.createSplitProto(split2);
    MRSplitsProto.Builder splitsProtoBuilder = MRSplitsProto.newBuilder();
    splitsProtoBuilder.addSplits(proto1);
    splitsProtoBuilder.addSplits(proto2);
    MRInputUserPayloadProto.Builder payloadProto = MRInputUserPayloadProto.newBuilder();
    payloadProto.setSplits(splitsProtoBuilder.build());
    payloadProto.setConfigurationBytes(confByteString);
    UserPayload userPayload =
        UserPayload.create(payloadProto.build().toByteString().asReadOnlyByteBuffer());

    InputInitializerContext context = new TezTestUtils.TezRootInputInitializerContextForTest(userPayload);
    MRInputSplitDistributor splitDist = new MRInputSplitDistributor(context);

    List<Event> events = splitDist.initialize();

    assertEquals(3, events.size());
    assertTrue(events.get(0) instanceof InputUpdatePayloadEvent);
    assertTrue(events.get(1) instanceof InputDataInformationEvent);
    assertTrue(events.get(2) instanceof InputDataInformationEvent);

    InputDataInformationEvent diEvent1 = (InputDataInformationEvent) (events.get(1));
    InputDataInformationEvent diEvent2 = (InputDataInformationEvent) (events.get(2));

    assertNull(diEvent1.getDeserializedUserPayload());
    assertNull(diEvent2.getDeserializedUserPayload());

    assertNotNull(diEvent1.getUserPayload());
    assertNotNull(diEvent2.getUserPayload());

    MRSplitProto event1Proto = MRSplitProto.parseFrom(ByteString.copyFrom(diEvent1.getUserPayload()));
    InputSplit is1 = MRInputUtils.getOldSplitDetailsFromEvent(event1Proto, new Configuration());
    assertTrue(is1 instanceof InputSplitForTest);
    assertEquals(1, ((InputSplitForTest) is1).identifier);

    MRSplitProto event2Proto = MRSplitProto.parseFrom(ByteString.copyFrom(diEvent2.getUserPayload()));
    InputSplit is2 = MRInputUtils.getOldSplitDetailsFromEvent(event2Proto, new Configuration());
    assertTrue(is2 instanceof InputSplitForTest);
    assertEquals(2, ((InputSplitForTest) is2).identifier);
  }

  @Test(timeout = 5000)
  public void testDeserializedPayload() throws IOException {

    Configuration conf = new Configuration(false);
    conf.setBoolean(MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD, false);
    ByteString confByteString = TezUtils.createByteStringFromConf(conf);
    InputSplit split1 = new InputSplitForTest(1);
    InputSplit split2 = new InputSplitForTest(2);
    MRSplitProto proto1 = MRInputHelpers.createSplitProto(split1);
    MRSplitProto proto2 = MRInputHelpers.createSplitProto(split2);
    MRSplitsProto.Builder splitsProtoBuilder = MRSplitsProto.newBuilder();
    splitsProtoBuilder.addSplits(proto1);
    splitsProtoBuilder.addSplits(proto2);
    MRInputUserPayloadProto.Builder payloadProto = MRInputUserPayloadProto.newBuilder();
    payloadProto.setSplits(splitsProtoBuilder.build());
    payloadProto.setConfigurationBytes(confByteString);
    UserPayload userPayload =
        UserPayload.create(payloadProto.build().toByteString().asReadOnlyByteBuffer());

    InputInitializerContext context = new TezTestUtils.TezRootInputInitializerContextForTest(userPayload);
    MRInputSplitDistributor splitDist = new MRInputSplitDistributor(context);

    List<Event> events = splitDist.initialize();

    assertEquals(3, events.size());
    assertTrue(events.get(0) instanceof InputUpdatePayloadEvent);
    assertTrue(events.get(1) instanceof InputDataInformationEvent);
    assertTrue(events.get(2) instanceof InputDataInformationEvent);

    InputDataInformationEvent diEvent1 = (InputDataInformationEvent) (events.get(1));
    InputDataInformationEvent diEvent2 = (InputDataInformationEvent) (events.get(2));

    assertNull(diEvent1.getUserPayload());
    assertNull(diEvent2.getUserPayload());

    assertNotNull(diEvent1.getDeserializedUserPayload());
    assertNotNull(diEvent2.getDeserializedUserPayload());

    assertTrue(diEvent1.getDeserializedUserPayload() instanceof InputSplitForTest);
    assertEquals(1, ((InputSplitForTest) diEvent1.getDeserializedUserPayload()).identifier);

    assertTrue(diEvent2.getDeserializedUserPayload() instanceof InputSplitForTest);
    assertEquals(2, ((InputSplitForTest) diEvent2.getDeserializedUserPayload()).identifier);
  }

  @Private
  private static class InputSplitForTest implements InputSplit {

    private int identifier;

    @SuppressWarnings("unused")
    public InputSplitForTest() {
      // For writable
    }

    public InputSplitForTest(int identifier) {
      this.identifier = identifier;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(identifier);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      identifier = in.readInt();
    }

    @Override
    public long getLength() throws IOException {
      return 1000;
    }

    @Override
    public String[] getLocations() throws IOException {
      return null;
    }

  }
}
