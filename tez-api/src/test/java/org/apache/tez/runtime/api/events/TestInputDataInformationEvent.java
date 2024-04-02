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
package org.apache.tez.runtime.api.events;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestInputDataInformationEvent {

  @Test
  public void testApiPayloadOrPath() {
    InputDataInformationEvent eventWithSerializedPayload =
        InputDataInformationEvent.createWithSerializedPayload(0, ByteBuffer.wrap("payload1".getBytes()));
    // event created by createWithSerializedPayload should contain serialized payload
    // but not a path or a deserialized payload
    Assert.assertEquals("payload1", Charsets.UTF_8.decode(eventWithSerializedPayload.getUserPayload()).toString());
    Assert.assertNull(eventWithSerializedPayload.getSerializedPath());
    Assert.assertNull(eventWithSerializedPayload.getDeserializedUserPayload());

    InputDataInformationEvent eventWithObjectPayload = InputDataInformationEvent.createWithObjectPayload(0, "payload2");
    // event created by eventWithObjectPayload should contain a deserialized payload
    // but not a path or serialized payload
    Assert.assertEquals("payload2", eventWithObjectPayload.getDeserializedUserPayload());
    Assert.assertNull(eventWithObjectPayload.getSerializedPath());
    Assert.assertNull(eventWithObjectPayload.getUserPayload());

    InputDataInformationEvent eventWithPath = InputDataInformationEvent.createWithSerializedPath(0, "file://hello");
    // event created by createWithSerializedPath should contain a path
    // but neither serialized nor deserialized payload
    Assert.assertEquals("file://hello", eventWithPath.getSerializedPath());
    Assert.assertNull(eventWithPath.getUserPayload());
    Assert.assertNull(eventWithPath.getDeserializedUserPayload());
  }
}
