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

package org.apache.tez.dag.api;

import java.io.IOException;

import java.nio.ByteBuffer;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.junit.Assert;
import org.junit.Test;

public class TestDagTypeConverters {

  @Test
  public void testTezEntityDescriptorSerialization() throws IOException {
    UserPayload payload = UserPayload.create(ByteBuffer.wrap(new String("Foobar").getBytes()), 100);
    String historytext = "Bar123";
    EntityDescriptor entityDescriptor =
        InputDescriptor.create("inputClazz").setUserPayload(payload)
        .setHistoryText(historytext);
    TezEntityDescriptorProto proto =
        DagTypeConverters.convertToDAGPlan(entityDescriptor);
    Assert.assertEquals(payload.getVersion(), proto.getVersion());
    Assert.assertArrayEquals(payload.deepCopyAsArray(), proto.getUserPayload().toByteArray());
    Assert.assertTrue(proto.hasHistoryText());
    Assert.assertNotEquals(historytext, proto.getHistoryText());
    Assert.assertEquals(historytext, new String(
        TezCommonUtils.decompressByteStringToByteArray(proto.getHistoryText())));

    // Ensure that the history text is not deserialized
    InputDescriptor inputDescriptor =
        DagTypeConverters.convertInputDescriptorFromDAGPlan(proto);
    Assert.assertNull(inputDescriptor.getHistoryText());
  }

}
