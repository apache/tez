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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.common.TezUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;

public class TestEntityDescriptor {

  public void verifyResults(InputDescriptor entityDescriptor, InputDescriptor deserialized, UserPayload payload,
                             String confVal) throws IOException {
    Assert.assertEquals(entityDescriptor.getClassName(), deserialized.getClassName());
    // History text is not serialized when sending to tasks
    Assert.assertNull(deserialized.getHistoryText());
    Assert.assertArrayEquals(payload.deepCopyAsArray(), deserialized.getUserPayload().deepCopyAsArray());
    Configuration deserializedConf = TezUtils.createConfFromUserPayload(deserialized.getUserPayload());
    Assert.assertEquals(confVal, deserializedConf.get("testKey"));
  }

  public void testSingularWrite(InputDescriptor entityDescriptor, InputDescriptor deserialized, UserPayload payload,
                                String confVal) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    entityDescriptor.write(out);
    out.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream(out.getData().length);
    bos.write(out.getData());

    Mockito.verify(entityDescriptor).writeSingular(eq(out), any(ByteBuffer.class));
    deserialized.readFields(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
    verifyResults(entityDescriptor, deserialized, payload, confVal);
  }

  public void testSegmentedWrite(InputDescriptor entityDescriptor, InputDescriptor deserialized, UserPayload payload,
                                 String confVal) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bos);
    entityDescriptor.write(out);
    out.close();

    Mockito.verify(entityDescriptor).writeSegmented(eq(out), any(ByteBuffer.class));
    deserialized.readFields(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
    verifyResults(entityDescriptor, deserialized, payload, confVal);
  }

  @Test (timeout=1000)
  public void testEntityDescriptorHadoopSerialization() throws IOException {
     /* This tests the alternate serialization code path
     * if the DataOutput is not DataOutputBuffer
     * AND, if it indeed is, with a read/write payload */
    Configuration conf = new Configuration(true);
    String confVal = RandomStringUtils.random(10000, true, true);
    conf.set("testKey", confVal);
    UserPayload payload = TezUtils.createUserPayloadFromConf(conf);

    InputDescriptor deserialized = InputDescriptor.create("dummy");
    InputDescriptor entityDescriptor =
        InputDescriptor.create("inputClazz").setUserPayload(payload)
                .setHistoryText("Bar123");
    InputDescriptor entityDescriptorLivingInFear = spy(entityDescriptor);

    testSingularWrite(entityDescriptorLivingInFear, deserialized, payload, confVal);

    /* make read-only payload */
    payload =  UserPayload.create(payload.getPayload());
    entityDescriptor = InputDescriptor.create("inputClazz").setUserPayload(payload)
                      .setHistoryText("Bar123");
    entityDescriptorLivingInFear = spy(entityDescriptor);
    testSegmentedWrite(entityDescriptorLivingInFear, deserialized, payload, confVal);
  }
}
