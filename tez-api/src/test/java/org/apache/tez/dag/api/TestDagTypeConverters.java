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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.junit.Assert;
import org.junit.Test;

public class TestDagTypeConverters {

  @Test(timeout = 5000)
  public void testTezEntityDescriptorSerialization() throws IOException {
    UserPayload payload = UserPayload.create(ByteBuffer.wrap(new String("Foobar").getBytes()), 100);
    String historytext = "Bar123";
    EntityDescriptor entityDescriptor =
        InputDescriptor.create("inputClazz").setUserPayload(payload)
        .setHistoryText(historytext);
    TezEntityDescriptorProto proto =
        DagTypeConverters.convertToDAGPlan(entityDescriptor);
    Assert.assertEquals(payload.getVersion(), proto.getTezUserPayload().getVersion());
    Assert.assertArrayEquals(payload.deepCopyAsArray(), proto.getTezUserPayload().getUserPayload().toByteArray());
    Assert.assertTrue(proto.hasHistoryText());
    Assert.assertNotEquals(historytext, proto.getHistoryText());
    Assert.assertEquals(historytext, new String(
        TezCommonUtils.decompressByteStringToByteArray(proto.getHistoryText())));

    // Ensure that the history text is not deserialized
    InputDescriptor inputDescriptor =
        DagTypeConverters.convertInputDescriptorFromDAGPlan(proto);
    Assert.assertNull(inputDescriptor.getHistoryText());

    // Check history text value
    String actualHistoryText = DagTypeConverters.getHistoryTextFromProto(proto);
    Assert.assertEquals(historytext, actualHistoryText);
  }

  @Test(timeout = 5000)
  public void testYarnPathTranslation() {
    // Without port
    String p1String = "hdfs://mycluster/file";
    Path p1Path = new Path(p1String);
    // Users would translate this via this mechanic.
    URL lr1Url = ConverterUtils.getYarnUrlFromPath(p1Path);
    // Serialize to dag plan.
    String p1StringSerialized = DagTypeConverters.convertToDAGPlan(lr1Url);
    // Deserialize
    URL lr1UrlDeserialized = DagTypeConverters.convertToYarnURL(p1StringSerialized);
    Assert.assertEquals("mycluster", lr1UrlDeserialized.getHost());
    Assert.assertEquals("/file", lr1UrlDeserialized.getFile());
    Assert.assertEquals("hdfs", lr1UrlDeserialized.getScheme());


    // With port
    String p2String = "hdfs://mycluster:2311/file";
    Path p2Path = new Path(p2String);
    // Users would translate this via this mechanic.
    URL lr2Url = ConverterUtils.getYarnUrlFromPath(p2Path);
    // Serialize to dag plan.
    String p2StringSerialized = DagTypeConverters.convertToDAGPlan(lr2Url);
    // Deserialize
    URL lr2UrlDeserialized = DagTypeConverters.convertToYarnURL(p2StringSerialized);
    Assert.assertEquals("mycluster", lr2UrlDeserialized.getHost());
    Assert.assertEquals("/file", lr2UrlDeserialized.getFile());
    Assert.assertEquals("hdfs", lr2UrlDeserialized.getScheme());
    Assert.assertEquals(2311, lr2UrlDeserialized.getPort());
  }

}
