/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestTezUtils {
  @Test
  public void testByteStringToAndFromConf() throws IOException {
    Configuration conf = getConf();
    Assert.assertEquals(conf.size(), 6);
    ByteString bsConf = TezUtils.createByteStringFromConf(conf);
    conf.clear();
    Assert.assertEquals(conf.size(), 0);
    conf = TezUtils.createConfFromByteString(bsConf);
    Assert.assertEquals(conf.size(), 6);
    checkConf(conf);
  }

  @Test
  public void testPayloadToAndFromConf() throws IOException {
    Configuration conf = getConf();
    Assert.assertEquals(conf.size(), 6);
    byte[] bConf = TezUtils.createUserPayloadFromConf(conf);
    conf.clear();
    Assert.assertEquals(conf.size(), 0);
    conf = TezUtils.createConfFromUserPayload(bConf);
    Assert.assertEquals(conf.size(), 6);
    checkConf(conf);
  }
  
  @Test
  public void testCleanVertexName() {
    String testString = "special characters & spaces and longer than "
        + TezUtils.MAX_VERTEX_NAME_LENGTH + " characters";
    Assert.assertTrue(testString.length() > TezUtils.MAX_VERTEX_NAME_LENGTH);
    String cleaned = TezUtils.cleanVertexName(testString);
    Assert.assertTrue(cleaned.length() <= TezUtils.MAX_VERTEX_NAME_LENGTH);
    Assert.assertFalse(cleaned.contains("\\s+"));
    Assert.assertTrue(cleaned.matches("\\w+"));
  }

  private Configuration getConf() {
    Configuration conf = new Configuration(false);
    conf.set("test1", "value1");
    conf.setBoolean("test2", true);
    conf.setDouble("test3", 1.2345);
    conf.setInt("test4", 34567);
    conf.setLong("test5", 1234567890L);
    conf.setStrings("test6", "S1", "S2", "S3");
    return conf;
  }

  private void checkConf(Configuration conf) {
    Assert.assertEquals(conf.get("test1"), "value1");
    Assert.assertTrue(conf.getBoolean("test2", false));
    Assert.assertEquals(conf.getDouble("test3", 0), 1.2345, 1e-15);
    Assert.assertEquals(conf.getInt("test4", 0), 34567);
    Assert.assertEquals(conf.getLong("test5", 0), 1234567890L);
    String tmp[] = conf.getStrings("test6");
    Assert.assertEquals(tmp.length, 3);
    Assert.assertEquals(tmp[0], "S1");
    Assert.assertEquals(tmp[1], "S2");
    Assert.assertEquals(tmp[2], "S3");

  }
}
