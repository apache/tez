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
import java.util.BitSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.UserPayload;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestTezUtils {

  @Test (timeout=2000)
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

  @Test (timeout=2000)
  public void testPayloadToAndFromConf() throws IOException {
    Configuration conf = getConf();
    Assert.assertEquals(conf.size(), 6);
    UserPayload bConf = TezUtils.createUserPayloadFromConf(conf);
    conf.clear();
    Assert.assertEquals(conf.size(), 0);
    conf = TezUtils.createConfFromUserPayload(bConf);
    Assert.assertEquals(conf.size(), 6);
    checkConf(conf);
  }

  @Test (timeout=2000)
  public void testCleanVertexName() {
    String testString = "special characters & spaces and longer than "
        + TezUtilsInternal.MAX_VERTEX_NAME_LENGTH + " characters";
    Assert.assertTrue(testString.length() > TezUtilsInternal.MAX_VERTEX_NAME_LENGTH);
    String cleaned = TezUtilsInternal.cleanVertexName(testString);
    Assert.assertTrue(cleaned.length() <= TezUtilsInternal.MAX_VERTEX_NAME_LENGTH);
    Assert.assertFalse(cleaned.contains("\\s+"));
    Assert.assertTrue(cleaned.matches("\\w+"));
  }

  @Test (timeout=2000)
  public void testBitSetToByteArray() {
    BitSet bitSet = createBitSet(0);
    byte[] bytes = TezUtilsInternal.toByteArray(bitSet);
    Assert.assertTrue(bytes.length == ((bitSet.length() / 8) + 1));

    bitSet = createBitSet(1000);
    bytes = TezUtilsInternal.toByteArray(bitSet);
    Assert.assertTrue(bytes.length == ((bitSet.length() / 8) + 1));
  }

  @Test (timeout=2000)
  public void testBitSetFromByteArray() {
    BitSet bitSet = createBitSet(0);
    byte[] bytes = TezUtilsInternal.toByteArray(bitSet);
    Assert.assertEquals(TezUtilsInternal.fromByteArray(bytes).cardinality(), bitSet.cardinality());
    Assert.assertTrue(TezUtilsInternal.fromByteArray(bytes).equals(bitSet));

    bitSet = createBitSet(1);
    bytes = TezUtilsInternal.toByteArray(bitSet);
    Assert.assertEquals(TezUtilsInternal.fromByteArray(bytes).cardinality(), bitSet.cardinality());
    Assert.assertTrue(TezUtilsInternal.fromByteArray(bytes).equals(bitSet));
    
    bitSet = createBitSet(1000);
    bytes = TezUtilsInternal.toByteArray(bitSet);
    Assert.assertEquals(TezUtilsInternal.fromByteArray(bytes).cardinality(), bitSet.cardinality());
    Assert.assertTrue(TezUtilsInternal.fromByteArray(bytes).equals(bitSet));
  }

  @Test (timeout=2000)
  public void testBitSetConversion() {
    for (int i = 0 ; i < 16 ; i++) {
      BitSet bitSet = createBitSetWithSingleEntry(i);
      byte[] bytes = TezUtilsInternal.toByteArray(bitSet);
      
      BitSet deseraialized = TezUtilsInternal.fromByteArray(bytes);
      Assert.assertEquals(bitSet, deseraialized);
      Assert.assertEquals(bitSet.cardinality(), deseraialized.cardinality());
      Assert.assertEquals(1, deseraialized.cardinality());
    }
  }

  private BitSet createBitSet(int size) {
    BitSet bitSet = new BitSet();
    int bitsToEnable = (int) (size * 0.1);
    Random rnd = new Random();
    for(int i = 0;i < bitsToEnable;i++) {
      bitSet.set(rnd.nextInt(size));
    }
    return bitSet;
  }

  private BitSet createBitSetWithSingleEntry(int bitToSet) {
    BitSet bitSet = new BitSet();
    bitSet.set(bitToSet);
    return bitSet;
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

  private void checkJSONConfigObj(JSONObject confObject) throws JSONException {
    Assert.assertNotNull(confObject);
    Assert.assertEquals("value1", confObject.getString("test1"));
    Assert.assertEquals("true", confObject.getString("test2"));
    Assert.assertEquals("1.2345", confObject.getString("test3"));
    Assert.assertEquals("34567", confObject.getString("test4"));
    Assert.assertEquals("1234567890", confObject.getString("test5"));
    Assert.assertEquals("S1,S2,S3", confObject.getString("test6"));
  }

  @Test (timeout=2000)
  public void testConvertToHistoryText() throws JSONException {
    Configuration conf = getConf();

    String confToJson = TezUtils.convertToHistoryText(conf);

    JSONObject jsonObject = new JSONObject(confToJson);

    Assert.assertFalse(jsonObject.has(ATSConstants.DESCRIPTION));
    Assert.assertTrue(jsonObject.has(ATSConstants.CONFIG));

    JSONObject confObject = jsonObject.getJSONObject(ATSConstants.CONFIG);
    checkJSONConfigObj(confObject);

    String desc = "desc123";
    confToJson = TezUtils.convertToHistoryText(desc, conf);
    jsonObject = new JSONObject(confToJson);

    Assert.assertTrue(jsonObject.has(ATSConstants.DESCRIPTION));
    String descFromJson = jsonObject.getString(ATSConstants.DESCRIPTION);
    Assert.assertEquals(desc, descFromJson);

    Assert.assertTrue(jsonObject.has(ATSConstants.CONFIG));
    confObject = jsonObject.getJSONObject("config");
    checkJSONConfigObj(confObject);

  }

  @Test (timeout=2000)
  public void testConvertToHistoryTextWithReplaceVars() throws JSONException {
    Configuration conf = getConf();
    conf.set("user", "user1");
    conf.set("location", "/tmp/${user}/");

    String location = "/tmp/user1/";
    Assert.assertEquals(location, conf.get("location"));

    String confToJson = TezUtils.convertToHistoryText(conf);

    JSONObject jsonObject = new JSONObject(confToJson);

    Assert.assertFalse(jsonObject.has(ATSConstants.DESCRIPTION));
    Assert.assertTrue(jsonObject.has(ATSConstants.CONFIG));

    JSONObject confObject = jsonObject.getJSONObject(ATSConstants.CONFIG);
    checkJSONConfigObj(confObject);
    Assert.assertEquals("user1", confObject.getString("user"));
    Assert.assertEquals(location, confObject.getString("location"));

    String desc = "desc123";
    confToJson = TezUtils.convertToHistoryText(desc, conf);
    jsonObject = new JSONObject(confToJson);

    Assert.assertTrue(jsonObject.has(ATSConstants.DESCRIPTION));
    String descFromJson = jsonObject.getString(ATSConstants.DESCRIPTION);
    Assert.assertEquals(desc, descFromJson);

    Assert.assertTrue(jsonObject.has(ATSConstants.CONFIG));
    confObject = jsonObject.getJSONObject("config");
    checkJSONConfigObj(confObject);
    Assert.assertEquals("user1", confObject.getString("user"));
    Assert.assertEquals(location, confObject.getString("location"));

  }


}
