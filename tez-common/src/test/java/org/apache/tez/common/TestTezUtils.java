/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;

import com.google.protobuf.ByteString;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestTezUtils {

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testByteStringToAndFromConf() throws IOException {
    Configuration conf = getConf();
    assertEquals(conf.size(), 6);
    ByteString bsConf = TezUtils.createByteStringFromConf(conf);
    conf.clear();
    assertEquals(conf.size(), 0);
    conf = TezUtils.createConfFromByteString(bsConf);
    assertEquals(conf.size(), 6);
    checkConf(conf);
  }

  private String constructLargeValue() {
    int largeSizeMinimum = 64 * 1024 * 1024;
    final String alphaString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int largeSize = (largeSizeMinimum + alphaString.length() - 1) / alphaString.length();
    largeSize *= alphaString.length();
    assertTrue(largeSize >= alphaString.length());
    StringBuilder sb = new StringBuilder(largeSize);

    while (sb.length() < largeSize) {
      sb.append(alphaString);
    }

    String largeValue = sb.toString();
    assertEquals(largeSize, largeValue.length());
    return largeValue;
  }

  private ByteString createByteString(Configuration conf, String largeValue) throws IOException {
    conf.set("testLargeValue", largeValue);
    assertEquals(conf.size(), 7);
    return TezUtils.createByteStringFromConf(conf);
  }

  @Test
  @Timeout(value = 20000, unit = TimeUnit.MILLISECONDS)
  public void testByteStringToAndFromLargeConf() throws IOException {
    Configuration conf = getConf();
    String largeValue = constructLargeValue();
    ByteString bsConf = createByteString(conf, largeValue);
    conf.clear();
    assertEquals(conf.size(), 0);
    conf = TezUtils.createConfFromByteString(bsConf);
    assertEquals(conf.size(), 7);
    checkConf(conf);
    assertEquals(conf.get("testLargeValue"), largeValue);
  }

  @Test
  @Timeout(value = 20000, unit = TimeUnit.MILLISECONDS)
  public void testByteStringAddToLargeConf() throws IOException {
    Configuration conf = getConf();
    String largeValue = constructLargeValue();
    ByteString bsConf = createByteString(conf, largeValue);
    conf.clear();
    assertEquals(conf.size(), 0);
    TezUtils.addToConfFromByteString(conf, bsConf);
    assertEquals(conf.size(), 7);
    checkConf(conf);
    assertEquals(conf.get("testLargeValue"), largeValue);
  }

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testPayloadToAndFromConf() throws IOException {
    Configuration conf = getConf();
    assertEquals(conf.size(), 6);
    UserPayload bConf = TezUtils.createUserPayloadFromConf(conf);
    conf.clear();
    assertEquals(conf.size(), 0);
    conf = TezUtils.createConfFromUserPayload(bConf);
    assertEquals(conf.size(), 6);
    checkConf(conf);
  }

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testCleanVertexName() {
    String testString = "special characters & spaces and longer than "
        + TezUtilsInternal.MAX_VERTEX_NAME_LENGTH + " characters";
    assertTrue(testString.length() > TezUtilsInternal.MAX_VERTEX_NAME_LENGTH);
    String cleaned = TezUtilsInternal.cleanVertexName(testString);
    assertTrue(cleaned.length() <= TezUtilsInternal.MAX_VERTEX_NAME_LENGTH);
    assertFalse(cleaned.contains("\\s+"));
    assertTrue(cleaned.matches("\\w+"));
  }

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testBitSetToByteArray() {
    BitSet bitSet = createBitSet(0);
    byte[] bytes = TezUtilsInternal.toByteArray(bitSet);
    assertEquals(bytes.length, (bitSet.length() + 7) / 8);

    bitSet = createBitSet(1000);
    bytes = TezUtilsInternal.toByteArray(bitSet);
    assertEquals(bytes.length, (bitSet.length() + 7) / 8);
  }

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testBitSetFromByteArray() {
    BitSet bitSet = createBitSet(0);
    byte[] bytes = TezUtilsInternal.toByteArray(bitSet);
    assertEquals(TezUtilsInternal.fromByteArray(bytes).cardinality(), bitSet.cardinality());
    assertEquals(TezUtilsInternal.fromByteArray(bytes), bitSet);

    bitSet = createBitSet(1);
    bytes = TezUtilsInternal.toByteArray(bitSet);
    assertEquals(TezUtilsInternal.fromByteArray(bytes).cardinality(), bitSet.cardinality());
    assertEquals(TezUtilsInternal.fromByteArray(bytes), bitSet);

    bitSet = createBitSet(1000);
    bytes = TezUtilsInternal.toByteArray(bitSet);
    assertEquals(TezUtilsInternal.fromByteArray(bytes).cardinality(), bitSet.cardinality());
    assertEquals(TezUtilsInternal.fromByteArray(bytes), bitSet);
  }

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testBitSetConversion() {
    for (int i = 0 ; i < 16 ; i++) {
      BitSet bitSet = createBitSetWithSingleEntry(i);
      byte[] bytes = TezUtilsInternal.toByteArray(bitSet);

      BitSet deseraialized = TezUtilsInternal.fromByteArray(bytes);
      assertEquals(bitSet, deseraialized);
      assertEquals(bitSet.cardinality(), deseraialized.cardinality());
      assertEquals(1, deseraialized.cardinality());
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
    assertEquals(conf.get("test1"), "value1");
    assertTrue(conf.getBoolean("test2", false));
    assertEquals(conf.getDouble("test3", 0), 1.2345, 1e-15);
    assertEquals(conf.getInt("test4", 0), 34567);
    assertEquals(conf.getLong("test5", 0), 1234567890L);
    String tmp[] = conf.getStrings("test6");
    assertEquals(tmp.length, 3);
    assertEquals(tmp[0], "S1");
    assertEquals(tmp[1], "S2");
    assertEquals(tmp[2], "S3");

  }

  private void checkJSONConfigObj(JSONObject confObject) throws JSONException {
    assertNotNull(confObject);
    assertEquals("value1", confObject.getString("test1"));
    assertEquals("true", confObject.getString("test2"));
    assertEquals("1.2345", confObject.getString("test3"));
    assertEquals("34567", confObject.getString("test4"));
    assertEquals("1234567890", confObject.getString("test5"));
    assertEquals("S1,S2,S3", confObject.getString("test6"));
  }

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testConvertToHistoryText() throws JSONException {
    Configuration conf = getConf();

    String confToJson = TezUtils.convertToHistoryText(conf);

    JSONObject jsonObject = new JSONObject(confToJson);

    assertFalse(jsonObject.has(ATSConstants.DESC));
    assertTrue(jsonObject.has(ATSConstants.CONFIG));

    JSONObject confObject = jsonObject.getJSONObject(ATSConstants.CONFIG);
    checkJSONConfigObj(confObject);

    String desc = "desc123";
    confToJson = TezUtils.convertToHistoryText(desc, conf);
    jsonObject = new JSONObject(confToJson);

    assertTrue(jsonObject.has(ATSConstants.DESC));
    String descFromJson = jsonObject.getString(ATSConstants.DESC);
    assertEquals(desc, descFromJson);

    assertTrue(jsonObject.has(ATSConstants.CONFIG));
    confObject = jsonObject.getJSONObject("config");
    checkJSONConfigObj(confObject);

  }

  @Test
  @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
  public void testConvertToHistoryTextWithReplaceVars() throws JSONException {
    Configuration conf = getConf();
    conf.set("user", "user1");
    conf.set("location", "/tmp/${user}/");

    String location = "/tmp/user1/";
    assertEquals(location, conf.get("location"));

    String confToJson = TezUtils.convertToHistoryText(conf);

    JSONObject jsonObject = new JSONObject(confToJson);

    assertFalse(jsonObject.has(ATSConstants.DESC));
    assertTrue(jsonObject.has(ATSConstants.CONFIG));

    JSONObject confObject = jsonObject.getJSONObject(ATSConstants.CONFIG);
    checkJSONConfigObj(confObject);
    assertEquals("user1", confObject.getString("user"));
    assertEquals(location, confObject.getString("location"));

    String desc = "desc123";
    confToJson = TezUtils.convertToHistoryText(desc, conf);
    jsonObject = new JSONObject(confToJson);

    assertTrue(jsonObject.has(ATSConstants.DESC));
    String descFromJson = jsonObject.getString(ATSConstants.DESC);
    assertEquals(desc, descFromJson);

    assertTrue(jsonObject.has(ATSConstants.CONFIG));
    confObject = jsonObject.getJSONObject("config");
    checkJSONConfigObj(confObject);
    assertEquals("user1", confObject.getString("user"));
    assertEquals(location, confObject.getString("location"));

  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testPopulateConfProtoFromEntries() {
      Map<String, String> map = new HashMap<>();
      map.put("nonNullKey", "value");
      map.put("nullKey", null);
      DAGProtos.ConfigurationProto.Builder confBuilder = DAGProtos.ConfigurationProto.newBuilder();
      TezUtils.populateConfProtoFromEntries(map.entrySet(), confBuilder);
      assertEquals(confBuilder.getConfKeyValuesList().size(), 1);
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testReadTezConfigurationXmlFromClasspath() throws IOException {
    InputStream is = ClassLoader.getSystemResourceAsStream(TezConfiguration.TEZ_SITE_XML);
    Configuration conf = TezUtilsInternal.readTezConfigurationXml(is);
    assertEquals("tez.tar.gz", conf.get("tez.lib.uris"));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testPluginsDescriptorFromJSON() throws IOException {
    InputStream is = ClassLoader.getSystemResourceAsStream(TezConstants.SERVICE_PLUGINS_DESCRIPTOR_JSON);
    ServicePluginsDescriptor spd = TezClientUtils.createPluginsDescriptorFromJSON(is);
    TaskSchedulerDescriptor tsd = spd.getTaskSchedulerDescriptors()[0];
    ContainerLauncherDescriptor cld = spd.getContainerLauncherDescriptors()[0];
    TaskCommunicatorDescriptor tcd = spd.getTaskCommunicatorDescriptors()[0];

    assertFalse(spd.areContainersEnabled());
    assertTrue(spd.isUberEnabled());
    assertEquals("testScheduler0_class", tsd.getClassName());
    assertEquals("testScheduler0", tsd.getEntityName());
    assertEquals("testLauncher0_class", cld.getClassName());
    assertEquals("testLauncher0", cld.getEntityName());
    assertEquals("testComm0_class", tcd.getClassName());
    assertEquals("testComm0", tcd.getEntityName());
    assertEquals(1, tcd.getUserPayload().getVersion());
    assertArrayEquals(new byte[] {0, 0, 0, 1}, tcd.getUserPayload().deepCopyAsArray());
  }
}
