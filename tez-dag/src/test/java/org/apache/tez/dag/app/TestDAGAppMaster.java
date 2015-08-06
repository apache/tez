/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.TezNamedEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.TezUserPayloadProto;
import org.junit.Test;

public class TestDAGAppMaster {

  private static final String TEST_KEY = "TEST_KEY";
  private static final String TEST_VAL = "TEST_VAL";
  private static final String TS_NAME = "TS";
  private static final String CL_NAME = "CL";
  private static final String TC_NAME = "TC";
  private static final String CLASS_SUFFIX = "_CLASS";

  @Test(timeout = 5000)
  public void testPluginParsing() throws IOException {
    BiMap<String, Integer> pluginMap = HashBiMap.create();
    Configuration conf = new Configuration(false);
    conf.set("testkey", "testval");
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    List<TezNamedEntityDescriptorProto> entityDescriptors = new LinkedList<>();
    List<NamedEntityDescriptor> entities;

    // Test empty descriptor list, yarn enabled
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, null, true, false, defaultPayload);
    assertEquals(1, pluginMap.size());
    assertEquals(1, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezYarnServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezYarnServicePluginName()));
    assertEquals("testval",
        TezUtils.createConfFromUserPayload(entities.get(0).getUserPayload()).get("testkey"));

    // Test empty descriptor list, uber enabled
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, null, false, true, defaultPayload);
    assertEquals(1, pluginMap.size());
    assertEquals(1, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezUberServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezUberServicePluginName()));
    assertEquals("testval",
        TezUtils.createConfFromUserPayload(entities.get(0).getUserPayload()).get("testkey"));

    // Test empty descriptor list, yarn enabled, uber enabled
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, null, true, true, defaultPayload);
    assertEquals(2, pluginMap.size());
    assertEquals(2, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezYarnServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezYarnServicePluginName()));
    assertTrue(pluginMap.containsKey(TezConstants.getTezUberServicePluginName()));
    assertTrue(1 == pluginMap.get(TezConstants.getTezUberServicePluginName()));


    String pluginName = "d1";
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    TezNamedEntityDescriptorProto d1 =
        TezNamedEntityDescriptorProto.newBuilder().setName(pluginName).setEntityDescriptor(
            DAGProtos.TezEntityDescriptorProto.newBuilder().setClassName("d1Class")
                .setTezUserPayload(
                    TezUserPayloadProto.newBuilder()
                        .setUserPayload(ByteString.copyFrom(bb)))).build();
    entityDescriptors.add(d1);

    // Test descriptor, no yarn, no uber
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, entityDescriptors, false, false, defaultPayload);
    assertEquals(1, pluginMap.size());
    assertEquals(1, entities.size());
    assertTrue(pluginMap.containsKey(pluginName));
    assertTrue(0 == pluginMap.get(pluginName));

    // Test descriptor, yarn and uber
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, entityDescriptors, true, true, defaultPayload);
    assertEquals(3, pluginMap.size());
    assertEquals(3, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezYarnServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezYarnServicePluginName()));
    assertTrue(pluginMap.containsKey(TezConstants.getTezUberServicePluginName()));
    assertTrue(1 == pluginMap.get(TezConstants.getTezUberServicePluginName()));
    assertTrue(pluginMap.containsKey(pluginName));
    assertTrue(2 == pluginMap.get(pluginName));
    entityDescriptors.clear();
  }


  @Test(timeout = 5000)
  public void testParseAllPluginsNoneSpecified() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TEST_KEY, TEST_VAL);
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    List<NamedEntityDescriptor> tsDescriptors;
    BiMap<String, Integer> tsMap;
    List<NamedEntityDescriptor> clDescriptors;
    BiMap<String, Integer> clMap;
    List<NamedEntityDescriptor> tcDescriptors;
    BiMap<String, Integer> tcMap;


    // No plugins. Non local
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        null, false, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 1, true, TezConstants.getTezYarnServicePluginName());
    verifyDescAndMap(clDescriptors, clMap, 1, true, TezConstants.getTezYarnServicePluginName());
    verifyDescAndMap(tcDescriptors, tcMap, 1, true, TezConstants.getTezYarnServicePluginName());

    // No plugins. Local
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        null, true, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 1, true, TezConstants.getTezUberServicePluginName());
    verifyDescAndMap(clDescriptors, clMap, 1, true, TezConstants.getTezUberServicePluginName());
    verifyDescAndMap(tcDescriptors, tcMap, 1, true, TezConstants.getTezUberServicePluginName());
  }

  @Test(timeout = 5000)
  public void testParseAllPluginsOnlyCustomSpecified() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TEST_KEY, TEST_VAL);
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);
    TezUserPayloadProto payloadProto = TezUserPayloadProto.newBuilder()
        .setUserPayload(ByteString.copyFrom(defaultPayload.getPayload())).build();

    AMPluginDescriptorProto proto = createAmPluginDescriptor(false, false, true, payloadProto);

    List<NamedEntityDescriptor> tsDescriptors;
    BiMap<String, Integer> tsMap;
    List<NamedEntityDescriptor> clDescriptors;
    BiMap<String, Integer> clMap;
    List<NamedEntityDescriptor> tcDescriptors;
    BiMap<String, Integer> tcMap;


    // Only plugin, Yarn.
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        proto, false, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 2, true, TS_NAME,
        TezConstants.getTezYarnServicePluginName());
    verifyDescAndMap(clDescriptors, clMap, 1, true, CL_NAME);
    verifyDescAndMap(tcDescriptors, tcMap, 1, true, TC_NAME);
    assertEquals(TS_NAME + CLASS_SUFFIX, tsDescriptors.get(0).getClassName());
    assertEquals(CL_NAME + CLASS_SUFFIX, clDescriptors.get(0).getClassName());
    assertEquals(TC_NAME + CLASS_SUFFIX, tcDescriptors.get(0).getClassName());
  }

  @Test(timeout = 5000)
  public void testParseAllPluginsCustomAndYarnSpecified() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TEST_KEY, TEST_VAL);
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);
    TezUserPayloadProto payloadProto = TezUserPayloadProto.newBuilder()
        .setUserPayload(ByteString.copyFrom(defaultPayload.getPayload())).build();

    AMPluginDescriptorProto proto = createAmPluginDescriptor(true, false, true, payloadProto);

    List<NamedEntityDescriptor> tsDescriptors;
    BiMap<String, Integer> tsMap;
    List<NamedEntityDescriptor> clDescriptors;
    BiMap<String, Integer> clMap;
    List<NamedEntityDescriptor> tcDescriptors;
    BiMap<String, Integer> tcMap;


    // Only plugin, Yarn.
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        proto, false, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 2, true, TezConstants.getTezYarnServicePluginName(),
        TS_NAME);
    verifyDescAndMap(clDescriptors, clMap, 2, true, TezConstants.getTezYarnServicePluginName(),
        CL_NAME);
    verifyDescAndMap(tcDescriptors, tcMap, 2, true, TezConstants.getTezYarnServicePluginName(),
        TC_NAME);
    assertNull(tsDescriptors.get(0).getClassName());
    assertNull(clDescriptors.get(0).getClassName());
    assertNull(tcDescriptors.get(0).getClassName());
    assertEquals(TS_NAME + CLASS_SUFFIX, tsDescriptors.get(1).getClassName());
    assertEquals(CL_NAME + CLASS_SUFFIX, clDescriptors.get(1).getClassName());
    assertEquals(TC_NAME + CLASS_SUFFIX, tcDescriptors.get(1).getClassName());
  }

  private void verifyDescAndMap(List<NamedEntityDescriptor> descriptors, BiMap<String, Integer> map,
                                int numExpected, boolean verifyPayload,
                                String... expectedNames) throws
      IOException {
    Preconditions.checkArgument(expectedNames.length == numExpected);
    assertEquals(numExpected, descriptors.size());
    assertEquals(numExpected, map.size());
    for (int i = 0; i < numExpected; i++) {
      assertEquals(expectedNames[i], descriptors.get(i).getEntityName());
      if (verifyPayload) {
        assertEquals(TEST_VAL,
            TezUtils.createConfFromUserPayload(descriptors.get(0).getUserPayload()).get(TEST_KEY));
      }
      assertTrue(map.get(expectedNames[i]) == i);
      assertTrue(map.inverse().get(i) == expectedNames[i]);
    }
  }

  private AMPluginDescriptorProto createAmPluginDescriptor(boolean enableYarn, boolean enableUber,
                                                           boolean addCustom,
                                                           TezUserPayloadProto payloadProto) {
    AMPluginDescriptorProto.Builder builder = AMPluginDescriptorProto.newBuilder()
        .setUberEnabled(enableUber)
        .setContainersEnabled(enableYarn);
    if (addCustom) {
      builder.addTaskSchedulers(
          TezNamedEntityDescriptorProto.newBuilder()
              .setName(TS_NAME)
              .setEntityDescriptor(
                  DAGProtos.TezEntityDescriptorProto.newBuilder()
                      .setClassName(TS_NAME + CLASS_SUFFIX)
                      .setTezUserPayload(payloadProto)))
          .addContainerLaunchers(
              TezNamedEntityDescriptorProto.newBuilder()
                  .setName(CL_NAME)
                  .setEntityDescriptor(
                      DAGProtos.TezEntityDescriptorProto.newBuilder()
                          .setClassName(CL_NAME + CLASS_SUFFIX)
                          .setTezUserPayload(payloadProto)))
          .addTaskCommunicators(
              TezNamedEntityDescriptorProto.newBuilder()
                  .setName(TC_NAME)
                  .setEntityDescriptor(
                      DAGProtos.TezEntityDescriptorProto.newBuilder()
                          .setClassName(TC_NAME + CLASS_SUFFIX)
                          .setTezUserPayload(payloadProto)));
    }
    return builder.build();
  }


}
