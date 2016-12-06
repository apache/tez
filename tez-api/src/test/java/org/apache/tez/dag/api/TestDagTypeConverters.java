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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;
import org.apache.tez.dag.api.records.DAGProtos.ACLInfo;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.TezNamedEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexExecutionContextProto;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

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
    assertTrue(proto.hasHistoryText());
    Assert.assertNotEquals(historytext, proto.getHistoryText());
    Assert.assertEquals(historytext, new String(
        TezCommonUtils.decompressByteStringToByteArray(proto.getHistoryText())));

    // Ensure that the history text is not deserialized
    InputDescriptor inputDescriptor =
        DagTypeConverters.convertInputDescriptorFromDAGPlan(proto);
    Assert.assertNull(inputDescriptor.getHistoryText());

    // Check history text value
    String actualHistoryText = DagTypeConverters.getHistoryTextFromProto(proto, TezCommonUtils.newInflater());
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


  @Test(timeout = 5000)
  public void testVertexExecutionContextTranslation() {
    VertexExecutionContext originalContext;
    VertexExecutionContextProto contextProto;
    VertexExecutionContext retrievedContext;


    // Uber
    originalContext = VertexExecutionContext.createExecuteInAm(true);
    contextProto = DagTypeConverters.convertToProto(originalContext);
    retrievedContext = DagTypeConverters.convertFromProto(contextProto);
    assertEquals(originalContext, retrievedContext);

    // Regular containers
    originalContext = VertexExecutionContext.createExecuteInContainers(true);
    contextProto = DagTypeConverters.convertToProto(originalContext);
    retrievedContext = DagTypeConverters.convertFromProto(contextProto);
    assertEquals(originalContext, retrievedContext);

    // Custom
    originalContext = VertexExecutionContext.create("plugin", "plugin", "plugin");
    contextProto = DagTypeConverters.convertToProto(originalContext);
    retrievedContext = DagTypeConverters.convertFromProto(contextProto);
    assertEquals(originalContext, retrievedContext);
  }


  static final String testScheduler = "testScheduler";
  static final String testLauncher = "testLauncher";
  static final String testComm = "testComm";
  static final String classSuffix = "_class";

  @Test(timeout = 5000)
  public void testServiceDescriptorTranslation() {


    TaskSchedulerDescriptor[] taskSchedulers;
    ContainerLauncherDescriptor[] containerLaunchers;
    TaskCommunicatorDescriptor[] taskComms;

    ServicePluginsDescriptor servicePluginsDescriptor;
    AMPluginDescriptorProto proto;

    // Uber-execution
    servicePluginsDescriptor = ServicePluginsDescriptor.create(true);
    proto = DagTypeConverters.convertServicePluginDescriptorToProto(servicePluginsDescriptor);
    assertTrue(proto.hasUberEnabled());
    assertTrue(proto.hasContainersEnabled());
    assertTrue(proto.getUberEnabled());
    assertTrue(proto.getContainersEnabled());
    assertEquals(0, proto.getTaskSchedulersCount());
    assertEquals(0, proto.getContainerLaunchersCount());
    assertEquals(0, proto.getTaskCommunicatorsCount());

    // Single plugin set specified. One with a payload.
    taskSchedulers = createTaskScheduelrs(1, false);
    containerLaunchers = createContainerLaunchers(1, false);
    taskComms = createTaskCommunicators(1, true);

    servicePluginsDescriptor = ServicePluginsDescriptor.create(taskSchedulers, containerLaunchers,
        taskComms);
    proto = DagTypeConverters.convertServicePluginDescriptorToProto(servicePluginsDescriptor);
    assertTrue(proto.hasUberEnabled());
    assertTrue(proto.hasContainersEnabled());
    assertFalse(proto.getUberEnabled());
    assertTrue(proto.getContainersEnabled());
    verifyPlugins(proto.getTaskSchedulersList(), 1, testScheduler, false);
    verifyPlugins(proto.getContainerLaunchersList(), 1, testLauncher, false);
    verifyPlugins(proto.getTaskCommunicatorsList(), 1, testComm, true);


    // Multiple plugin set specified. All with a payload
    taskSchedulers = createTaskScheduelrs(3, true);
    containerLaunchers = createContainerLaunchers(3, true);
    taskComms = createTaskCommunicators(3, true);

    servicePluginsDescriptor = ServicePluginsDescriptor.create(taskSchedulers, containerLaunchers,
        taskComms);
    proto = DagTypeConverters.convertServicePluginDescriptorToProto(servicePluginsDescriptor);
    assertTrue(proto.hasUberEnabled());
    assertTrue(proto.hasContainersEnabled());
    assertFalse(proto.getUberEnabled());
    assertTrue(proto.getContainersEnabled());
    verifyPlugins(proto.getTaskSchedulersList(), 3, testScheduler, true);
    verifyPlugins(proto.getContainerLaunchersList(), 3, testLauncher, true);
    verifyPlugins(proto.getTaskCommunicatorsList(), 3, testComm, true);

    // Single plugin set specified. One with a payload. No container execution. Uber enabled.
    taskSchedulers = createTaskScheduelrs(1, false);
    containerLaunchers = createContainerLaunchers(1, false);
    taskComms = createTaskCommunicators(1, true);

    servicePluginsDescriptor = ServicePluginsDescriptor.create(false, true, taskSchedulers, containerLaunchers,
        taskComms);
    proto = DagTypeConverters.convertServicePluginDescriptorToProto(servicePluginsDescriptor);
    assertTrue(proto.hasUberEnabled());
    assertTrue(proto.hasContainersEnabled());
    assertTrue(proto.getUberEnabled());
    assertFalse(proto.getContainersEnabled());
    verifyPlugins(proto.getTaskSchedulersList(), 1, testScheduler, false);
    verifyPlugins(proto.getContainerLaunchersList(), 1, testLauncher, false);
    verifyPlugins(proto.getTaskCommunicatorsList(), 1, testComm, true);
  }

  @Test
  public void testAclConversions() {
    DAGAccessControls dagAccessControls = new DAGAccessControls("u1,u2 g1,g2", "u3,u4 g3,g4");
    ACLInfo aclInfo = DagTypeConverters.convertDAGAccessControlsToProto(dagAccessControls);
    assertSame(dagAccessControls, aclInfo);
    assertSame(DagTypeConverters.convertDAGAccessControlsFromProto(aclInfo), aclInfo);

    dagAccessControls = new DAGAccessControls("u1 ", "u2 ");
    aclInfo = DagTypeConverters.convertDAGAccessControlsToProto(dagAccessControls);
    assertSame(dagAccessControls, aclInfo);
    assertSame(DagTypeConverters.convertDAGAccessControlsFromProto(aclInfo), aclInfo);

    dagAccessControls = new DAGAccessControls(" g1", " g3,g4");
    aclInfo = DagTypeConverters.convertDAGAccessControlsToProto(dagAccessControls);
    assertSame(dagAccessControls, aclInfo);
    assertSame(DagTypeConverters.convertDAGAccessControlsFromProto(aclInfo), aclInfo);

    dagAccessControls = new DAGAccessControls("*", "*");
    aclInfo = DagTypeConverters.convertDAGAccessControlsToProto(dagAccessControls);
    assertSame(dagAccessControls, aclInfo);
    assertSame(DagTypeConverters.convertDAGAccessControlsFromProto(aclInfo), aclInfo);
  }

  private void assertSame(DAGAccessControls dagAccessControls, ACLInfo aclInfo) {
    assertEquals(dagAccessControls.getUsersWithViewACLs(),
        Sets.newHashSet(aclInfo.getUsersWithViewAccessList()));
    assertEquals(dagAccessControls.getUsersWithModifyACLs(),
        Sets.newHashSet(aclInfo.getUsersWithModifyAccessList()));
    assertEquals(dagAccessControls.getGroupsWithViewACLs(),
        Sets.newHashSet(aclInfo.getGroupsWithViewAccessList()));
    assertEquals(dagAccessControls.getGroupsWithModifyACLs(),
        Sets.newHashSet(aclInfo.getGroupsWithModifyAccessList()));
  }

  private void verifyPlugins(List<TezNamedEntityDescriptorProto> entities, int expectedCount,
                             String baseString, boolean hasPayload) {
    assertEquals(expectedCount, entities.size());
    for (int i = 0; i < expectedCount; i++) {
      assertEquals(indexedEntity(baseString, i), entities.get(i).getName());
      TezEntityDescriptorProto subEntityProto = entities.get(i).getEntityDescriptor();
      assertEquals(append(indexedEntity(baseString, i), classSuffix),
          subEntityProto.getClassName());
      assertEquals(hasPayload, subEntityProto.hasTezUserPayload());
      if (hasPayload) {
        UserPayload userPayload =
            UserPayload
                .create(subEntityProto.getTezUserPayload().getUserPayload().asReadOnlyByteBuffer(),
                    subEntityProto.getTezUserPayload().getVersion());
        ByteBuffer bb = userPayload.getPayload();
        assertNotNull(bb);
        assertEquals(i, bb.getInt());
      }
    }
  }

  private TaskSchedulerDescriptor[] createTaskScheduelrs(int count, boolean withUserPayload) {
    TaskSchedulerDescriptor[] descriptors = new TaskSchedulerDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = TaskSchedulerDescriptor.create(indexedEntity(testScheduler, i),
          append(indexedEntity(testScheduler, i), classSuffix));
      if (withUserPayload) {
        descriptors[i].setUserPayload(createPayload(i));
      }
    }
    return descriptors;
  }

  private ContainerLauncherDescriptor[] createContainerLaunchers(int count,
                                                                 boolean withUserPayload) {
    ContainerLauncherDescriptor[] descriptors = new ContainerLauncherDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = ContainerLauncherDescriptor.create(indexedEntity(testLauncher, i),
          append(indexedEntity(testLauncher, i), classSuffix));
      if (withUserPayload) {
        descriptors[i].setUserPayload(createPayload(i));
      }
    }
    return descriptors;
  }

  private TaskCommunicatorDescriptor[] createTaskCommunicators(int count, boolean withUserPayload) {
    TaskCommunicatorDescriptor[] descriptors = new TaskCommunicatorDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = TaskCommunicatorDescriptor.create(indexedEntity(testComm, i),
          append(indexedEntity(testComm, i), classSuffix));
      if (withUserPayload) {
        descriptors[i].setUserPayload(createPayload(i));
      }
    }
    return descriptors;
  }

  private static UserPayload createPayload(int i) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, i);
    UserPayload payload = UserPayload.create(bb);
    return payload;
  }

  private String indexedEntity(String name, int index) {
    return name + index;
  }

  private String append(String s1, String s2) {
    return s1 + s2;
  }
}
