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

package org.apache.tez.dag.api.client.registry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.utils.AMRegistryUtils;

import org.junit.Test;

public class TestAMRegistry {

  @Test(timeout = 5000)
  public void testAMRegistryFactory() throws Exception {
    Configuration conf = new Configuration();
    AMRegistry amRegistry = AMRegistryUtils.createAMRegistry(conf);
    assertNull(amRegistry);
    String className = "org.apache.tez.dag.api.client.registry.TestAMRegistry$SkeletonAMRegistry";
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_CLASS, className);
    amRegistry = AMRegistryUtils.createAMRegistry(conf);
    assertEquals(className, amRegistry.getClass().getName());
  }

  @Test(timeout = 5000)
  public void testRecordForDagServer() {
    DAGClientServer dagClientServer = mock(DAGClientServer.class);
    when(dagClientServer.getBindAddress()).thenReturn(new InetSocketAddress("testhost", 1000));
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    String id = UUID.randomUUID().toString();
    AMRecord record = AMRegistryUtils.recordForDAGClientServer(appId, id, dagClientServer);
    assertEquals(appId, record.getApplicationId());
    assertEquals("testhost", record.getHost());
    assertEquals(1000, record.getPort());
    assertEquals(record.getId(), id);
  }

  @Test(timeout = 20000)
  public void testAMRegistryService() throws Exception {
    DAGClientHandler dagClientHandler = mock(DAGClientHandler.class);
    ApplicationAttemptId appAttemptId = mock(ApplicationAttemptId.class);
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    String uuid = UUID.randomUUID().toString();
    when(appAttemptId.getApplicationId()).thenReturn(appId);
    AMRegistry amRegistry = mock(AMRegistry.class);
    FileSystem fs = mock(FileSystem.class);
    DAGClientServer dagClientServer = new DAGClientServer(dagClientHandler, appAttemptId, fs);
    try {
      DAGAppMaster.initAmRegistry(appAttemptId.getApplicationId(), uuid, amRegistry, dagClientServer);
      dagClientServer.init(new Configuration());
      dagClientServer.start();
      AMRecord record = AMRegistryUtils.recordForDAGClientServer(appId, uuid, dagClientServer);
      verify(amRegistry, times(1)).add(record);
    } finally {
      dagClientServer.stop();
    }
  }

  public static class SkeletonAMRegistry extends AMRegistry {
    public SkeletonAMRegistry() {
      super("SkeletonAMRegistry");
    }
    @Override public void add(AMRecord server) throws Exception { }
    @Override public void remove(AMRecord server) throws Exception { }
  }
}
