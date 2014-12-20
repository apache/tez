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

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;

public class TestTezAMRMClient {

  private TezAMRMClientAsync amrmClient;

  @BeforeClass
  public static void beforeClass() {
    MockDNSToSwitchMapping.initializeMockRackResolver();
  }

  @Before
  public void setup() {
    amrmClient = new TezAMRMClientAsync(new AMRMClientImpl(),
      1000, mock(AMRMClientAsync.CallbackHandler.class));
    RackResolver.init(new Configuration());
  }

  @After
  public void teardown() {
    amrmClient = null;
  }

  @Test(timeout=10000)
  public void testMatchingRequestsForTopPriority() {
    String[] hosts = { "host1" };
    String[] racks = { "rack1" };
    AMRMClient.ContainerRequest req1 = new AMRMClient.ContainerRequest(
      Resource.newInstance(2048, 1), hosts, racks,
      Priority.newInstance(1));
    AMRMClient.ContainerRequest req2 = new AMRMClient.ContainerRequest(
      Resource.newInstance(1024, 1), hosts, racks,
      Priority.newInstance(2));
    AMRMClient.ContainerRequest req3 = new AMRMClient.ContainerRequest(
      Resource.newInstance(1024, 1), hosts, racks,
      Priority.newInstance(3));
    amrmClient.addContainerRequest(req1);
    amrmClient.addContainerRequest(req2);
    amrmClient.addContainerRequest(req3);

    Assert.assertTrue(amrmClient.getMatchingRequestsForTopPriority("host1",
      Resource.newInstance(1024, 1)).isEmpty());

    List<? extends Collection<AMRMClient.ContainerRequest>> ret =
      amrmClient.getMatchingRequestsForTopPriority("host1",
        Resource.newInstance(2048, 1));
    Assert.assertFalse(ret.isEmpty());
    Assert.assertEquals(req1, ret.get(0).iterator().next());

    amrmClient.removeContainerRequest(req1);

    ret = amrmClient.getMatchingRequestsForTopPriority("host1",
        Resource.newInstance(1024, 1));
    Assert.assertFalse(ret.isEmpty());
    Assert.assertEquals(req2, ret.get(0).iterator().next());
  }

}
