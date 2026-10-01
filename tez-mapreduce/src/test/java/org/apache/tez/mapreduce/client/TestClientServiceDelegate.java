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

package org.apache.tez.mapreduce.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

import org.junit.Test;

public class TestClientServiceDelegate {

  @Test
  void testGetJobCountersWithoutDAGClientReturnsEmpty() throws Exception {
    Configuration conf = new Configuration(false);
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    JobID jobId = new JobID("jt", 1);
    ClientServiceDelegate delegate = new ClientServiceDelegate(conf, rm, jobId);

    Counters counters = delegate.getJobCounters(jobId);

    assertNotNull(counters);
    assertEquals(0, counters.countCounters());
  }

  @Test
  void testGetJobCountersWithDAGClientReturnsTranslatedCounters() throws Exception {
    Configuration conf = new Configuration(false);
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    JobID jobId = new JobID("jt", 2);

    TezCounters tezCounters = new TezCounters();
    tezCounters.addGroup("TestGroup", "Test Group Display");
    tezCounters.findCounter("TestGroup", "TestCounter").setValue(42L);

    DAGStatus dagStatus = mock(DAGStatus.class);
    when(dagStatus.getDAGCounters()).thenReturn(tezCounters);

    DAGClient dagClient = mock(DAGClient.class);
    when(dagClient.getDAGStatus(any())).thenReturn(dagStatus);

    ClientServiceDelegate delegate = new ClientServiceDelegate(conf, rm, jobId, dagClient);

    Counters counters = delegate.getJobCounters(jobId);

    assertNotNull(counters);
    assertTrue("Expected at least one counter", counters.countCounters() >= 1);
    assertNotNull(counters.getGroup("TestGroup"));
    assertEquals(42L, counters.findCounter("TestGroup", "TestCounter").getValue());
  }

  @Test
  void testGetJobCountersWhenDAGStatusReturnsNullCounters() throws Exception {
    Configuration conf = new Configuration(false);
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    JobID jobId = new JobID("jt", 3);

    DAGStatus dagStatus = mock(DAGStatus.class);
    when(dagStatus.getDAGCounters()).thenReturn(null);

    DAGClient dagClient = mock(DAGClient.class);
    when(dagClient.getDAGStatus(any())).thenReturn(dagStatus);

    ClientServiceDelegate delegate = new ClientServiceDelegate(conf, rm, jobId, dagClient);

    Counters counters = delegate.getJobCounters(jobId);

    assertNotNull(counters);
    assertEquals(0, counters.countCounters());
  }
}
