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

package org.apache.tez.dag.history.ats.acls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.apache.tez.tests.MiniTezClusterWithTimeline;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class TestATSHistoryWithACLs {

  private static final Logger LOG = LoggerFactory.getLogger(TestATSHistoryWithACLs.class);

  protected static MiniTezClusterWithTimeline mrrTezCluster = null;
  protected static MiniDFSCluster dfsCluster = null;
  private static String timelineAddress;
  private Random random = new Random();

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestATSHistoryWithACLs.class.getName() + "-tmpDir";

  private static String user;

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null)
          .build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (mrrTezCluster == null) {
      try {
        mrrTezCluster = new MiniTezClusterWithTimeline(TestATSHistoryWithACLs.class.getName(),
            1, 1, 1, true);
        Configuration conf = new Configuration();
        conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
        conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
        conf.setInt("yarn.nodemanager.delete.debug-delay-sec", 20000);
        mrrTezCluster.init(conf);
        mrrTezCluster.start();
      } catch (Throwable e) {
        LOG.info("Failed to start Mini Tez Cluster", e);
      }
    }
    user = UserGroupInformation.getCurrentUser().getShortUserName();
    timelineAddress = mrrTezCluster.getConfig().get(
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS);
    if (timelineAddress != null) {
      // Hack to handle bug in MiniYARNCluster handling of webapp address
      timelineAddress = timelineAddress.replace("0.0.0.0", "localhost");
    }
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    LOG.info("Shutdown invoked");
    Thread.sleep(10000);
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  // To be replaced after Timeline has java APIs for domains
  private <K> K getTimelineData(String url, Class<K> clazz) {
    Client client = new Client();
    WebResource resource = client.resource(url);

    ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

    K entity = response.getEntity(clazz);
    assertNotNull(entity);
    return entity;
  }

  private TimelineDomain getDomain(String domainId) {
    assertNotNull(timelineAddress);
    String url = "http://" + timelineAddress + "/ws/v1/timeline/domain/" + domainId;
    LOG.info("Getting timeline domain: " + url);
    TimelineDomain domain = getTimelineData(url, TimelineDomain.class);
    assertNotNull(domain);
    assertNotNull(domain.getOwner());
    assertNotNull(domain.getReaders());
    assertNotNull(domain.getWriters());
    LOG.info("TimelineDomain for id " + domainId
        + ", owner=" + domain.getOwner()
        + ", readers=" + domain.getReaders()
        + ", writers=" + domain.getWriters());
    return domain;
  }

  private void verifyDomainACLs(TimelineDomain timelineDomain,
    Collection<String> users, Collection<String> groups) {
    String readers = timelineDomain.getReaders();
    int pos = readers.indexOf(" ");
    String readerUsers = readers.substring(0, pos);
    String readerGroups = readers.substring(pos+1);

    assertTrue(readerUsers.contains(user));
    for (String s : users) {
      assertTrue(readerUsers.contains(s));
    }
    for (String s : groups) {
      assertTrue(readerGroups.contains(s));
    }

    if (!user.equals("nobody1") && !users.contains("nobody1")) {
      assertFalse(readerUsers.contains("nobody1"));
    }

  }

  @Test (timeout=50000)
  public void testSimpleAMACls() throws Exception {
    TezClient tezSession = null;
    ApplicationId applicationId;
    String viewAcls = "nobody nobody_group";
    try {
      SleepProcessorConfig spConf = new SleepProcessorConfig(1);

      DAG dag = DAG.create("TezSleepProcessor");
      Vertex vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
              SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
          Resource.newInstance(256, 1));
      dag.addVertex(vertex);

      TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
      tezConf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewAcls);
      tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
          ATSHistoryLoggingService.class.getName());
      Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
          .nextInt(100000))));
      remoteFs.mkdirs(remoteStagingDir);
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

      tezSession = TezClient.create("TezSleepProcessor", tezConf, true);
      tezSession.start();

      applicationId = tezSession.getAppMasterApplicationId();

      DAGClient dagClient = tezSession.submitDAG(dag);

      DAGStatus dagStatus = dagClient.getDAGStatus(null);
      while (!dagStatus.isCompleted()) {
        LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
            + dagStatus.getState());
        Thread.sleep(500l);
        dagStatus = dagClient.getDAGStatus(null);
      }
      assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }

    TimelineDomain timelineDomain = getDomain(
        ATSHistoryACLPolicyManager.DOMAIN_ID_PREFIX + applicationId.toString());
    verifyDomainACLs(timelineDomain,
        Collections.singleton("nobody"), Collections.singleton("nobody_group"));

    verifyEntityDomains(applicationId, true);
  }

  @Test (timeout=50000)
  public void testDAGACls() throws Exception {
    TezClient tezSession = null;
    ApplicationId applicationId;
    String viewAcls = "nobody nobody_group";
    try {
      SleepProcessorConfig spConf = new SleepProcessorConfig(1);

      DAG dag = DAG.create("TezSleepProcessor");
      Vertex vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
              SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
          Resource.newInstance(256, 1));
      dag.addVertex(vertex);
      DAGAccessControls accessControls = new DAGAccessControls();
      accessControls.setUsersWithViewACLs(Collections.singleton("nobody2"));
      accessControls.setGroupsWithViewACLs(Collections.singleton("nobody_group2"));
      dag.setAccessControls(accessControls);

      TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
      tezConf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewAcls);
      tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
          ATSHistoryLoggingService.class.getName());
      Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
          .nextInt(100000))));
      remoteFs.mkdirs(remoteStagingDir);
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

      tezSession = TezClient.create("TezSleepProcessor", tezConf, true);
      tezSession.start();

      applicationId = tezSession.getAppMasterApplicationId();

      DAGClient dagClient = tezSession.submitDAG(dag);

      DAGStatus dagStatus = dagClient.getDAGStatus(null);
      while (!dagStatus.isCompleted()) {
        LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
            + dagStatus.getState());
        Thread.sleep(500l);
        dagStatus = dagClient.getDAGStatus(null);
      }
      assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }

    TimelineDomain timelineDomain = getDomain(
        ATSHistoryACLPolicyManager.DOMAIN_ID_PREFIX + applicationId.toString());
    verifyDomainACLs(timelineDomain,
        Collections.singleton("nobody"), Collections.singleton("nobody_group"));

    timelineDomain = getDomain(ATSHistoryACLPolicyManager.DOMAIN_ID_PREFIX
        + applicationId.toString() + "_TezSleepProcessor");
    verifyDomainACLs(timelineDomain,
        Sets.newHashSet("nobody", "nobody2"),
        Sets.newHashSet("nobody_group", "nobody_group2"));

    verifyEntityDomains(applicationId, false);
  }

  private void verifyEntityDomains(ApplicationId applicationId, boolean sameDomain) {
    assertNotNull(timelineAddress);

    String appUrl = "http://" + timelineAddress + "/ws/v1/timeline/TEZ_APPLICATION/"
        + "tez_" + applicationId.toString();
    LOG.info("Getting timeline entity for tez application: " + appUrl);
    TimelineEntity appEntity = getTimelineData(appUrl, TimelineEntity.class);

    TezDAGID tezDAGID = TezDAGID.getInstance(applicationId, 1);
    String dagUrl = "http://" + timelineAddress + "/ws/v1/timeline/TEZ_DAG_ID/"
        + tezDAGID.toString();
    LOG.info("Getting timeline entity for tez dag: " + dagUrl);
    TimelineEntity dagEntity = getTimelineData(dagUrl, TimelineEntity.class);

    // App and DAG entities should have different domains
    assertEquals(ATSHistoryACLPolicyManager.DOMAIN_ID_PREFIX + applicationId.toString(),
        appEntity.getDomainId());
    if (!sameDomain) {
      assertEquals(ATSHistoryACLPolicyManager.DOMAIN_ID_PREFIX + applicationId.toString()
          + "_TezSleepProcessor", dagEntity.getDomainId());
    } else {
      assertEquals(appEntity.getDomainId(), dagEntity.getDomainId());
    }
  }


}
