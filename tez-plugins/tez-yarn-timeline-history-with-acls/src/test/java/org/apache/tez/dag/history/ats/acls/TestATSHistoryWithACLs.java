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
import static org.mockito.Mockito.*;

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
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.apache.tez.tests.MiniTezClusterWithTimeline;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.mockito.Matchers;

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

  /**
   * test failure of domain creation during dag submittion in session mode
   * only affect logging for that dag not following submitted dag 
   * @throws Exception
   */
  @Test (timeout=50000)
  public void testMultipleDagSession() throws Exception {
    TezClient tezSession = null;
    String viewAcls = "nobody nobody_group";
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

    //////submit first dag which fails in dag creation//////
    ATSHistoryACLPolicyManager myAclPolicyManager = ReflectionUtils.createClazzInstance(
              atsHistoryACLManagerClassName);
    myAclPolicyManager.timelineClient = mock(TimelineClient.class);

    doThrow(new IOException("Fail to Put Domain")).when(myAclPolicyManager.timelineClient).putDomain(Matchers.<TimelineDomain>anyVararg());
    tezSession.setUpHistoryAclManager(myAclPolicyManager);

    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
          + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    String dagLogging = dag.getDagConf().get(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED);
    assertEquals(dagLogging, "false");
    
    myAclPolicyManager.timelineClient = null;
    myAclPolicyManager.setConf(tezConf);
    tezSession.setUpHistoryAclManager(myAclPolicyManager);

    //////submit second dag which succeeds in dag creation//////
    DAG dag2 = DAG.create("TezSleepProcessor2");
    vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
          SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
       Resource.newInstance(256, 1));
    dag2.addVertex(vertex);
    accessControls = new DAGAccessControls();
    accessControls.setUsersWithViewACLs(Collections.singleton("nobody3"));
    accessControls.setGroupsWithViewACLs(Collections.singleton("nobody_group3"));
    dag2.setAccessControls(accessControls);
    dagClient = tezSession.submitDAG(dag2);
    dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
                + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    dagLogging = dag2.getDagConf().get(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED);
    Assert.assertNull(dagLogging);
    myAclPolicyManager.timelineClient = spy(myAclPolicyManager.timelineClient);
    tezSession.stop();
    verify(myAclPolicyManager.timelineClient, times(1)).stop();
  }
  
/**
 * test failure of domain creation during dag submittion in nonsession mode
 * only affect logging for that dag not following submitted dag 
 * @throws Exception
 */
  @Test (timeout=50000)
  public void testMultipleDagNonSession() throws Exception {
    TezClient tezClient = null;
    String viewAcls = "nobody nobody_group";
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

    tezClient = TezClient.create("TezSleepProcessor", tezConf, false);
    tezClient.start();

    //////submit first dag which fails in dag creation//////
    ATSHistoryACLPolicyManager myAclPolicyManager = ReflectionUtils.createClazzInstance(
              atsHistoryACLManagerClassName);
    myAclPolicyManager.timelineClient = mock(TimelineClient.class);

    doThrow(new IOException("Fail to Put Domain")).when(myAclPolicyManager.timelineClient).putDomain(Matchers.<TimelineDomain>anyVararg());
    tezClient.setUpHistoryAclManager(myAclPolicyManager);

    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
          + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    String dagLogging = dag.getDagConf().get(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED);
    assertEquals(dagLogging, "false");
    
    myAclPolicyManager.timelineClient = null;
    myAclPolicyManager.setConf(tezConf);
    tezClient.setUpHistoryAclManager(myAclPolicyManager);

    //////submit second dag which succeeds in dag creation//////
    DAG dag2 = DAG.create("TezSleepProcessor2");
    vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
           SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
        Resource.newInstance(256, 1));
    dag2.addVertex(vertex);
    accessControls = new DAGAccessControls();
    accessControls.setUsersWithViewACLs(Collections.singleton("nobody3"));
    accessControls.setGroupsWithViewACLs(Collections.singleton("nobody_group3"));
    dag2.setAccessControls(accessControls);
    dagClient = tezClient.submitDAG(dag2);
    dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
                   + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    dagLogging = dag2.getDagConf().get(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED);
    Assert.assertNull(dagLogging);
    myAclPolicyManager.timelineClient = spy(myAclPolicyManager.timelineClient);
    tezClient.stop();
    verify(myAclPolicyManager.timelineClient, times(1)).stop();

  }
  /**
   * Test Disable Logging for all dags in a session 
   * due to failure to create domain in session start
   * @throws Exception
   */
  @Test (timeout=50000)
  public void testDisableSessionLogging() throws Exception {
    TezClient tezSession = null;
    String viewAcls = "nobody nobody_group";
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
    ATSHistoryACLPolicyManager myAclPolicyManager = ReflectionUtils.createClazzInstance(
            atsHistoryACLManagerClassName);
    myAclPolicyManager.timelineClient = mock(TimelineClient.class);

    doThrow(new IOException("Fail to Put Domain")).
        when(myAclPolicyManager.timelineClient).putDomain(Matchers.<TimelineDomain>anyVararg());
    tezSession.setUpHistoryAclManager(myAclPolicyManager);
    tezSession.start();

    ///substitute back mocked timelineClient with a normal one
    myAclPolicyManager.timelineClient = null;
    myAclPolicyManager.setConf(tezConf);
    tezSession.setUpHistoryAclManager(myAclPolicyManager);
    //////submit first dag //////
    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
          + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    String dagLogging = dag.getDagConf().get(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED);
    assertEquals(dagLogging, "false");
 
    //////submit second dag//////
    DAG dag2 = DAG.create("TezSleepProcessor2");
    vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
          SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
       Resource.newInstance(256, 1));
    dag2.addVertex(vertex);
    accessControls = new DAGAccessControls();
    accessControls.setUsersWithViewACLs(Collections.singleton("nobody3"));
    accessControls.setGroupsWithViewACLs(Collections.singleton("nobody_group3"));
    dag2.setAccessControls(accessControls);
    dagClient = tezSession.submitDAG(dag2);
    dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
                + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    dagLogging = dag2.getDagConf().get(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED);
    assertEquals(dagLogging, "false");
    tezSession.stop();
  }
  /**
   * use mini cluster to verify data do not push to ats when the daglogging flag
   * in dagsubmittedevent is set off
   * @throws Exception
   */
  @Test (timeout=50000)
  public void testDagLoggingDisabled() throws Exception {
    ATSHistoryLoggingService historyLoggingService;
    historyLoggingService =
        ReflectionUtils.createClazzInstance(ATSHistoryLoggingService.class.getName());
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    String viewAcls = "nobody nobody_group";
    tezConf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewAcls);
    tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        ATSHistoryLoggingService.class.getName());
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
        .nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    historyLoggingService.serviceInit(tezConf);
    historyLoggingService.serviceStart();
    ApplicationId appId = ApplicationId.newInstance(100l, 1);
    TezDAGID tezDAGID = TezDAGID.getInstance(
                        appId, 100);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    DAGPlan dagPlan = DAGPlan.newBuilder().setName("DAGPlanMock").build();
    DAGSubmittedEvent submittedEvent = new DAGSubmittedEvent(tezDAGID,
          1, dagPlan, appAttemptId, null,
          "usr", tezConf, null);
    submittedEvent.setHistoryLoggingEnabled(false);
    DAGHistoryEvent event = new DAGHistoryEvent(tezDAGID, submittedEvent);
    historyLoggingService.handle(new DAGHistoryEvent(tezDAGID, submittedEvent));
    Thread.sleep(1000l);
    String url = "http://" + timelineAddress + "/ws/v1/timeline/TEZ_DAG_ID/"+event.getDagID();
    Client client = new Client();
    WebResource resource = client.resource(url);

    ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(404, response.getStatus());
  }
  
  /**
   * use mini cluster to verify data do push to ats when
   * the dag logging flag in dagsubmitted event is set on
   * @throws Exception
   */
  @Test (timeout=50000)
  public void testDagLoggingEnabled() throws Exception {
    ATSHistoryLoggingService historyLoggingService;
    historyLoggingService =
            ReflectionUtils.createClazzInstance(ATSHistoryLoggingService.class.getName());
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    String viewAcls = "nobody nobody_group";
    tezConf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewAcls);
    tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        ATSHistoryLoggingService.class.getName());
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
        .nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    historyLoggingService.serviceInit(tezConf);
    historyLoggingService.serviceStart();
    ApplicationId appId = ApplicationId.newInstance(100l, 1);
    TezDAGID tezDAGID = TezDAGID.getInstance(
                        appId, 11);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    DAGPlan dagPlan = DAGPlan.newBuilder().setName("DAGPlanMock").build();
    DAGSubmittedEvent submittedEvent = new DAGSubmittedEvent(tezDAGID,
            1, dagPlan, appAttemptId, null,
            "usr", tezConf, null);
    submittedEvent.setHistoryLoggingEnabled(true);
    DAGHistoryEvent event = new DAGHistoryEvent(tezDAGID, submittedEvent);
    historyLoggingService.handle(new DAGHistoryEvent(tezDAGID, submittedEvent));
    Thread.sleep(1000l);
    String url = "http://" + timelineAddress + "/ws/v1/timeline/TEZ_DAG_ID/"+event.getDagID();
    Client client = new Client();
    WebResource resource = client.resource(url);

    ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineEntity entity = response.getEntity(TimelineEntity.class);
    assertEquals(entity.getEntityType(), "TEZ_DAG_ID");
    assertEquals(entity.getEvents().get(0).getEventType(), HistoryEventType.DAG_SUBMITTED.toString());
  }
  
  private static final String atsHistoryACLManagerClassName =
      "org.apache.tez.dag.history.ats.acls.ATSHistoryACLPolicyManager";
  @Test (timeout=50000)
  public void testTimelineServiceDisabled() throws Exception {
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        ATSHistoryLoggingService.class.getName());
    tezConf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,false);
    ATSHistoryACLPolicyManager historyACLPolicyManager = ReflectionUtils.createClazzInstance(
        atsHistoryACLManagerClassName);
    historyACLPolicyManager.setConf(tezConf);
    Assert.assertNull(historyACLPolicyManager.timelineClient);
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
