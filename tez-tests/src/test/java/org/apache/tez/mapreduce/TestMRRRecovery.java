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

package org.apache.tez.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.RecoveryParser;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.mapreduce.examples.MRRSleepJob;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMRRRecovery {
  private static final Logger LOG = LoggerFactory.getLogger(TestMRRRecovery.class);

  private static MiniTezCluster mrrTezCluster;
  private static MiniDFSCluster dfsCluster;
  private static YarnClient yarnClient;

  private static FileSystem remoteFs;

  private static final String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestMRRJobs.class.getName() + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      Configuration conf = new Configuration();
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (mrrTezCluster == null) {
      mrrTezCluster = new MiniTezCluster(TestMRRJobs.class.getName(), 1,
          1, 1);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
      conf.setInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, 0);
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      conf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 500);
      conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
      conf.setBoolean(TezConfiguration.TEZ_AM_FAILURE_ON_MISSING_RECOVERY_DATA, true);
      mrrTezCluster.init(conf);
      mrrTezCluster.start();
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();
    }

  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    if (yarnClient != null) {
      yarnClient.close();
    }
  }

  private ApplicationId getApplicationId(Job job) throws Exception {
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    request.setName(job.getJobName());
    List<ApplicationReport> apps = yarnClient.getApplications(request);
    Assert.assertEquals(1, apps.size());
    return apps.get(0).getApplicationId();
  }

  private Path getJobStagingDir(Job job) throws Exception {
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    return new Path(String.format("%s/%s/.staging/%s", mrrTezCluster.getStagingPath(), userName, job.getJobID()));
  }

  private void assertStagingDir(Job job) throws Exception {
    // Wait for the clean-up process to be invoked
    while (true) {
      int numAllocatedCores = mrrTezCluster.getResourceManager().getResourceScheduler().getRootQueueMetrics()
          .getAllocatedVirtualCores();
      LOG.info("Number of cores in use: {}", numAllocatedCores);
      if (numAllocatedCores == 0) {
        break;
      }
      Thread.sleep(100L);
    }

    Assert.assertFalse(remoteFs.exists(getJobStagingDir(job)));
  }

  private void runJobAndKill(Job job) throws Exception {
    job.submit();

    ApplicationId applicationId = getApplicationId(job);
    Assert.assertTrue(remoteFs.exists(getJobStagingDir(job)));
    TimeUnit.SECONDS.sleep(10);
    List<ApplicationAttemptReport> attempts1 = yarnClient.getApplicationAttempts(applicationId);
    Assert.assertEquals(1, attempts1.size());
    Assert.assertTrue(remoteFs.exists(getJobStagingDir(job)));
    yarnClient.failApplicationAttempt(attempts1.get(0).getApplicationAttemptId());
  }

  @Test(timeout = 120000)
  public void testSucceed() throws Exception {
    Configuration sleepConf = new Configuration(mrrTezCluster.getConfig());

    MRRSleepJob sleepJob = new MRRSleepJob();
    sleepJob.setConf(sleepConf);
    Job job = sleepJob.createJob(1, 1, 1, 1, 1,
        1, 1, 1, 1, 1);
    job.setJobName("TestMRRRecovery-testSucceed");
    job.setJarByClass(MRRSleepJob.class);
    job.submit();
    Assert.assertTrue(job.waitForCompletion(true));
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    assertStagingDir(job);

    ApplicationId appId = getApplicationId(job);
    List<ApplicationAttemptReport> attempts = yarnClient.getApplicationAttempts(appId);
    Assert.assertEquals(1, attempts.size());
  }

  @Test(timeout = 120000)
  public void testAMRecoveryWhileMapperRunning() throws Exception {
    Configuration sleepConf = new Configuration(mrrTezCluster.getConfig());

    MRRSleepJob sleepJob = new MRRSleepJob();
    sleepJob.setConf(sleepConf);
    Job job = sleepJob.createJob(1, 1, 1, 1, 30000,
        1, 1, 1, 1, 1);
    job.setJobName("TestMRRRecovery-testAMRecoveryWhileMapperRunning");
    job.setJarByClass(MRRSleepJob.class);
    runJobAndKill(job);

    TimeUnit.SECONDS.sleep(10);
    List<ApplicationAttemptReport> attempts2 = yarnClient.getApplicationAttempts(getApplicationId(job));
    Assert.assertEquals(2, attempts2.size());
    Assert.assertTrue(remoteFs.exists(getJobStagingDir(job)));
    List<HistoryEvent> historyEvents1 = readRecoveryLog(job);
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 1).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 2).size());

    Assert.assertTrue(job.waitForCompletion(true));
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    assertStagingDir(job);

    ApplicationId appId = getApplicationId(job);
    List<ApplicationAttemptReport> attempts = yarnClient.getApplicationAttempts(appId);
    Assert.assertEquals(2, attempts.size());
  }

  @Test(timeout = 120000)
  public void testAMRecoveryWhileReducerRunning() throws Exception {
    Configuration sleepConf = new Configuration(mrrTezCluster.getConfig());

    MRRSleepJob sleepJob = new MRRSleepJob();
    sleepJob.setConf(sleepConf);
    Job job = sleepJob.createJob(1, 1, 1, 1, 1,
        1, 30000, 1, 1, 1);
    job.setJobName("TestMRRRecovery-testAMRecoveryWhileReducerRunning");
    job.setJarByClass(MRRSleepJob.class);
    runJobAndKill(job);

    TimeUnit.SECONDS.sleep(10);
    List<ApplicationAttemptReport> attempts2 = yarnClient.getApplicationAttempts(getApplicationId(job));
    Assert.assertEquals(2, attempts2.size());
    Assert.assertTrue(remoteFs.exists(getJobStagingDir(job)));
    List<HistoryEvent> historyEvents1 = readRecoveryLog(job);
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 1).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 2).size());

    Assert.assertTrue(job.waitForCompletion(true));
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    assertStagingDir(job);

    ApplicationId appId = getApplicationId(job);
    List<ApplicationAttemptReport> attempts = yarnClient.getApplicationAttempts(appId);
    Assert.assertEquals(2, attempts.size());
  }

  private List<HistoryEvent> readRecoveryLog(Job job) throws Exception {
    ApplicationId appId = getApplicationId(job);
    Configuration tezConf = mrrTezCluster.getConfig();
    Path tezSystemStagingDir = new Path(new Path(getJobStagingDir(job), TezCommonUtils.TEZ_SYSTEM_SUB_DIR),
        appId.toString());
    Path recoveryDataDir = TezCommonUtils.getRecoveryPath(tezSystemStagingDir, tezConf);
    FileSystem fs = tezSystemStagingDir.getFileSystem(tezConf);
    List<HistoryEvent> historyEvents = new ArrayList<>();
    int attemptId = 1;
    Path currentAttemptRecoveryDataDir = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, attemptId);
    Path recoveryFilePath =
        new Path(currentAttemptRecoveryDataDir, appId.toString().replace("application", "dag")
            + "_1" + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
    if (fs.exists(recoveryFilePath)) {
      LOG.info("Read recovery file:" + recoveryFilePath);
      historyEvents.addAll(RecoveryParser.parseDAGRecoveryFile(fs.open(recoveryFilePath)));
    }
    printHistoryEvents(historyEvents, attemptId);
    return historyEvents;
  }

  private void printHistoryEvents(List<HistoryEvent> historyEvents, int attemptId) {
    LOG.info("RecoveryLogs from attempt:" + attemptId);
    for(HistoryEvent historyEvent : historyEvents) {
      LOG.info("Parsed event from recovery stream"
          + ", eventType=" + historyEvent.getEventType()
          + ", event=" + historyEvent);
    }
    LOG.info("");
  }

  private List<TaskAttemptFinishedEvent> findTaskAttemptFinishedEvent(
      List<HistoryEvent> historyEvents, int vertexId) {
    List<TaskAttemptFinishedEvent> resultEvents = new ArrayList<>();
    for (HistoryEvent historyEvent : historyEvents) {
      if (historyEvent.getEventType() == HistoryEventType.TASK_ATTEMPT_FINISHED) {
        TaskAttemptFinishedEvent taFinishedEvent = (TaskAttemptFinishedEvent) historyEvent;
        if (taFinishedEvent.getState() == TaskAttemptState.KILLED) {
          continue;
        }
        if (taFinishedEvent.getVertexID().getId() == vertexId && taFinishedEvent.getTaskID().getId() == 0) {
          resultEvents.add(taFinishedEvent);
        }
      }
    }
    return resultEvents;
  }
}
