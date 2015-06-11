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

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.RandomTextWriterJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.mapreduce.examples.MRRSleepJob;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRRJobs {

  private static final Logger LOG = LoggerFactory.getLogger(TestMRRJobs.class);

  protected static MiniTezCluster mrrTezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static String TEST_ROOT_DIR = "target"
      + Path.SEPARATOR + TestMRRJobs.class.getName() + "-tmpDir";

  private static final String OUTPUT_ROOT_DIR = "/tmp" + Path.SEPARATOR +
      TestMRRJobs.class.getSimpleName();

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (!(new File(MiniTezCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniTezCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrrTezCluster == null) {
      mrrTezCluster = new MiniTezCluster(TestMRRJobs.class.getName(), 1,
          1, 1);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      conf.setLong(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0l);
      mrrTezCluster.init(conf);
      mrrTezCluster.start();
    }

  }

  @AfterClass
  public static void tearDown() {
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @Test (timeout = 60000)
  public void testMRRSleepJob() throws IOException, InterruptedException,
      ClassNotFoundException {
    LOG.info("\n\n\nStarting testMRRSleepJob().");

    if (!(new File(MiniTezCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniTezCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    Configuration sleepConf = new Configuration(mrrTezCluster.getConfig());

    MRRSleepJob sleepJob = new MRRSleepJob();
    sleepJob.setConf(sleepConf);

    Job job = sleepJob.createJob(1, 1, 1, 1, 1,
        1, 1, 1, 1, 1);

    job.setJarByClass(MRRSleepJob.class);
    job.setMaxMapAttempts(1); // speed up failures
    job.submit();
    String trackingUrl = job.getTrackingURL();
    String jobId = job.getJobID().toString();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    // There's one bug in YARN that there may be some suffix at the end of trackingURL (YARN-2246)
    // After TEZ-1961, the tracking will change from http://localhost:53419/proxy/application_1430963524753_0005
    // to http://localhost:53419/proxy/application_1430963524753_0005/ui/
    // So here use String#contains to verify.
    Assert.assertTrue("Tracking URL was " + trackingUrl +
                      " but didn't Match Job ID " + jobId ,
          trackingUrl.contains(jobId.substring(jobId.indexOf("_"))));

    // FIXME once counters and task progress can be obtained properly
    // TODO use dag client to test counters and task progress?
    // what about completed jobs?
  }

  @Test (timeout = 60000)
  public void testRandomWriter() throws IOException, InterruptedException,
      ClassNotFoundException {

    LOG.info("\n\n\nStarting testRandomWriter().");
    if (!(new File(MiniTezCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniTezCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    RandomTextWriterJob randomWriterJob = new RandomTextWriterJob();
    mrrTezCluster.getConfig().set(RandomTextWriterJob.TOTAL_BYTES, "3072");
    mrrTezCluster.getConfig().set(RandomTextWriterJob.BYTES_PER_MAP, "1024");
    Job job = randomWriterJob.createJob(mrrTezCluster.getConfig());
    Path outputDir = new Path(OUTPUT_ROOT_DIR, "random-output");
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setSpeculativeExecution(false);
    job.setJarByClass(RandomTextWriterJob.class);
    job.setMaxMapAttempts(1); // speed up failures
    job.submit();
    String trackingUrl = job.getTrackingURL();
    String jobId = job.getJobID().toString();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    Assert.assertTrue("Tracking URL was " + trackingUrl +
                      " but didn't Match Job ID " + jobId ,
          trackingUrl.contains(jobId.substring(jobId.indexOf("_"))));

    // Make sure there are three files in the output-dir

    RemoteIterator<FileStatus> iterator =
        FileContext.getFileContext(mrrTezCluster.getConfig()).listStatus(
            outputDir);
    int count = 0;
    while (iterator.hasNext()) {
      FileStatus file = iterator.next();
      if (!file.getPath().getName()
          .equals(FileOutputCommitter.SUCCEEDED_FILE_NAME)) {
        count++;
      }
    }
    Assert.assertEquals("Number of part files is wrong!", 3, count);

  }


  @Test (timeout = 60000)
  public void testFailingJob() throws IOException, InterruptedException,
      ClassNotFoundException {

    LOG.info("\n\n\nStarting testFailingJob().");

    if (!(new File(MiniTezCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniTezCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    Configuration sleepConf = new Configuration(mrrTezCluster.getConfig());

    MRRSleepJob sleepJob = new MRRSleepJob();
    sleepJob.setConf(sleepConf);

    Job job = sleepJob.createJob(1, 1, 1, 1, 1,
        1, 1, 1, 1, 1);

    job.setJarByClass(MRRSleepJob.class);
    job.setMaxMapAttempts(1); // speed up failures
    job.getConfiguration().setBoolean(MRRSleepJob.MAP_FATAL_ERROR, true);
    job.getConfiguration().set(MRRSleepJob.MAP_ERROR_TASK_IDS, "*");

    job.submit();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertFalse(succeeded);
    Assert.assertEquals(JobStatus.State.FAILED, job.getJobState());

    // FIXME once counters and task progress can be obtained properly
    // TODO verify failed task diagnostics
  }

  @Test (timeout = 60000)
  public void testFailingAttempt() throws IOException, InterruptedException,
      ClassNotFoundException {

    LOG.info("\n\n\nStarting testFailingAttempt().");

    if (!(new File(MiniTezCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniTezCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    Configuration sleepConf = new Configuration(mrrTezCluster.getConfig());

    MRRSleepJob sleepJob = new MRRSleepJob();
    sleepJob.setConf(sleepConf);

    Job job = sleepJob.createJob(1, 1, 1, 1, 1,
        1, 1, 1, 1, 1);

    job.setJarByClass(MRRSleepJob.class);
    job.setMaxMapAttempts(3); // speed up failures
    job.getConfiguration().setBoolean(MRRSleepJob.MAP_THROW_ERROR, true);
    job.getConfiguration().set(MRRSleepJob.MAP_ERROR_TASK_IDS, "0");

    job.submit();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());

    // FIXME once counters and task progress can be obtained properly
    // TODO verify failed task diagnostics
  }

  @Test (timeout = 60000)
  public void testMRRSleepJobWithCompression() throws IOException,
      InterruptedException, ClassNotFoundException {
    LOG.info("\n\n\nStarting testMRRSleepJobWithCompression().");

    if (!(new File(MiniTezCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniTezCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    Configuration sleepConf = new Configuration(mrrTezCluster.getConfig());

    MRRSleepJob sleepJob = new MRRSleepJob();
    sleepJob.setConf(sleepConf);

    Job job = sleepJob.createJob(1, 1, 2, 1, 1,
        1, 1, 1, 1, 1);

    job.setJarByClass(MRRSleepJob.class);
    job.setMaxMapAttempts(1); // speed up failures

    // enable compression
    job.getConfiguration().setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    job.getConfiguration().set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC,
        DefaultCodec.class.getName());

    job.submit();
    String trackingUrl = job.getTrackingURL();
    String jobId = job.getJobID().toString();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    Assert.assertTrue("Tracking URL was " + trackingUrl +
                      " but didn't Match Job ID " + jobId ,
          trackingUrl.contains(jobId.substring(jobId.indexOf("_"))));

    // FIXME once counters and task progress can be obtained properly
    // TODO use dag client to test counters and task progress?
    // what about completed jobs?

  }


  /*
  //@Test (timeout = 60000)
  public void testMRRSleepJobWithSecurityOn() throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.info("\n\n\nStarting testMRRSleepJobWithSecurityOn().");

    if (!(new File(MiniMRRTezCluster.APPJAR)).exists()) {
      return;
    }

    mrrTezCluster.getConfig().set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    mrrTezCluster.getConfig().set(YarnConfiguration.RM_KEYTAB, "/etc/krb5.keytab");
    mrrTezCluster.getConfig().set(YarnConfiguration.NM_KEYTAB, "/etc/krb5.keytab");
    mrrTezCluster.getConfig().set(YarnConfiguration.RM_PRINCIPAL,
        "rm/sightbusy-lx@LOCALHOST");
    mrrTezCluster.getConfig().set(YarnConfiguration.NM_PRINCIPAL,
        "nm/sightbusy-lx@LOCALHOST");

    UserGroupInformation.setConfiguration(mrrTezCluster.getConfig());

    // Keep it in here instead of after RM/NM as multiple user logins happen in
    // the same JVM.
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    LOG.info("User name is " + user.getUserName());
    for (Token<? extends TokenIdentifier> str : user.getTokens()) {
      LOG.info("Token is " + str.encodeToUrlString());
    }
    user.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        MRRSleepJob sleepJob = new MRRSleepJob();
        sleepJob.setConf(mrrTezCluster.getConfig());
        Job job = sleepJob.createJob(3, 0, 10000, 1, 0, 0);
        // //Job with reduces
        // Job job = sleepJob.createJob(3, 2, 10000, 1, 10000, 1);
        job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
        job.submit();
        String trackingUrl = job.getTrackingURL();
        String jobId = job.getJobID().toString();
        job.waitForCompletion(true);
        Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
        Assert.assertTrue("Tracking URL was " + trackingUrl +
                          " but didn't Match Job ID " + jobId ,
          trackingUrl.endsWith(jobId.substring(jobId.lastIndexOf("_")) + "/"));
        return null;
      }
    });

    // TODO later:  add explicit "isUber()" checks of some sort
  }
  */

}
