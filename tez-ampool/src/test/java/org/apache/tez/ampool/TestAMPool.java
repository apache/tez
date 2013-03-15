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

package org.apache.tez.ampool;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.tez.ampool.AMPoolService;
import org.apache.tez.ampool.client.AMPoolClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestAMPool {

  private static final Log LOG =
      LogFactory.getLog(TestAMPool.class);

  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new YarnConfiguration();
  protected static YarnClient yarnClient = null;

  protected static String APPMASTER_JAR =
      JarFinder.getJar(AMPoolService.class);

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    LOG.info("Starting up YARN cluster");
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        FifoScheduler.class, ResourceScheduler.class);
    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(TestAMPool.class.getName(),
          1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
      }
      yarnCluster.getConfig().set("yarn.application.classpath", new File(url.getPath()).getParent());
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      yarnCluster.getConfig().writeXml(os);
      yarnClient = new YarnClientImpl();
      yarnClient.init(yarnCluster.getConfig());
      yarnClient.start();
      os.close();
    }
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
    if (yarnClient != null) {
      yarnClient.stop();
      yarnClient = null;
    }
  }

  @Test
  @Ignore
  public void testSimpleRun() throws Exception {

    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--master_memory",
        "512",
    };

    LOG.info("Initializing AMPool Client");
    AMPoolClient client = new AMPoolClient();
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running AMPool Client");
    boolean result = client.run();

    LOG.info("Client run completed. Result=" + result);
    Assert.assertTrue(result);

    while(true) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        LOG.info("thread sleep interrupted. message=" + e.getMessage());
      }

      List<ApplicationReport> reports = yarnClient.getApplicationList();
      if (reports.isEmpty()) {
        LOG.error("Did not find any applications");
        break;
      }
      FinalApplicationStatus status =
          reports.get(0).getFinalApplicationStatus();
      if (status == FinalApplicationStatus.UNDEFINED) {
        LOG.info("Application is still running");
        continue;
      } else {
        LOG.info("Application completed with status"
            + status);
        break;
      }

    }

  }

}
