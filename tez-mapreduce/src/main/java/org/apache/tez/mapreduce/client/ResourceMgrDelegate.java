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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

public class ResourceMgrDelegate {
  private static final Log LOG = LogFactory.getLog(ResourceMgrDelegate.class);
      
  private YarnConfiguration conf;
  private GetNewApplicationResponse application;
  private ApplicationId applicationId;
  private YarnClient client;
  private InetSocketAddress rmAddress;

  /**
   * Delegate responsible for communicating with the Resource Manager's {@link ApplicationClientProtocol}.
   * @param conf the configuration object.
   */
  public ResourceMgrDelegate(YarnConfiguration conf) {
    super();
    this.conf = conf;
    client = YarnClient.createYarnClient();
    client.init(conf);
    this.rmAddress = conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
    client.start();
  }

  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnNodes(client.getNodeReports());
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    try {
      Set<String> appTypes = new HashSet<String>(1);
      appTypes.add(TezConstants.TEZ_APPLICATION_TYPE);
      return TypeConverter.fromYarnApps(client.getApplications(appTypes),
          this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    // TODO: Implement getBlacklistedTrackers
    LOG.warn("getBlacklistedTrackers - Not implemented yet");
    return new TaskTrackerInfo[0];
  }

  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    YarnClusterMetrics metrics;
    try {
      metrics = client.getYarnClusterMetrics();
    } catch (YarnException e) {
      throw new IOException(e);
    }
    ClusterMetrics oldMetrics = new ClusterMetrics(1, 1, 1, 1, 1, 1, 
        metrics.getNumNodeManagers() * 10, metrics.getNumNodeManagers() * 2, 1,
        metrics.getNumNodeManagers(), 0, 0);
    return oldMetrics;
  }

  @SuppressWarnings("rawtypes")
  public Token getDelegationToken(Text renewer) throws IOException,
      InterruptedException {
    try {
      // Remove rmAddress after YARN-868 is addressed
      return ConverterUtils.convertFromYarn(
        client.getRMDelegationToken(renewer), rmAddress);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public String getFilesystemName() throws IOException, InterruptedException {
    return FileSystem.get(conf).getUri().toString();
  }

  public JobID getNewJobID() throws IOException, InterruptedException {
    try {
      this.application = 
          client.createApplication().getNewApplicationResponse();
    } catch (YarnException e) {
      throw new IOException(e);
    }
    this.applicationId = this.application.getApplicationId();
    return TypeConverter.fromYarn(applicationId);
  }

  public QueueInfo getQueue(String queueName) throws IOException,
  InterruptedException {
    try {
      org.apache.hadoop.yarn.api.records.QueueInfo queueInfo =
          client.getQueueInfo(queueName);
      return (queueInfo == null) ? null : TypeConverter.fromYarn(queueInfo,
          conf);
      } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnQueueUserAclsInfo(
          client.getQueueAclsInfo());
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getAllQueues(), this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getRootQueueInfos(),
          this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getChildQueueInfos(parent),
        this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public String getStagingAreaDir() throws IOException, InterruptedException {
//    Path path = new Path(MRJobConstants.JOB_SUBMIT_DIR);
    String user = 
      UserGroupInformation.getCurrentUser().getShortUserName();
    Path path = MRApps.getStagingAreaDir(conf, user);
    LOG.debug("getStagingAreaDir: dir=" + path);
    return path.toString();
  }


  public String getSystemDir() throws IOException, InterruptedException {
    Path sysDir = new Path(MRJobConfig.JOB_SUBMIT_DIR);
    //FileContext.getFileContext(conf).delete(sysDir, true);
    return sysDir.toString();
  }
  

  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return 0;
  }
  
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    return;
  }


  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public void killApplication(ApplicationId appId)
      throws YarnException, IOException {
    client.killApplication(appId);
  }

}
