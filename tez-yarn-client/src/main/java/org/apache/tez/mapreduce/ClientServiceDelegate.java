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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

import java.io.IOException;

public class ClientServiceDelegate {
  private static final Log LOG = LogFactory.getLog(ClientServiceDelegate.class);

  private final TezConfiguration conf;
  private final ResourceMgrDelegate rm;
  private DAGClient dagClient;
  private String currentDAGId;
  private TezClient tezClient;
  private ApplicationReport appReport;

  // FIXME
  // how to handle completed jobs that the RM does not know about?

  public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm,
      JobID jobId) {
    this.conf = new TezConfiguration(conf); // Cloning for modifying.
    // For faster redirects from AM to HS.
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        this.conf.getInt(MRJobConfig.MR_CLIENT_TO_AM_IPC_MAX_RETRIES,
            MRJobConfig.DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES));
    this.rm = rm;
    tezClient = new TezClient(this.conf);
  }

  public org.apache.hadoop.mapreduce.Counters getJobCounters(JobID jobId)
      throws IOException, InterruptedException {
    // FIXME needs counters support from DAG
    // with a translation layer on client side
    org.apache.hadoop.mapreduce.Counters empty =
        new org.apache.hadoop.mapreduce.Counters();
    return empty;
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobId,
      int fromEventId, int maxEvents)
      throws IOException, InterruptedException {
    // FIXME seems like there is support in client to query task failure
    // related information
    // However, api does not make sense for DAG
    return new TaskCompletionEvent[0];
  }

  public String[] getTaskDiagnostics(org.apache.hadoop.mapreduce.TaskAttemptID
      taId)
      throws IOException, InterruptedException {
    // FIXME need support to query task diagnostics?
    return new String[0];
  }
  
  public JobStatus getJobStatus(JobID oldJobID) throws IOException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId =
      TypeConverter.toYarn(oldJobID);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    String jobFile = MRApps.getJobFile(conf, user, oldJobID);

    try {
      if(dagClient == null) {
        appReport = getAppReport(oldJobID);
        if(appReport.getYarnApplicationState() != YarnApplicationState.RUNNING) {
          // if job not running return status from appReport;
          return getJobStatusFromRM(appReport, jobFile);
        } else {
          // job is running. create dag am client
          dagClient = tezClient.getDAGClient(appReport.getHost(), 
                                             appReport.getRpcPort());
          currentDAGId = dagClient.getAllDAGs().get(0);
        }
      }
      // return status from client. use saved appReport for queue etc
      DAGStatus dagStatus = dagClient.getDAGStatus(currentDAGId);
      return new DAGJobStatus(appReport, dagStatus, jobFile);
    } catch (TezException e) {
      // AM not responding
      dagClient = null;
      currentDAGId = null;
      appReport = getAppReport(oldJobID);
      if(appReport.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        LOG.info("App not running. Falling back to RM for report.");
      } else {
        LOG.warn("App running but failed to get report from AM.", e);
      }
    }
    
    // final fallback
    return getJobStatusFromRM(appReport, jobFile);
  }
  
  private JobStatus getJobStatusFromRM(ApplicationReport appReport, String jobFile) {
    JobStatus jobStatus =
      new DAGJobStatus(appReport, null, jobFile);
    return jobStatus;            
  }

  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(
      JobID oldJobID, TaskType taskType)
       throws IOException{
    // TEZ-146: need to return real task reports
    return new org.apache.hadoop.mapreduce.TaskReport[0];
  }

  public boolean killTask(TaskAttemptID taskAttemptID, boolean fail)
       throws IOException {
    // FIXME need support to kill a task attempt?
    throw new UnsupportedOperationException();
  }

  public boolean killJob(JobID oldJobID)
       throws IOException {
    // FIXME need support to kill a dag?
    // Should this be just an RM killApplication?
    // For one dag per AM, RM kill should suffice
    throw new UnsupportedOperationException();
  }

  public LogParams getLogFilePath(JobID oldJobID,
      TaskAttemptID oldTaskAttemptID)
      throws YarnException, IOException {
    // FIXME logs for an attempt?
    throw new UnsupportedOperationException();
  }
  
  private ApplicationReport getAppReport(JobID oldJobID) throws IOException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId =
      TypeConverter.toYarn(oldJobID);
    try {
      return rm.getApplicationReport(jobId.getAppId());
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }
}
