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
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.hadoop.TezTypeConverters;

public class ClientServiceDelegate {

  private final TezConfiguration conf;
  private final DAGClient dagClient;

  // FIXME
  // how to handle completed jobs that the RM does not know about?

  public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm,
      JobID jobId) {
    this(conf, rm, jobId, null);
  }

  public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm,
      JobID jobId, DAGClient dagClient) {
    this.conf = new TezConfiguration(conf); // Cloning for modifying.
    this.dagClient = dagClient;
    // For faster redirects from AM to HS.
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        this.conf.getInt(MRJobConfig.MR_CLIENT_TO_AM_IPC_MAX_RETRIES,
            MRJobConfig.DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES));
  }

  /**
   * Returns the DAG client for this delegate, if any (used for counters).
   */
  DAGClient getDAGClient() {
    return dagClient;
  }

  /**
   * Returns job counters from the Tez DAG, translated to MapReduce Counters.
   * Returns empty counters if no DAG client is set, if the AM is unreachable
   * (e.g. job already finished and AM exited), or if counters are not available.
   */
  public Counters getJobCounters(JobID jobId)
      throws IOException, InterruptedException {
    if (dagClient == null) {
      return new Counters();
    }
    try {
      TezCounters tezCounters = dagClient.getDAGStatus(
          EnumSet.of(StatusGetOpts.GET_COUNTERS)).getDAGCounters();
      Counters mrCounters = TezTypeConverters.fromTez(tezCounters);
      return mrCounters != null ? mrCounters : new Counters();
    } catch (TezException e) {
      // AM may be gone (e.g. completed job); return empty counters
      return new Counters();
    }
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
    // handled in YARNRunner
    throw new UnsupportedOperationException();
  }

  public TaskReport[] getTaskReports(
      JobID oldJobID, TaskType taskType)
       throws IOException{
    // TEZ-146: need to return real task reports
    return new TaskReport[0];
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
}
