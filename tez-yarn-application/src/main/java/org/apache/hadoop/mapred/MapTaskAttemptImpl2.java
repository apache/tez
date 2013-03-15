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

package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.TaskHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.TaskAttemptImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.TezTypeConverters;
import org.apache.tez.mapreduce.task.impl.MRTaskContext;

@SuppressWarnings("rawtypes")
public class MapTaskAttemptImpl2 extends TaskAttemptImpl {

  private final TaskSplitMetaInfo splitInfo;

  public MapTaskAttemptImpl2(TaskId taskId, int attempt, 
      EventHandler eventHandler, Path jobFile, 
      int partition, TaskSplitMetaInfo splitInfo, JobConf conf,
      TaskAttemptListener taskAttemptListener, 
      OutputCommitter committer, Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock, TaskHeartbeatHandler thh,
      AppContext appContext, String tezModuleClassName) {
    super(taskId, attempt, eventHandler, 
        taskAttemptListener, jobFile, partition, conf, splitInfo.getLocations(),
        committer, jobToken, credentials, clock, thh, appContext,
        tezModuleClassName);
    this.splitInfo = splitInfo;
  }

  @Override
  public MRTaskContext createRemoteMRTaskContext() {
    MRTaskContext mrTaskContext = new MRTaskContext(
        TezTypeConverters.toTez(getID()), conf.get(MRJobConfig.USER_NAME),
        conf.get(MRJobConfig.JOB_NAME), tezModuleClassName, null,
        splitInfo.getSplitLocation(), splitInfo.getStartOffset(), 0);

    return mrTaskContext;
  }

}
