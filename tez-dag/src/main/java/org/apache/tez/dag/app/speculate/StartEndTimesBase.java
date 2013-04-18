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

package org.apache.tez.dag.app.speculate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.records.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate.TaskAttemptStatus;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskID;
import org.apache.tez.engine.records.TezVertexID;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

abstract class StartEndTimesBase<V> implements TaskRuntimeEstimator {
  static final float MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE
      = 0.05F;
  static final int MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
      = 1;

  protected Configuration conf = null;
  protected AppContext context = null;

  protected final Map<TezTaskAttemptID, Long> startTimes
      = new ConcurrentHashMap<TezTaskAttemptID, Long>();

  // XXXX This class design assumes that the contents of AppContext.getAllJobs
  //   never changes.  Is that right?
  //
  // This assumption comes in in several places, mostly in data structure that
  //   can grow without limit if a AppContext gets new Job's when the old ones
  //   run out.  Also, these mapper statistics blocks won't cover the Job's
  //   we don't know about.
  // TODO handle multiple DAGs
  protected final Map<TezVertexID, DataStatistics> vertexStatistics
      = new HashMap<TezVertexID, DataStatistics>();

  private float slowTaskRelativeTresholds = 0f;

  protected final Set<Task> doneTasks = new HashSet<Task>();

  @Override
  public void enrollAttempt(TaskAttemptStatus status, long timestamp) {
    startTimes.put(status.id,timestamp);
  }

  @Override
  public long attemptEnrolledTime(TezTaskAttemptID attemptID) {
    Long result = startTimes.get(attemptID);

    return result == null ? Long.MAX_VALUE : result;
  }


  @Override
  public void contextualize(Configuration conf, AppContext context) {
    this.conf = conf;
    this.context = context;


    final DAG dag = context.getDAG();
    for (Entry<TezVertexID, Vertex> entry: dag.getVertices().entrySet()) {
      vertexStatistics.put(entry.getKey(), new DataStatistics());
      slowTaskRelativeTresholds =
          conf.getFloat(MRJobConfig.SPECULATIVE_SLOWTASK_THRESHOLD, 1.0f);
    }
  }

  protected DataStatistics dataStatisticsForTask(TezTaskID taskID) {
    DAG dag = context.getDAG();

    if (dag == null) {
      return null;
    }

    Task task = dag.getVertex(taskID.getVertexID()).getTask(taskID);

    if (task == null) {
      return null;
    }

    return vertexStatistics.get(taskID.getVertexID());
  }

  @Override
  public long thresholdRuntime(TezTaskID taskID) {
    DAG job = context.getDAG();

    DataStatistics statistics = dataStatisticsForTask(taskID);

    Vertex v = job.getVertex(taskID.getVertexID());
    int completedTasksOfType = v.getCompletedTasks();
    int totalTasksOfType = v.getTotalTasks();
    
    if (completedTasksOfType < MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
        || (((float)completedTasksOfType) / totalTasksOfType)
              < MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE ) {
      return Long.MAX_VALUE;
    }

    long result =  statistics == null
        ? Long.MAX_VALUE
        : (long)statistics.outlier(slowTaskRelativeTresholds);
    return result;
  }

  @Override
  public long estimatedNewAttemptRuntime(TezTaskID id) {
    DataStatistics statistics = dataStatisticsForTask(id);

    if (statistics == null) {
      return -1L;
    }

    return (long)statistics.mean();
  }

  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {

    TezTaskAttemptID attemptID = status.id;
    TezTaskID taskID = attemptID.getTaskID();
    DAG job = context.getDAG();

    if (job == null) {
      return;
    }

    Task task = job.getVertex(taskID.getVertexID()).getTask(taskID);

    if (task == null) {
      return;
    }

    Long boxedStart = startTimes.get(attemptID);
    long start = boxedStart == null ? Long.MIN_VALUE : boxedStart;
    
    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    if (taskAttempt.getState() == TaskAttemptState.SUCCEEDED) {
      boolean isNew = false;
      // is this  a new success?
      synchronized (doneTasks) {
        if (!doneTasks.contains(task)) {
          doneTasks.add(task);
          isNew = true;
        }
      }

      // It's a new completion
      // Note that if a task completes twice [because of a previous speculation
      //  and a race, or a success followed by loss of the machine with the
      //  local data] we only count the first one.
      if (isNew) {
        long finish = timestamp;
        if (start > 1L && finish > 1L && start <= finish) {
          long duration = finish - start;

          DataStatistics statistics
          = dataStatisticsForTask(taskID);

          if (statistics != null) {
            statistics.add(duration);
          }
        }
      }
    }
  }
}
