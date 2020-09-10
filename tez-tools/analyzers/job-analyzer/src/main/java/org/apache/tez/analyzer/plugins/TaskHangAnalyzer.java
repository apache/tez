/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.analyzer.plugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Get the Task assignments on different nodes of the cluster.
 */
public class TaskHangAnalyzer extends TezAnalyzerBase implements Analyzer {
  private final String[] headers = { "vertex", "task", " number_of_attempts", "last_attempt_id",
      "last_attempt_status", "last_attempt_duration_ms", "last_attempt_node" };
  private final CSVResult csvResult;

  public TaskHangAnalyzer(Configuration config) {
    super(config);
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    Map<String, Map<String, String>> taskData = new HashMap<>(); // task attempt count per task
    for (VertexInfo vertex : dagInfo.getVertices()) {
      taskData.clear();
      for (TaskAttemptInfo attempt : vertex.getTaskAttempts()) {
        String taskId = attempt.getTaskInfo().getTaskId();

        int numAttemptsForTask = attempt.getTaskInfo().getNumberOfTaskAttempts();
        Map<String, String> thisTaskData = taskData.get(taskId);

        if (thisTaskData == null) {
          thisTaskData = new HashMap<>();
          thisTaskData.put("num_attempts", Integer.toString(numAttemptsForTask));
          taskData.put(taskId, thisTaskData);
        }

        // attempt_1599682376162_0006_27_00_000086_1
        int attemptNumber = Integer.parseInt(attempt.getTaskAttemptId().split("_")[6]);
        if (attemptNumber == numAttemptsForTask - 1) {
          thisTaskData.put("last_attempt_id", attempt.getTaskAttemptId());
          thisTaskData.put("last_attempt_status", attempt.getDetailedStatus());
          thisTaskData.put("last_attempt_node", attempt.getNodeId());

          thisTaskData.put("last_attempt_duration_ms",
              (attempt.getFinishTime() == 0 || attempt.getStartTime() == 0) ? "-1"
                : Long.toString(attempt.getFinishTime() - attempt.getStartTime()));
        }
      }
      for (Map.Entry<String, Map<String, String>> task : taskData.entrySet()) {
        addARecord(vertex.getVertexName(), task.getKey(), task.getValue().get("num_attempts"),
            task.getValue().get("last_attempt_id"), task.getValue().get("last_attempt_status"),
            task.getValue().get("last_attempt_duration_ms"),
            task.getValue().get("last_attempt_node"));
      }
    }

    csvResult.sort(new Comparator<String[]>() {
      public int compare(String[] first, String[] second) {
        int vertexOrder = first[0].compareTo(second[0]);
        int lastAttemptStatusOrder =
            (first[4] == null || second[4] == null) ? 0 : first[4].compareTo(second[4]);
        int attemptNumberOrder = Integer.valueOf(second[2]).compareTo(Integer.valueOf(first[2]));

        return vertexOrder == 0
          ? (lastAttemptStatusOrder == 0 ? attemptNumberOrder : lastAttemptStatusOrder)
          : vertexOrder;
      }
    });
  }

  private void addARecord(String vertexName, String taskId, String numAttempts,
      String lastAttemptId, String lastAttemptStatus, String lastAttemptDuration,
      String lastAttemptNode) {
    String[] record = new String[7];
    record[0] = vertexName;
    record[1] = taskId;
    record[2] = numAttempts;
    record[3] = lastAttemptId;
    record[4] = lastAttemptStatus;
    record[5] = lastAttemptDuration;
    record[6] = lastAttemptNode;

    csvResult.addRecord(record);
  }

  @Override
  public Result getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Task Hang Analyzer";
  }

  @Override
  public String getDescription() {
    return "TaskHandAnalyzer can give quick insights about hanging tasks/task attempts"
        + " by giving an overview of all tasks and their last attempts' status, duration, etc.";
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    TaskHangAnalyzer analyzer = new TaskHangAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
