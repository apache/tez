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

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.Event;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

public class DagOverviewAnalyzer extends TezAnalyzerBase implements Analyzer {
  private final String[] headers =
      { "name", "id", "event_type", "status", "event_time", "event_time_str", "vertex_task_stats", "diagnostics" };
  private final CSVResult csvResult;
  private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  public DagOverviewAnalyzer(Configuration config) {
    super(config);
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    for (Event event : dagInfo.getEvents()) {
      csvResult.addRecord(new String[] { dagInfo.getDagId(), dagInfo.getDagId(), event.getType(),
          dagInfo.getStatus(), Long.toString(event.getTime()), toDateStr(event.getTime()), "", "" });
    }
    for (VertexInfo vertex : dagInfo.getVertices()) {
      for (Event event : vertex.getEvents()) {
        String vertexFailureInfoIfAny = "";
        for (TaskAttemptInfo attempt : vertex.getTaskAttempts()) {
          if (attempt.getStatus().contains("FAILED")) {
            vertexFailureInfoIfAny = attempt.getTaskAttemptId() + ": "
                + attempt.getDiagnostics().replaceAll(",", " ").replaceAll("\n", " ");
            break;
          }
        }
        csvResult.addRecord(new String[] { vertex.getVertexName(), vertex.getVertexId(),
            event.getType(), vertex.getStatus(), Long.toString(event.getTime()),
            toDateStr(event.getTime()), getTaskStats(vertex), vertexFailureInfoIfAny });
      }

      // a failed task can lead to dag failure, so hopefully holds valuable information
      for (TaskInfo failedTask : vertex.getFailedTasks()) {
        for (Event failedTaskEvent : failedTask.getEvents()) {
          if (failedTaskEvent.getType().equalsIgnoreCase("TASK_FINISHED")) {
            csvResult.addRecord(new String[] { vertex.getVertexName(), failedTask.getTaskId(),
                failedTaskEvent.getType(), failedTask.getStatus(), Long.toString(failedTaskEvent.getTime()),
                toDateStr(failedTaskEvent.getTime()), getTaskStats(vertex),
                failedTask.getDiagnostics().replaceAll(",", " ").replaceAll("\n", " ") });
          }
        }
        // if we already found a failing task, let's scan the failing attempts as well
        for (TaskAttemptInfo failedAttempt : failedTask.getFailedTaskAttempts()) {
          for (Event failedTaskAttemptEvent : failedAttempt.getEvents()) {
            if (failedTaskAttemptEvent.getType().equalsIgnoreCase("TASK_ATTEMPT_FINISHED")) {
              csvResult.addRecord(new String[] { vertex.getVertexName(),
                  failedAttempt.getTaskAttemptId(), failedTaskAttemptEvent.getType(),
                  failedAttempt.getStatus(), Long.toString(failedTaskAttemptEvent.getTime()),
                  toDateStr(failedTaskAttemptEvent.getTime()), getTaskStats(vertex),
                  failedAttempt.getDiagnostics().replaceAll(",", " ").replaceAll("\n", " ") });
            }
          }
        }
      }
    }

    csvResult.sort(new Comparator<String[]>() {
      public int compare(String[] first, String[] second) {
        return (int) (Long.parseLong(first[4]) - Long.parseLong(second[4]));
      }
    });
  }

  private String getTaskStats(VertexInfo vertex) {
    return String.format("numTasks: %d failedTasks: %d completedTasks: %d", vertex.getNumTasks(),
        vertex.getFailedTasksCount(), vertex.getCompletedTasksCount());
  }

  private static synchronized String toDateStr(long time) {
    return FORMAT.format(new Date(time));
  }

  @Override
  public Result getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Dag overview analyzer";
  }

  @Override
  public String getDescription() {
    return "High level dag events overview (dag, vertex event summary)."
        + " Helps understand the overall progress of a dag by simply listing the dag/vertex related events";
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    DagOverviewAnalyzer analyzer = new DagOverviewAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
