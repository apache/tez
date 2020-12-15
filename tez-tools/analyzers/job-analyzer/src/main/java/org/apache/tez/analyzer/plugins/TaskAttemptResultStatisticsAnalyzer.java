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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

/**
 * Get simple count of task attempt states on vertex:node:status level, like below.
 *
 * vertex (+task stats: all/succeeded/failed/killed),node,status,numAttempts
 * Map 1 (vertex_x_y_z) (216/153/0/63),node1,KILLED:INTERNAL_PREEMPTION,1185
 * Map 1 (vertex_x_y_z) (216/153/0/63),node1,KILLED:TERMINATED_AT_SHUTDOWN,22
 * Map 1 (vertex_x_y_z) (216/153/0/63),node1,KILLED:EXTERNAL_PREEMPTION,3349
 * Map 1 (vertex_x_y_z) (216/153/0/63),node1,SUCCEEDED,1
 */
public class TaskAttemptResultStatisticsAnalyzer extends TezAnalyzerBase implements Analyzer {
  private final String[] headers =
      { "vertex (+task stats: all/succeeded/failed/killed)", "node", "status", "numAttempts" };
  private final Configuration config;
  private final CSVResult csvResult;

  public TaskAttemptResultStatisticsAnalyzer(Configuration config) {
    this.config = config;
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    Map<String, Integer> map = new HashMap<>();

    for (VertexInfo vertex : dagInfo.getVertices()) {
      String taskStatsInVertex =
          String.format("%s/%s/%s/%s", vertex.getNumTasks(), vertex.getSucceededTasksCount(),
              vertex.getFailedTasksCount(), vertex.getKilledTasksCount());
      for (TaskAttemptInfo attempt : vertex.getTaskAttempts()) {
        String key = String.format("%s#%s#%s",
            String.format("%s (%s) (%s)", vertex.getVertexName(), vertex.getVertexId(),
                taskStatsInVertex),
            attempt.getNodeId(), attempt.getDetailedStatus());
        Integer previousValue = (Integer) map.get(key);
        map.put(key, previousValue == null ? 1 : previousValue + 1);
      }
    }

    map.forEach((key, value) -> {
      addARecord(key.split("#")[0], key.split("#")[1], key.split("#")[2], value);
    });

    csvResult.sort(new Comparator<String[]>() {
      public int compare(String[] first, String[] second) {
        int vertexOrder = first[0].compareTo(second[0]);
        int nodeOrder = first[1].compareTo(second[1]);
        int statusOrder = first[2].compareTo(second[2]);

        return vertexOrder == 0 ? (nodeOrder == 0 ? statusOrder : nodeOrder) : vertexOrder;
      }
    });
  }

  private void addARecord(String vertexData, String node, String status,
      int numAttempts) {
    String[] record = new String[4];
    record[0] = vertexData;
    record[1] = node;
    record[2] = status;
    record[3] = Integer.toString(numAttempts);
    csvResult.addRecord(record);
  }

  @Override
  public Result getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Task Attempt Result Statistics Analyzer";
  }

  @Override
  public String getDescription() {
    return "Get statistics about task attempts states in vertex:node:status level";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    TaskAttemptResultStatisticsAnalyzer analyzer = new TaskAttemptResultStatisticsAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
