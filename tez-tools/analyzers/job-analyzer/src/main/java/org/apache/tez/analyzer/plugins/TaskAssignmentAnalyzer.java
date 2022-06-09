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

import java.util.HashMap;
import java.util.Map;

/**
 * Get the Task assignments on different nodes of the cluster.
 */
public class TaskAssignmentAnalyzer extends TezAnalyzerBase
    implements Analyzer {
  private final String[] headers = {"vertex", "node", "numTaskAttempts", "load"};
  private final CSVResult csvResult;

  public TaskAssignmentAnalyzer(Configuration config) {
    super(config);
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    Map<String, Integer> taskAttemptsPerNode = new HashMap<>();
    for (VertexInfo vertex : dagInfo.getVertices()) {
      taskAttemptsPerNode.clear();
      for (TaskAttemptInfo attempt : vertex.getTaskAttempts()) {
        Integer previousValue = taskAttemptsPerNode.get(attempt.getNodeId());
        taskAttemptsPerNode.put(attempt.getNodeId(), previousValue == null ? 1 : previousValue + 1);
      }
      double mean = vertex.getTaskAttempts().size() / Math.max(1.0, taskAttemptsPerNode.size());
      for (Map.Entry<String, Integer> assignment : taskAttemptsPerNode.entrySet()) {
        addARecord(vertex.getVertexName(), assignment.getKey(), assignment.getValue(),
            assignment.getValue() * 100 / mean);
      }
    }
  }

  private void addARecord(String vertexName, String node, int numTasks,
                          double load) {
    String[] record = new String[4];
    record[0] = vertexName;
    record[1] = node;
    record[2] = String.valueOf(numTasks);
    record[3] = String.format("%.2f", load);
    csvResult.addRecord(record);
  }

  @Override
  public Result getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Task Assignment Analyzer";
  }

  @Override
  public String getDescription() {
    return "Get the Task assignments on different nodes of the cluster";
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    TaskAssignmentAnalyzer analyzer = new TaskAssignmentAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
