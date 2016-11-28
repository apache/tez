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
  private final String[] headers = { "vertex", "node", "numTasks", "load" };
  private final Configuration config;
  private final CSVResult csvResult;

  public TaskAssignmentAnalyzer(Configuration config) {
    this.config = config;
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    Map<String, Integer> map = new HashMap<>();
    for (VertexInfo vertex : dagInfo.getVertices()) {
      map.clear();
      for (TaskAttemptInfo attempt : vertex.getTaskAttempts()) {
        Integer previousValue = map.get(attempt.getNodeId());
        map.put(attempt.getNodeId(),
            previousValue == null ? 1 : previousValue + 1);
      }
      double mean = vertex.getTaskAttempts().size() / Math.max(1.0, map.size());
      for (Map.Entry<String, Integer> assignment : map.entrySet()) {
        addARecord(vertex.getVertexName(), assignment.getKey(),
            assignment.getValue(), assignment.getValue() * 100 / mean);
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

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    TaskAssignmentAnalyzer analyzer = new TaskAssignmentAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
