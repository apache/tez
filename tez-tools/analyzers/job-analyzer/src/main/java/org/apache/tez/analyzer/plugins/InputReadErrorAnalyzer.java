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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.Event;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

/**
 * Helps finding the root cause of shuffle errors, e.g. which node(s) can be blamed for them.
 */
public class InputReadErrorAnalyzer extends TezAnalyzerBase implements Analyzer {
  private final String[] headers = { "vertex:attempt", "status", "time", "node", "diagnostics" };
  private final CSVResult csvResult;

  public InputReadErrorAnalyzer(Configuration config) {
    super(config);
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    for (VertexInfo vertex : dagInfo.getVertices()) {
      for (TaskAttemptInfo attempt : vertex.getTaskAttempts()) {
        String terminationCause = attempt.getTerminationCause();
        if ("INPUT_READ_ERROR".equalsIgnoreCase(terminationCause)
            || "OUTPUT_LOST".equalsIgnoreCase(terminationCause)
            || "NODE_FAILED".equalsIgnoreCase(terminationCause)) {
          for (Event event : attempt.getEvents()) {
            if (event.getType().equalsIgnoreCase("TASK_ATTEMPT_FINISHED")) {
              csvResult.addRecord(new String[] {
                  vertex.getVertexName() + ":" + attempt.getTaskAttemptId(),
                  attempt.getDetailedStatus(), String.valueOf(event.getTime()), attempt.getNodeId(),
                  attempt.getDiagnostics().replaceAll(",", " ").replaceAll("\n", " ") });
            }
          }
        }
      }
    }

    csvResult.sort(new Comparator<String[]>() {
      public int compare(String[] first, String[] second) {
        return (int) (Long.parseLong(second[2]) - Long.parseLong(first[2]));
      }
    });
  }

  @Override
  public Result getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Input read error analyzer";
  }

  @Override
  public String getDescription() {
    return "Prints every task attempt (with node) which are related to input read errors";
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    InputReadErrorAnalyzer analyzer = new InputReadErrorAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
