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

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * Analyze slow tasks in the DAG. Top 100 tasks are listed by default.
 *
 * <p/>
 * //TODO: We do not get counters for killed task attempts yet.
 */
public class SlowTaskIdentifier extends TezAnalyzerBase implements Analyzer {

  private static final String[] headers = { "vertexName", "taskAttemptId",
      "Node", "taskDuration", "Status", "diagnostics",
      "NoOfInputs" };

  private final CSVResult csvResult;

  private static final String NO_OF_TASKS = "tez.slow-task-analyzer.task.count";
  private static final int NO_OF_TASKS_DEFAULT = 100;

  private final Configuration config;

  public SlowTaskIdentifier(Configuration config) {
    this.config = config;
    this.csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    List<TaskAttemptInfo> taskAttempts = Lists.newArrayList();
    for(VertexInfo vertexInfo : dagInfo.getVertices()) {
      taskAttempts.addAll(vertexInfo.getTaskAttempts());
    }

    //sort them by runtime in descending order
    Collections.sort(taskAttempts, new Comparator<TaskAttemptInfo>() {
      @Override public int compare(TaskAttemptInfo o1, TaskAttemptInfo o2) {
        return (o1.getTimeTaken() > o2.getTimeTaken()) ? -1 :
            ((o1.getTimeTaken() == o2.getTimeTaken()) ?
                0 : 1);
      }
    });

    int limit = Math.min(taskAttempts.size(),
        Math.max(0, config.getInt(NO_OF_TASKS, NO_OF_TASKS_DEFAULT)));

    if (limit == 0) {
      return;
    }

    for (int i = 0; i < limit - 1; i++) {
      List<String> record = Lists.newLinkedList();
      record.add(taskAttempts.get(i).getTaskInfo().getVertexInfo().getVertexName());
      record.add(taskAttempts.get(i).getTaskAttemptId());
      record.add(taskAttempts.get(i).getContainer().getHost());
      record.add(taskAttempts.get(i).getTimeTaken() + "");
      record.add(taskAttempts.get(i).getStatus());
      record.add(taskAttempts.get(i).getDiagnostics());
      record.add(taskAttempts.get(i).getTaskInfo().getVertexInfo().getInputEdges().size() + "");

      csvResult.addRecord(record.toArray(new String[record.size()]));
    }

  }

  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Slow Task Identifier";
  }

  @Override
  public String getDescription() {
    return "Identifies slow tasks in the DAG";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    SlowTaskIdentifier analyzer = new SlowTaskIdentifier(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
