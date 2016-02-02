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
import com.google.common.collect.TreeMultiset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.util.Comparator;
import java.util.List;

/**
 * Analyze concurrent tasks running in every vertex at regular intervals.
 */
public class TaskConcurrencyAnalyzer extends TezAnalyzerBase implements Analyzer {

  private static final String[] headers = { "time", "vertexName", "concurrentTasksRunning" };

  private final CSVResult csvResult;
  private final Configuration config;

  public TaskConcurrencyAnalyzer(Configuration conf) {
    this.csvResult = new CSVResult(headers);
    this.config = conf;
  }

  private enum EventType {START, FINISH}

  static class TimeInfo {
    EventType eventType;
    long timestamp;
    int concurrentTasks;

    public TimeInfo(EventType eventType, long timestamp) {
      this.eventType = eventType;
      this.timestamp = timestamp;
    }
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {

    //For each vertex find the concurrent tasks running at any point
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      List<TaskAttemptInfo> taskAttempts =
          Lists.newLinkedList(vertexInfo.getTaskAttempts(true, null));

      String vertexName = vertexInfo.getVertexName();

      /**
       * - Get sorted multi-set of timestamps (S1, S2,...E1, E2..). Possible to have multiple
       * tasks starting/ending at same time.
       * - Walk through the set
       * - Increment concurrent tasks when start event is encountered
       * - Decrement concurrent tasks when start event is encountered
       */
      TreeMultiset<TimeInfo> timeInfoSet = TreeMultiset.create(new Comparator<TimeInfo>() {
        @Override public int compare(TimeInfo o1, TimeInfo o2) {
          if (o1.timestamp < o2.timestamp) {
            return -1;
          }

          if (o1.timestamp > o2.timestamp) {
            return 1;
          }

          if (o1.timestamp == o2.timestamp) {
            //check event type
            if (o1.eventType.equals(o2.eventType)) {
              return 0;
            }

            if (o1.eventType.equals(EventType.START)
                && o2.eventType.equals(EventType.FINISH)) {
              return -1;
            } else {
              return 1;
            }
          }
          return 0;
        }
      });

      for (TaskAttemptInfo attemptInfo : taskAttempts) {
        TimeInfo startTimeInfo = new TimeInfo(EventType.START, attemptInfo.getStartTime());
        TimeInfo stopTimeInfo = new TimeInfo(EventType.FINISH, attemptInfo.getFinishTime());

        timeInfoSet.add(startTimeInfo);
        timeInfoSet.add(stopTimeInfo);
      }

      //Compute concurrent tasks in the list now.
      int concurrentTasks = 0;
      for(TimeInfo timeInfo : timeInfoSet.elementSet()) {
        switch (timeInfo.eventType) {
        case START:
          concurrentTasks += timeInfoSet.count(timeInfo);
          break;
        case FINISH:
          concurrentTasks -= timeInfoSet.count(timeInfo);
          break;
        default:
          break;
        }
        timeInfo.concurrentTasks = concurrentTasks;
        addToResult(vertexName, timeInfo.timestamp, timeInfo.concurrentTasks);
      }
    }
  }

  private void addToResult(String vertexName, long currentTime, int concurrentTasks) {
    String[] record = { currentTime + "", vertexName, concurrentTasks + "" };
    csvResult.addRecord(record);
  }

  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "TaskConcurrencyAnalyzer";
  }

  @Override
  public String getDescription() {
    return "Analyze how many tasks were running in every vertex at given point in time. This "
        + "would be helpful in understanding whether any starvation was there or not.";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    TaskConcurrencyAnalyzer analyzer = new TaskConcurrencyAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
