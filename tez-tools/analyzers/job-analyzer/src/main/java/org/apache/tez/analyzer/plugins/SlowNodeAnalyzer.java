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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;

import java.util.Collection;
import java.util.List;


/**
 * This will provide the set of nodes participated in the DAG in descending order of task execution
 * time.
 * <p/>
 * Combine it with other counters to understand slow nodes better.
 */
public class SlowNodeAnalyzer extends TezAnalyzerBase implements Analyzer {

  private static final Log LOG = LogFactory.getLog(SlowNodeAnalyzer.class);

  private static final String[] headers = { "nodeName", "noOfTasksExecuted", "noOfKilledTasks",
      "noOfFailedTasks", "avgSucceededTaskExecutionTime", "avgKilledTaskExecutionTime",
      "avgFailedTaskExecutionTime", "avgHDFSBytesRead", "avgHDFSBytesWritten",
      "avgFileBytesRead", "avgFileBytesWritten", "avgGCTimeMillis", "avgCPUTimeMillis" };

  private final CSVResult csvResult = new CSVResult(headers);

  private final Configuration config;

  public SlowNodeAnalyzer(Configuration config) {
    this.config = config;
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    Multimap<String, TaskAttemptInfo> nodeDetails = dagInfo.getNodeDetails();
    for (String nodeName : nodeDetails.keySet()) {
      List<String> record = Lists.newLinkedList();

      Collection<TaskAttemptInfo> taskAttemptInfos = nodeDetails.get(nodeName);

      record.add(nodeName);
      record.add(taskAttemptInfos.size() + "");
      record.add(getNumberOfTasks(taskAttemptInfos, TaskAttemptState.KILLED) + "");
      record.add(getNumberOfTasks(taskAttemptInfos, TaskAttemptState.FAILED) + "");

      Iterable<TaskAttemptInfo> succeedTasks = getFilteredTaskAttempts(taskAttemptInfos,
          TaskAttemptState.SUCCEEDED);
      record.add(getAvgTaskExecutionTime(succeedTasks) + "");

      Iterable<TaskAttemptInfo> killedTasks = getFilteredTaskAttempts(taskAttemptInfos,
          TaskAttemptState.KILLED);
      record.add(getAvgTaskExecutionTime(killedTasks) + "");

      Iterable<TaskAttemptInfo> failedTasks = getFilteredTaskAttempts(taskAttemptInfos,
          TaskAttemptState.FAILED);
      record.add(getAvgTaskExecutionTime(failedTasks) + "");

      record.add(getAvgCounter(taskAttemptInfos, FileSystemCounter.class
          .getName(), FileSystemCounter.HDFS_BYTES_READ.name()) + "");
      record.add(getAvgCounter(taskAttemptInfos, FileSystemCounter.class
          .getName(), FileSystemCounter.HDFS_BYTES_WRITTEN.name()) + "");
      record.add(getAvgCounter(taskAttemptInfos, FileSystemCounter.class
          .getName(), FileSystemCounter.FILE_BYTES_READ.name()) + "");
      record.add(getAvgCounter(taskAttemptInfos, FileSystemCounter.class
          .getName(), FileSystemCounter.FILE_BYTES_WRITTEN.name()) + "");
      record.add(getAvgCounter(taskAttemptInfos, TaskCounter.class
          .getName(), TaskCounter.GC_TIME_MILLIS.name()) + "");
      record.add(getAvgCounter(taskAttemptInfos, TaskCounter.class
              .getName(), TaskCounter.CPU_MILLISECONDS.name()) + "");

          csvResult.addRecord(record.toArray(new String[record.size()]));
    }
  }

  private Iterable<TaskAttemptInfo> getFilteredTaskAttempts(Collection<TaskAttemptInfo>
      taskAttemptInfos, final TaskAttemptState status) {
    return Iterables.filter(taskAttemptInfos, new
        Predicate<TaskAttemptInfo>() {
          @Override public boolean apply(TaskAttemptInfo input) {
            return input.getStatus().equalsIgnoreCase(status.toString());
          }
        });
  }

  private float getAvgTaskExecutionTime(Iterable<TaskAttemptInfo> taskAttemptInfos) {
    long totalTime = 0;
    int size = 0;
    for (TaskAttemptInfo attemptInfo : taskAttemptInfos) {
      totalTime += attemptInfo.getTimeTaken();
      size++;
    }
    return (size > 0) ? (totalTime * 1.0f / size) : 0;
  }

  private int getNumberOfTasks(Collection<TaskAttemptInfo> taskAttemptInfos, TaskAttemptState
      status) {
    int tasks = 0;
    for (TaskAttemptInfo attemptInfo : taskAttemptInfos) {
      if (attemptInfo.getStatus().equalsIgnoreCase(status.toString())) {
        tasks++;
      }
    }
    return tasks;
  }

  private float getAvgCounter(Collection<TaskAttemptInfo> taskAttemptInfos, String
      counterGroupName, String counterName) {
    long total = 0;
    int taskCount = 0;
    for (TaskAttemptInfo attemptInfo : taskAttemptInfos) {
      TezCounters tezCounters = attemptInfo.getTezCounters();
      TezCounter counter = tezCounters.findCounter(counterGroupName, counterName);
      if (counter != null) {
        total += counter.getValue();
        taskCount++;
      } else {
        LOG.info("Could not find counterGroupName=" + counterGroupName + ", counter=" +
            counterName + " in " + attemptInfo);
      }
    }
    return (taskCount > 0) ? (total * 1.0f / taskCount) : 0;
  }

  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Slow Node Analyzer";
  }

  @Override
  public String getDescription() {
    StringBuilder sb = new StringBuilder();
    sb.append("Analyze node details for the DAG.").append("\n");
    sb.append("This could be used to find out the set of nodes where the tasks are taking more "
        + "time on average.").append("\n");
    sb.append("This could be used to find out the set of nodes where the tasks are taking more "
        + "time on average and to understand whether too many tasks got scheduled on a node.")
        .append("\n");
    sb.append("One needs to combine the task execution time with other metrics like bytes "
        + "read/written etc to get better idea of bad nodes. In order to understand the slow "
        + "nodes due to network, it might be worthwhile to consider the shuffle performance "
        + "analyzer tool in tez-tools").append("\n");
    return sb.toString();
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    SlowNodeAnalyzer analyzer = new SlowNodeAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
