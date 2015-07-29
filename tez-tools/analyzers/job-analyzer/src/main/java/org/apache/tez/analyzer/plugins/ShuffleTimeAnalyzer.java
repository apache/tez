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
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.util.List;
import java.util.Map;


/**
 * Analyze the time taken by merge phase, shuffle phase, time taken to do realistic work etc in
 * tasks.
 *
 * Just dump REDUCE_INPUT_GROUPS, REDUCE_INPUT_RECORDS, its ratio and SHUFFLE_BYTES for tasks
 * grouped by vertices. Provide time taken as well.  Just render it as a table for now.
 *
 */
public class ShuffleTimeAnalyzer implements Analyzer {

  private static final String SHUFFLE_TIME_RATIO = "tez.shuffle-time-analyzer.shuffle.ratio";
  private static final float SHUFFLE_TIME_RATIO_DEFAULT = 0.5f;

  private static final String MIN_SHUFFLE_RECORDS = "tez.shuffle-time-analyzer.shuffle.min.records";
  private static final long MIN_SHUFFLE_RECORDS_DEFAULT = 10000;

  private static final String[] headers = { "vertexName", "taskAttemptId", "Node", "counterGroup",
      "Comments", "REDUCE_INPUT_GROUPS", "REDUCE_INPUT_RECORDS", "ratio", "SHUFFLE_BYTES",
      "Time taken to receive all events", "MERGE_PHASE_TIME", "SHUFFLE_PHASE_TIME",
      "TimeTaken_For_Real_Task", "FIRST_EVENT_RECEIVED", "LAST_EVENT_RECEIVED",
      "SHUFFLE_BYTES_DISK_DIRECT" };

  private final CSVResult csvResult = new CSVResult(headers);

  private final Configuration config;

  private final float shuffleTimeRatio;
  private final long minShuffleRecords;


  public ShuffleTimeAnalyzer(Configuration config) {
    this.config = config;

    shuffleTimeRatio = config.getFloat
        (SHUFFLE_TIME_RATIO, SHUFFLE_TIME_RATIO_DEFAULT);
    minShuffleRecords = config.getLong(MIN_SHUFFLE_RECORDS, MIN_SHUFFLE_RECORDS_DEFAULT);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {

    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      for (TaskAttemptInfo attemptInfo : vertexInfo.getTaskAttempts()) {
        //counter_group (basically source) --> counter
        Map<String, TezCounter> reduceInputGroups = attemptInfo.getCounter(TaskCounter
            .REDUCE_INPUT_GROUPS.toString());
        Map<String, TezCounter> reduceInputRecords = attemptInfo.getCounter(TaskCounter
            .REDUCE_INPUT_RECORDS.toString());

        if (reduceInputGroups == null) {
          continue;
        }

        for (Map.Entry<String, TezCounter> entry : reduceInputGroups.entrySet()) {
          String counterGroupName = entry.getKey();
          long reduceInputGroupsVal = entry.getValue().getValue();
          long reduceInputRecordsVal = (reduceInputRecords.get(counterGroupName) != null) ?
          reduceInputRecords.get(counterGroupName).getValue() : 0;

          if (reduceInputRecordsVal <= 0) {
            continue;
          }
          float ratio = (reduceInputGroupsVal * 1.0f / reduceInputRecordsVal);

          if (ratio > 0 && reduceInputRecordsVal > minShuffleRecords) {
            List<String> result = Lists.newLinkedList();
            result.add(vertexInfo.getVertexName());
            result.add(attemptInfo.getTaskAttemptId());
            result.add(attemptInfo.getNodeId());
            result.add(counterGroupName);

            //Real work done in the task
            long timeTakenForRealWork = attemptInfo.getTimeTaken() -
                Long.parseLong(getCounterValue(TaskCounter.MERGE_PHASE_TIME, counterGroupName,
                    attemptInfo));

            String comments = "";
            if ((timeTakenForRealWork * 1.0f / attemptInfo.getTimeTaken()) < shuffleTimeRatio) {
              comments = "Time taken in shuffle is more than the actual work being done in task. "
                  + " Check if source/destination machine is a slow node. Check if merge phase "
                  + "time is more to understand disk bottlenecks in this node.  Check for skew";
            }
            result.add(comments);

            result.add(reduceInputGroupsVal + "");
            result.add(reduceInputRecordsVal + "");
            result.add("" + (1.0f * reduceInputGroupsVal / reduceInputRecordsVal));
            result.add(getCounterValue(TaskCounter.SHUFFLE_BYTES, counterGroupName, attemptInfo));

            //Total time taken for receiving all events from source tasks
            result.add(getOverheadFromSourceTasks(counterGroupName, attemptInfo));
            result.add(getCounterValue(TaskCounter.MERGE_PHASE_TIME, counterGroupName, attemptInfo));
            result.add(getCounterValue(TaskCounter.SHUFFLE_PHASE_TIME, counterGroupName, attemptInfo));


            result.add(Long.toString(timeTakenForRealWork));

            result.add(getCounterValue(TaskCounter.FIRST_EVENT_RECEIVED, counterGroupName, attemptInfo));
            result.add(getCounterValue(TaskCounter.LAST_EVENT_RECEIVED, counterGroupName, attemptInfo));
            result.add(getCounterValue(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT, counterGroupName, attemptInfo));

            csvResult.addRecord(result.toArray(new String[result.size()]));
          }
        }
      }
    }

  }

  /**
   * Time taken to receive all events from source tasks
   *
   * @param counterGroupName
   * @param attemptInfo
   * @return String
   */
  private String getOverheadFromSourceTasks(String counterGroupName, TaskAttemptInfo attemptInfo) {
    long firstEventReceived = Long.parseLong(getCounterValue(TaskCounter.FIRST_EVENT_RECEIVED,
        counterGroupName, attemptInfo));
    long lastEventReceived = Long.parseLong(getCounterValue(TaskCounter.LAST_EVENT_RECEIVED,
        counterGroupName, attemptInfo));
    return Long.toString(lastEventReceived - firstEventReceived);
  }

  private String getCounterValue(TaskCounter counter, String counterGroupName,
      TaskAttemptInfo attemptInfo) {
    Map<String, TezCounter> tezCounterMap = attemptInfo.getCounter(counter.toString());
    if (tezCounterMap != null) {
      for (Map.Entry<String, TezCounter> entry : tezCounterMap.entrySet()) {
        String groupName = entry.getKey();
        long val = entry.getValue().getValue();
        if (groupName.equals(counterGroupName)) {
          return Long.toString(val);
        }
      }
    }
    return "";
  }

  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Shuffle time analyzer";
  }

  @Override
  public String getDescription() {
    return "Analyze the time taken for shuffle, merge "
        + "and the real work done in the task";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }
}
