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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
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
 * <p/>
 * Identify the skew (RECORD_INPUT_GROUPS / REDUCE_INPUT_RECORDS) ratio for all task attempts
 * and report if they are below a certain threshold.
 * <p/>
 * <p/>
 * - Case 1: Ratio of (reduce_input_groups / reduce_input_records) < 0.2  && SHUFFLE_BYTES > 1 GB
 * per task attempt from a source. This means couple of keys having too many records. Either
 * partitioning is wrong, or we need to increase memory limit for this vertex.
 * <p/>
 * - Case 2: Ratio of (reduce_input_groups / reduce_input_records) > 0.6 & Number of reduce input
 * records in task attempt is closer to say 60% of overall number of records
 * in vertex level & numTasks in vertex is greater than 1.  This might have any number of reducer
 * groups.  This means that, partitioning is wrong (can also consider reducing number of tasks
 * for that vertex). In some cases, too many reducers are launched and this can help find those.
 * <p/>
 * - Case 3: Ratio of (reduce_input_groups / reduce_input_records) is between 0.2 & 0.6 per task
 * attempt & numTasks is greater than 1 & SHUFFLE_BYTES > 1 GB per task attempt from a
 * source. This means, may be consider increasing parallelism based on the task attempt runtime.
 * <p/>
 */
public class SkewAnalyzer extends TezAnalyzerBase implements Analyzer {

  /**
   * Amount of bytes that was sent as shuffle bytes from source. If it is below this threshold,
   * it would not be considered for analysis.
   */
  private static final String SHUFFLE_BYTES_PER_ATTEMPT_PER_SOURCE = "tez.skew-analyzer.shuffle"
      + ".bytes.per.source";
  private static final long SHUFFLE_BYTES_PER_ATTEMPT_PER_SOURCE_DEFAULT = 900 * 1024 * 1024l;

  //Min reducer input group : reducer keys ratio for computation
  private static final String ATTEMPT_SHUFFLE_KEY_GROUP_MIN_RATIO = "tez.skew-analyzer.shuffle.key"
      + ".group.min.ratio";
  private static final float ATTEMPT_SHUFFLE_KEY_GROUP_MIN_RATIO_DEFAULT = 0.2f;

  //Max reducer input group : reducer keys ratio for computation
  private static final String ATTEMPT_SHUFFLE_KEY_GROUP_MAX_RATIO = "tez.skew-analyzer.shuffle.key"
      + ".group.max.ratio";
  private static final float ATTEMPT_SHUFFLE_KEY_GROUP_MAX_RATIO_DEFAULT = 0.4f;



  private static final String[] headers = { "vertexName", "taskAttemptId", "counterGroup", "node",
      "REDUCE_INPUT_GROUPS", "REDUCE_INPUT_RECORDS", "ratio", "SHUFFLE_BYTES", "timeTaken",
      "observation" };

  private final CSVResult csvResult = new CSVResult(headers);

  private final Configuration config;

  private final float minRatio;
  private final float maxRatio;
  private final long maxShuffleBytesPerSource;

  public SkewAnalyzer(Configuration config) {
    this.config = config;
    maxRatio = config.getFloat(ATTEMPT_SHUFFLE_KEY_GROUP_MAX_RATIO,
        ATTEMPT_SHUFFLE_KEY_GROUP_MAX_RATIO_DEFAULT);
    minRatio = config.getFloat(ATTEMPT_SHUFFLE_KEY_GROUP_MIN_RATIO,
        ATTEMPT_SHUFFLE_KEY_GROUP_MIN_RATIO_DEFAULT);
    maxShuffleBytesPerSource = config.getLong(SHUFFLE_BYTES_PER_ATTEMPT_PER_SOURCE,
        SHUFFLE_BYTES_PER_ATTEMPT_PER_SOURCE_DEFAULT);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    Preconditions.checkArgument(dagInfo != null, "DAG can't be null");
    analyzeReducers(dagInfo);
  }

  private void analyzeReducers(DagInfo dagInfo) {
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      for (TaskAttemptInfo attemptInfo : vertexInfo.getTaskAttempts()) {
        analyzeGroupSkewPerSource(attemptInfo);
        analyzeRecordSkewPerSource(attemptInfo);
        analyzeForParallelism(attemptInfo);
      }
    }
  }

  /**
   * Analyze scenario where couple keys are having too many records per source
   *
   * @param attemptInfo
   */
  private void analyzeGroupSkewPerSource(TaskAttemptInfo attemptInfo) {

    //counter_group (basically source) --> counter
    Map<String, TezCounter> reduceInputGroups = attemptInfo.getCounter(TaskCounter
        .REDUCE_INPUT_GROUPS.toString());
    Map<String, TezCounter> reduceInputRecords = attemptInfo.getCounter(TaskCounter
        .REDUCE_INPUT_RECORDS.toString());
    Map<String, TezCounter> shuffleBytes = attemptInfo.getCounter(TaskCounter.SHUFFLE_BYTES.toString());


    //tez counter for every source
    for (Map.Entry<String, TezCounter> entry : reduceInputGroups.entrySet()) {
      if (entry.getKey().equals(TaskCounter.class.getName())) {
        //TODO: Tez counters always ends up adding fgroups and groups, due to which we end up
        // getting TaskCounter details as well.
        continue;
      }

      String counterGroup = entry.getKey();
      long inputGroupsCount = entry.getValue().getValue();
      long inputRecordsCount = (reduceInputRecords.get(counterGroup) != null) ? reduceInputRecords
          .get(counterGroup).getValue() : 0;
      long shuffleBytesPerSource = (shuffleBytes.get(counterGroup) != null) ? shuffleBytes.get
          (counterGroup).getValue() : 0;

      float ratio = (inputGroupsCount * 1.0f / inputRecordsCount);

      //Case 1: Couple of keys having too many records per source.
      if (shuffleBytesPerSource > maxShuffleBytesPerSource) {
        if (ratio < minRatio) {
          List<String> result = Lists.newLinkedList();
          result.add(attemptInfo.getTaskInfo().getVertexInfo().getVertexName());
          result.add(attemptInfo.getTaskAttemptId());
          result.add(counterGroup);
          result.add(attemptInfo.getNodeId());
          result.add(inputGroupsCount + "");
          result.add(inputRecordsCount + "");
          result.add(ratio + "");
          result.add(shuffleBytesPerSource + "");
          result.add(attemptInfo.getTimeTaken() + "");
          result.add("Please check partitioning. Otherwise consider increasing memLimit");

          csvResult.addRecord(result.toArray(new String[result.size()]));
        }
      }
    }
  }

  /**
   * Analyze scenario where one task is getting > 60% of the vertex level records
   *
   * @param attemptInfo
   */
  private void analyzeRecordSkewPerSource(TaskAttemptInfo attemptInfo) {

    Map<String, TezCounter> vertexLevelReduceInputRecords =
        attemptInfo.getTaskInfo().getVertexInfo()
            .getCounter(TaskCounter.REDUCE_INPUT_RECORDS.toString());

    int vertexNumTasks = attemptInfo.getTaskInfo().getVertexInfo().getNumTasks();

    //counter_group (basically source) --> counter
    Map<String, TezCounter> reduceInputGroups = attemptInfo.getCounter(TaskCounter
        .REDUCE_INPUT_GROUPS.toString());
    Map<String, TezCounter> reduceInputRecords = attemptInfo.getCounter(TaskCounter
        .REDUCE_INPUT_RECORDS.toString());
    Map<String, TezCounter> shuffleBytes = attemptInfo.getCounter(TaskCounter.SHUFFLE_BYTES.toString());


    //tez counter for every source
    for (Map.Entry<String, TezCounter> entry : reduceInputGroups.entrySet()) {
      if (entry.getKey().equals(TaskCounter.class.getName())) {
        //TODO: Tez counters always ends up adding fgroups and groups, due to which we end up
        // getting TaskCounter details as well.
        continue;
      }

      String counterGroup = entry.getKey();
      long inputGroupsCount = entry.getValue().getValue();
      long inputRecordsCount = (reduceInputRecords.get(counterGroup) != null) ? reduceInputRecords
          .get(counterGroup).getValue() : 0;
      long shuffleBytesPerSource = (shuffleBytes.get(counterGroup) != null) ?shuffleBytes.get
          (counterGroup).getValue() : 0;
      long vertexLevelInputRecordsCount = (vertexLevelReduceInputRecords.get(counterGroup) !=
          null) ?
          vertexLevelReduceInputRecords.get(counterGroup).getValue() : 0;

      float ratio = (inputRecordsCount * 1.0f / vertexLevelInputRecordsCount);

      if (vertexNumTasks > 1) {
        if (ratio > maxRatio) {
          //input records > 60% of vertex level record count
          if (inputRecordsCount > (vertexLevelInputRecordsCount * 0.60)) {
            List<String> result = Lists.newLinkedList();
            result.add(attemptInfo.getTaskInfo().getVertexInfo().getVertexName());
            result.add(attemptInfo.getTaskAttemptId());
            result.add(counterGroup);
            result.add(attemptInfo.getNodeId());
            result.add(inputGroupsCount + "");
            result.add(inputRecordsCount + "");
            result.add(ratio + "");
            result.add(shuffleBytesPerSource + "");
            result.add(attemptInfo.getTimeTaken() + "");
            result.add("Some task attempts are getting > 60% of reduce input records. "
                + "Consider adjusting parallelism & check partition logic");

            csvResult.addRecord(result.toArray(new String[result.size()]));

          }
        }
      }
    }
  }

  /**
   * Analyze scenario where a vertex would need to increase parallelism
   *
   * @param attemptInfo
   */
  private void analyzeForParallelism(TaskAttemptInfo attemptInfo) {

    //counter_group (basically source) --> counter
    Map<String, TezCounter> reduceInputGroups = attemptInfo.getCounter(TaskCounter
        .REDUCE_INPUT_GROUPS.toString());
    Map<String, TezCounter> reduceInputRecords = attemptInfo.getCounter(TaskCounter
        .REDUCE_INPUT_RECORDS.toString());
    Map<String, TezCounter> shuffleBytes = attemptInfo.getCounter(TaskCounter.SHUFFLE_BYTES.toString());

    //tez counter for every source
    for (Map.Entry<String, TezCounter> entry : reduceInputGroups.entrySet()) {
      if (entry.getKey().equals(TaskCounter.class.getName())) {
        //TODO: Tez counters always ends up adding fgroups and groups, due to which we end up
        // getting TaskCounter details as well.
        continue;
      }

      String counterGroup = entry.getKey();
      long inputGroupsCount = entry.getValue().getValue();
      long inputRecordsCount = (reduceInputRecords.get(counterGroup) != null) ? reduceInputRecords
          .get(counterGroup).getValue() : 0;
      long shuffleBytesPerSource = (shuffleBytes.get(counterGroup) != null) ? shuffleBytes.get
          (counterGroup).getValue() : 0;

      float ratio = (inputGroupsCount * 1.0f / inputRecordsCount);

      //Case 3: Shuffle_Bytes > 1 GB.  Ratio between 0.2 & < 0.6. Consider increasing
      // parallelism based on task runtime.
      if (shuffleBytesPerSource > SHUFFLE_BYTES_PER_ATTEMPT_PER_SOURCE_DEFAULT) {
        if (ratio > minRatio && ratio < maxRatio) {
          //couple of keys have too many records. Classic case of partition issue.
          List<String> result = Lists.newLinkedList();
          result.add(attemptInfo.getTaskInfo().getVertexInfo().getVertexName());
          result.add(attemptInfo.getTaskAttemptId());
          result.add(counterGroup);
          result.add(attemptInfo.getNodeId());
          result.add(inputGroupsCount + "");
          result.add(inputRecordsCount + "");
          result.add(ratio + "");
          result.add(shuffleBytesPerSource + "");
          result.add(attemptInfo.getTimeTaken() + "");
          result.add("Consider increasing parallelism.");

          csvResult.addRecord(result.toArray(new String[result.size()]));
        }
      }
    }


  }


  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Skew Analyzer";
  }

  @Override
  public String getDescription() {
    return "Analyzer reducer skews by mining reducer task counters";
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    SkewAnalyzer analyzer = new SkewAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
