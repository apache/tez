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
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.util.List;
import java.util.Map;


/**
 * Get locality information for tasks for vertices and get their task execution times.
 * This would be helpeful to co-relate if the vertex runtime is anyways related to the data
 * locality.
 */
public class LocalityAnalyzer extends TezAnalyzerBase implements Analyzer {

  private final String[] headers = { "vertexName", "numTasks", "dataLocalRatio", "rackLocalRatio",
      "otherRatio", "avgDataLocalTaskRuntime", "avgRackLocalTaskRuntime",
      "avgOtherLocalTaskRuntime", "noOfInputs", "avgHDFSBytesRead_DataLocal",
      "avgHDFSBytesRead_RackLocal", "avgHDFSBytesRead_Others", "recommendation" };

  private static final String DATA_LOCAL_RATIO = "tez.locality-analyzer.data.local.ratio";
  private static final float DATA_LOCAL_RATIO_DEFAULT = 0.5f;

  private final Configuration config;

  private final CSVResult csvResult;

  public LocalityAnalyzer(Configuration config) {
    this.config = config;
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      String vertexName = vertexInfo.getVertexName();

      Map<String, TezCounter> dataLocalTask = vertexInfo.getCounter(DAGCounter.class.getName(),
          DAGCounter.DATA_LOCAL_TASKS.toString());
      Map<String, TezCounter> rackLocalTask = vertexInfo.getCounter(DAGCounter.class.getName(),
          DAGCounter.RACK_LOCAL_TASKS.toString());

      long dataLocalTasks = 0;
      long rackLocalTasks = 0;

      if (!dataLocalTask.isEmpty()) {
        dataLocalTasks = dataLocalTask.get(DAGCounter.class.getName()).getValue();
      }

      if (!rackLocalTask.isEmpty()) {
        rackLocalTasks = rackLocalTask.get(DAGCounter.class.getName()).getValue();
      }

      long totalVertexTasks = vertexInfo.getNumTasks();

      if (dataLocalTasks > 0 || rackLocalTasks > 0) {
        //compute locality details.
        float dataLocalRatio = dataLocalTasks * 1.0f / totalVertexTasks;
        float rackLocalRatio = rackLocalTasks * 1.0f / totalVertexTasks;
        float othersRatio = (totalVertexTasks - (dataLocalTasks + rackLocalTasks)) * 1.0f /
            totalVertexTasks;

        List<String> record = Lists.newLinkedList();
        record.add(vertexName);
        record.add(totalVertexTasks + "");
        record.add(dataLocalRatio + "");
        record.add(rackLocalRatio + "");
        record.add(othersRatio + "");

        TaskAttemptDetails dataLocalResult = computeAverages(vertexInfo,
            DAGCounter.DATA_LOCAL_TASKS);
        TaskAttemptDetails rackLocalResult = computeAverages(vertexInfo,
            DAGCounter.RACK_LOCAL_TASKS);
        TaskAttemptDetails otherTaskResult = computeAverages(vertexInfo,
            DAGCounter.OTHER_LOCAL_TASKS);

        record.add(dataLocalResult.avgRuntime + "");
        record.add(rackLocalResult.avgRuntime + "");
        record.add(otherTaskResult.avgRuntime + "");

        //Get the number of inputs to this vertex
        record.add(vertexInfo.getInputEdges().size()
            + vertexInfo.getAdditionalInputInfoList().size() + "");

        //Get the avg HDFS bytes read in this vertex for different type of locality
        record.add(dataLocalResult.avgHDFSBytesRead + "");
        record.add(rackLocalResult.avgHDFSBytesRead + "");
        record.add(otherTaskResult.avgHDFSBytesRead + "");

        String recommendation = "";
        if (dataLocalRatio < config.getFloat(DATA_LOCAL_RATIO, DATA_LOCAL_RATIO_DEFAULT)) {
          recommendation = "Data locality is poor for this vertex. Try tuning "
              + TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS + ", "
              + TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED + ", "
              + TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED;
        }

        record.add(recommendation);
        csvResult.addRecord(record.toArray(new String[record.size()]));
      }
    }
  }

  /**
   * Compute counter averages for specific vertex
   *
   * @param vertexInfo
   * @param counter
   * @return task attempt details
   */
  private TaskAttemptDetails computeAverages(VertexInfo vertexInfo, DAGCounter counter) {
    long totalTime = 0;
    long totalTasks = 0;
    long totalHDFSBytesRead = 0;

    TaskAttemptDetails result = new TaskAttemptDetails();

    for(TaskAttemptInfo attemptInfo : vertexInfo.getTaskAttempts()) {
      Map<String, TezCounter> localityCounter = attemptInfo.getCounter(DAGCounter.class.getName(),
          counter.toString());

      if (!localityCounter.isEmpty() &&
          localityCounter.get(DAGCounter.class.getName()).getValue() > 0) {
        totalTime += attemptInfo.getTimeTaken();
        totalTasks++;

        //get HDFSBytes read counter
        Map<String, TezCounter> hdfsBytesReadCounter = attemptInfo.getCounter(FileSystemCounter
            .class.getName(), FileSystemCounter.HDFS_BYTES_READ.name());
        for(Map.Entry<String, TezCounter> entry : hdfsBytesReadCounter.entrySet()) {
          totalHDFSBytesRead += entry.getValue().getValue();
        }
      }
    }
    if (totalTasks > 0) {
      result.avgRuntime = (totalTime * 1.0f / totalTasks);
      result.avgHDFSBytesRead = (totalHDFSBytesRead * 1.0f / totalTasks);
    }
    return result;
  }

  @Override public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override public String getName() {
    return "Locality Analyzer";
  }

  @Override public String getDescription() {
    return "Analyze for locality information (data local, rack local, off-rack)";
  }

  @Override public Configuration getConfiguration() {
    return config;
  }

  /**
   * Placeholder for task attempt details
   */
  static class TaskAttemptDetails {
    float avgHDFSBytesRead;
    float avgRuntime;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    LocalityAnalyzer analyzer = new LocalityAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
