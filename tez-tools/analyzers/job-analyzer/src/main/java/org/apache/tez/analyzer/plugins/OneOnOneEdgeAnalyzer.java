/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.analyzer.plugins;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.EdgeInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * When 1:1 edge is configured, InputReadyVertexManager adds task location affinity
 * so that the downstream tasks gets a chance to get scheduled on the same node where the
 * data was generated. This analyzer helps in finding out the tasks which were not
 * scheduled on the same node as source and ended up doing network downloads.
 * </p>
 */
public class OneOnOneEdgeAnalyzer extends TezAnalyzerBase implements Analyzer {

  private static final Logger LOG = LoggerFactory.getLogger(OneOnOneEdgeAnalyzer.class);

  private final String[] headers = { "sourceVertex", "downstreamVertex", "srcTaskId",
      "srcContainerHost", "destContainerHost" };

  // DataMovementType::ONE_TO_ONE
  private static final String ONE_TO_ONE = "ONE_TO_ONE";
  private final Configuration config;

  private final CSVResult csvResult;

  public OneOnOneEdgeAnalyzer(Configuration config) {
    this.config = config;
    csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    for (VertexInfo v : dagInfo.getVertices()) {
      for (EdgeInfo e : v.getOutputEdges()) {
        if (e.getDataMovementType() != null && e.getDataMovementType().equals(ONE_TO_ONE)) {
          LOG.info("Src --> Dest : {} --> {}", e.getSourceVertex(), e.getDestinationVertex());

          VertexInfo sourceVertex = e.getSourceVertex();
          VertexInfo destinationVertex = e.getDestinationVertex();

          Map<Integer, String> sourceTaskToContainerMap =
              getContainerMappingForVertex(sourceVertex);
          Map<Integer, String> downStreamTaskToContainerMap =
              getContainerMappingForVertex(destinationVertex);

          int missedCounter = 0;
          List<String> result = Lists.newLinkedList();
          for (Map.Entry<Integer, String> entry : sourceTaskToContainerMap.entrySet()) {
            Integer taskId = entry.getKey();
            String sourceContainerHost = entry.getValue();

            // check on downstream vertex.
            String downstreamContainerHost = downStreamTaskToContainerMap.get(taskId);
            if (downstreamContainerHost != null) {
              if (!sourceContainerHost.equalsIgnoreCase(downstreamContainerHost)) {
               // downstream task got scheduled on different machine than src
                LOG.info("TaskID: {}, source: {}, downStream:{}",
                    taskId, sourceContainerHost, downstreamContainerHost);
                result.add(sourceVertex.getVertexName());
                result.add(destinationVertex.getVertexName());
                result.add(taskId + "");
                result.add(sourceContainerHost);
                result.add(downstreamContainerHost);
                csvResult.addRecord(result.toArray(new String[result.size()]));

                missedCounter++;
              }
            }
           result.clear();
          }
          LOG.info("Total tasks:{}, miss: {}", sourceTaskToContainerMap.size(), missedCounter);
        }
      }
    }
  }

  private Map<Integer, String> getContainerMappingForVertex(VertexInfo vertexInfo) {
    Map<Integer, String> taskToContainerMap = Maps.newHashMap();
    for (TaskInfo taskInfo : vertexInfo.getTasks()) {
      TaskAttemptInfo successfulAttempt = taskInfo.getSuccessfulTaskAttempt();
      if (successfulAttempt != null) {
        TezTaskAttemptID id = TezTaskAttemptID.fromString(successfulAttempt.getTaskAttemptId());
        if (id != null) {
          taskToContainerMap
              .put(id.getTaskID().getId(), successfulAttempt.getContainer().getHost());
        }
      }
    }
    return taskToContainerMap;
  }

  @Override
  public Result getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "One-to-One edge analyzer";
  }

  @Override
  public String getDescription() {
    return "To understand the locality miss in 1:1 edge";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    OneOnOneEdgeAnalyzer analyzer = new OneOnOneEdgeAnalyzer(conf);
    int res = ToolRunner.run(conf, analyzer, args);
    System.exit(res);
  }
}

