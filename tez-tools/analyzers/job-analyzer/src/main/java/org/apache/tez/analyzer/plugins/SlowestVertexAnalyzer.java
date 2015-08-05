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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
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
 * Identify the slowest vertex in the DAG.
 */
public class SlowestVertexAnalyzer implements Analyzer {

  private static final String[] headers = { "vertexName", "taskAttempts", "totalTime",
      "shuffleTime", "shuffleTime_Max", "LastEventReceived", "LastEventReceivedFrom",
      "TimeTaken_ForRealWork", "75thPercentile", "95thPercentile", "98thPercentile", "Median",
      "observation", "comments" };

  private final CSVResult csvResult = new CSVResult(headers);

  private final Configuration config;
  private final MetricRegistry metrics = new MetricRegistry();
  private Histogram taskAttemptRuntimeHistorgram;

  public SlowestVertexAnalyzer(Configuration config) {
    this.config = config;
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {

    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      String vertexName = vertexInfo.getVertexName();
      long totalTime = vertexInfo.getTimeTaken();

      long max = Long.MIN_VALUE;
      String maxSourceName = "";
      taskAttemptRuntimeHistorgram = metrics.histogram(vertexName);


      for (TaskAttemptInfo attemptInfo : vertexInfo.getTaskAttempts()) {

        taskAttemptRuntimeHistorgram.update(attemptInfo.getTimeTaken());

        //Get the last event received from the incoming vertices
        Map<String, TezCounter> lastEventReceivedMap = attemptInfo.getCounter(
            TaskCounter.LAST_EVENT_RECEIVED.toString());

        for (Map.Entry<String, TezCounter> entry : lastEventReceivedMap.entrySet()) {
          if (entry.getKey().equals(TaskCounter.class.getName())) {
            //TODO: Tez counters always ends up adding fgroups and groups, due to which we end up
            // getting TaskCounter details as well.
            continue;
          }
          //Find the slowest last event received
          if (entry.getValue().getValue() > max) {
            //w.r.t vertex start time.
            max =(attemptInfo.getStartTimeInterval() +  entry.getValue().getValue()) -
                (vertexInfo.getStartTimeInterval());
            maxSourceName = entry.getKey();
          }
        }
      }

      long shuffleMax = Long.MIN_VALUE;
      String shuffleMaxSource = "";
      for (TaskAttemptInfo attemptInfo : vertexInfo.getTaskAttempts()) {
        //Get the last event received from the incoming vertices
        Map<String, TezCounter> lastEventReceivedMap = attemptInfo.getCounter(
            TaskCounter.SHUFFLE_PHASE_TIME.toString());

        for (Map.Entry<String, TezCounter> entry : lastEventReceivedMap.entrySet()) {
          if (entry.getKey().equals(TaskCounter.class.getName())) {
            //ignore. TODO: hack for taskcounter issue
            continue;
          }
          //Find the slowest last event received
          if (entry.getValue().getValue() > shuffleMax) {
            //w.r.t vertex start time.
            shuffleMax =(attemptInfo.getStartTimeInterval() +  entry.getValue().getValue()) -
                (vertexInfo.getStartTimeInterval());
            shuffleMaxSource = entry.getKey();
          }
        }
      }

      String comments = "";

      List<String> record = Lists.newLinkedList();
      record.add(vertexName);
      record.add(vertexInfo.getTaskAttempts().size() + "");
      record.add(totalTime + "");
      record.add(Math.max(0, shuffleMax) + "");
      record.add(shuffleMaxSource);
      record.add(Math.max(0, max) + "");
      record.add(maxSourceName);
      record.add(Math.max(0,(totalTime - max)) + "");

      StringBuilder sb = new StringBuilder();
      double percentile75 = taskAttemptRuntimeHistorgram.getSnapshot().get75thPercentile();
      double percentile95 = taskAttemptRuntimeHistorgram.getSnapshot().get95thPercentile();
      double percentile98 = taskAttemptRuntimeHistorgram.getSnapshot().get98thPercentile();
      double percentile99 = taskAttemptRuntimeHistorgram.getSnapshot().get99thPercentile();
      double medianAttemptRuntime = taskAttemptRuntimeHistorgram.getSnapshot().getMedian();

      record.add("75th=" + percentile75);
      record.add("95th=" + percentile95);
      record.add("98th=" + percentile98);
      record.add("median=" + medianAttemptRuntime);

      if (percentile75 / percentile99 < 0.5) {
        //looks like some straggler task is there.
        sb.append("Looks like some straggler task is there");
      }

      record.add(sb.toString());

      if (totalTime > 0 && vertexInfo.getTaskAttempts().size() > 0) {
        if ((shuffleMax * 1.0f / totalTime) > 0.5) {
          if ((max * 1.0f / totalTime) > 0.5) {
            comments = "This vertex is slow due to its dependency on parent. Got a lot delayed last"
                + " event received";
          } else {
            comments =
                "Spending too much time on shuffle. Check shuffle bytes from previous vertex";
          }
        } else {
          if (totalTime > 10000) { //greater than 10 seconds. //TODO: Configure it later.
            comments = "Concentrate on this vertex (totalTime > 10 seconds)";
          }
        }
      }

      record.add(comments);
      csvResult.addRecord(record.toArray(new String[record.size()]));
    }
  }


  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "SlowVertexAnalyzer";
  }

  @Override
  public String getDescription() {
    return "Identify the slowest vertex in the DAG, which needs to be looked into first";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

}
