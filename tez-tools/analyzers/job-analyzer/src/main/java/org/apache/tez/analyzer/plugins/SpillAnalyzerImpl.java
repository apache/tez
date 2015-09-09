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
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

import java.util.List;
import java.util.Map;


/**
 * Find out tasks which have more than 1 spill (ADDITIONAL_SPILL_COUNT).
 * <p/>
 * Accompany this with OUTPUT_BYTES (> 1 GB data written)
 */
public class SpillAnalyzerImpl extends TezAnalyzerBase implements Analyzer {

  private static final String[] headers = { "vertexName", "taskAttemptId",
      "Node", "counterGroupName",
      "spillCount", "taskDuration",
      "OUTPUT_BYTES", "OUTPUT_RECORDS",
      "SPILLED_RECORDS", "Recommendation" };

  private final CSVResult csvResult;

  /**
   * Minimum output bytes that should be chunrned out by a task
   */
  private static final String OUTPUT_BYTES_THRESHOLD = "tez.spill-analyzer.min.output.bytes"
      + ".threshold";
  private static long OUTPUT_BYTES_THRESHOLD_DEFAULT = 1 * 1024 * 1024 * 1024l;

  private final long minOutputBytesPerTask;

  private final Configuration config;

  public SpillAnalyzerImpl(Configuration config) {
    this.config = config;
    minOutputBytesPerTask = Math.max(0, config.getLong(OUTPUT_BYTES_THRESHOLD,
        OUTPUT_BYTES_THRESHOLD_DEFAULT));
    this.csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      String vertexName = vertexInfo.getVertexName();

      for (TaskAttemptInfo attemptInfo : vertexInfo.getTaskAttempts()) {
        //Get ADDITIONAL_SPILL_COUNT, OUTPUT_BYTES for every source
        Map<String, TezCounter> spillCountMap =
            attemptInfo.getCounter(TaskCounter.ADDITIONAL_SPILL_COUNT.name());
        Map<String, TezCounter> spilledRecordsMap =
            attemptInfo.getCounter(TaskCounter.SPILLED_RECORDS.name());
        Map<String, TezCounter> outputRecordsMap =
            attemptInfo.getCounter(TaskCounter.OUTPUT_RECORDS.name());

        Map<String, TezCounter> outputBytesMap =
            attemptInfo.getCounter(TaskCounter.OUTPUT_BYTES.name());

        for (Map.Entry<String, TezCounter> entry : spillCountMap.entrySet()) {
          String source = entry.getKey();
          long spillCount = entry.getValue().getValue();
          long outBytes = outputBytesMap.get(source).getValue();

          long outputRecords = outputRecordsMap.get(source).getValue();
          long spilledRecords = spilledRecordsMap.get(source).getValue();

          if (spillCount > 1 && outBytes > minOutputBytesPerTask) {
            List<String> recorList = Lists.newLinkedList();
            recorList.add(vertexName);
            recorList.add(attemptInfo.getTaskAttemptId());
            recorList.add(attemptInfo.getNodeId());
            recorList.add(source);
            recorList.add(spillCount + "");
            recorList.add(attemptInfo.getTimeTaken() + "");
            recorList.add(outBytes + "");
            recorList.add(outputRecords + "");
            recorList.add(spilledRecords + "");
            recorList.add("Consider increasing " + TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB
                + ". Try increasing container size.");

            csvResult.addRecord(recorList.toArray(new String[recorList.size()]));
          }
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
    return "SpillAnalyzer";
  }

  @Override
  public String getDescription() {
    return "Analyze spill details in the task";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    SpillAnalyzerImpl analyzer = new SpillAnalyzerImpl(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
