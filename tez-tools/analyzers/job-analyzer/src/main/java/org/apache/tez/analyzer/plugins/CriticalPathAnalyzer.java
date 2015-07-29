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

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.util.List;
import java.util.Map;

/**
 * Identify a set of vertices which fall in the critical path in a DAG.
 */
public class CriticalPathAnalyzer implements Analyzer {
  private final Configuration config;

  private static final String[] headers = { "CriticalPath", "Score" };

  private final CSVResult csvResult;

  public CriticalPathAnalyzer(Configuration config) {
    this.config = config;
    this.csvResult = new CSVResult(headers);
  }

  @Override public void analyze(DagInfo dagInfo) throws TezException {
    Map<String, Long> result = Maps.newLinkedHashMap();
    getCriticalPath("", dagInfo.getVertices().get(dagInfo.getVertices().size() - 1), 0, result);

    System.out.println();
    System.out.println();

    for (Map.Entry<String, Long> entry : sortByValues(result).entrySet()) {
      List<String> record = Lists.newLinkedList();
      record.add(entry.getKey());
      record.add(entry.getValue() + "");
      csvResult.addRecord(record.toArray(new String[record.size()]));
      System.out.println(entry.getKey() + ", " + entry.getValue());
    }
  }

  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "CriticalPathAnalyzer";
  }

  @Override
  public String getDescription() {
    return "Analyze critical path of the DAG";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  private static Map<String, Long> sortByValues(Map<String, Long> result) {
    //Sort result by time in reverse order
    final Ordering<String> reversValueOrdering =
        Ordering.natural().reverse().nullsLast().onResultOf(Functions.forMap(result, null));
    Map<String, Long> orderedMap = ImmutableSortedMap.copyOf(result, reversValueOrdering);
    return orderedMap;
  }

  private static void getCriticalPath(String predecessor, VertexInfo dest, long time,
      Map<String, Long> result) {
    String destVertexName = (dest != null) ? (dest.getVertexName()) : "";

    if (dest != null) {
      time += dest.getTimeTaken();
      predecessor += destVertexName + "-->";

      for (VertexInfo incomingVertex : dest.getInputVertices()) {
        getCriticalPath(predecessor, incomingVertex, time, result);
      }

      result.put(predecessor, time);
    }
  }
}
