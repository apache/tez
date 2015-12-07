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
import com.google.common.collect.Multimap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.Container;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.util.List;


/**
 * Get container reuse information at a per vertex level basis.
 */
public class ContainerReuseAnalyzer extends TezAnalyzerBase implements Analyzer {

  private final Configuration config;

  private static final String[] headers =
      { "vertexName", "taskAttempts", "node", "containerId", "reuseCount" };

  private final CSVResult csvResult;

  public ContainerReuseAnalyzer(Configuration config) {
    this.config = config;
    this.csvResult = new CSVResult(headers);
  }

  @Override
  public void analyze(DagInfo dagInfo) throws TezException {
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      Multimap<Container, TaskAttemptInfo> containers = vertexInfo.getContainersMapping();
      for (Container container : containers.keySet()) {
        List<String> record = Lists.newLinkedList();
        record.add(vertexInfo.getVertexName());
        record.add(vertexInfo.getTaskAttempts().size() + "");
        record.add(container.getHost());
        record.add(container.getId());
        record.add(Integer.toString(containers.get(container).size()));
        csvResult.addRecord(record.toArray(new String[record.size()]));
      }
    }
  }

  @Override
  public CSVResult getResult() throws TezException {
    return csvResult;
  }

  @Override
  public String getName() {
    return "Container Reuse Analyzer";
  }

  @Override
  public String getDescription() {
    return "Get details on container reuse analysis";
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    ContainerReuseAnalyzer analyzer = new ContainerReuseAnalyzer(config);
    int res = ToolRunner.run(config, analyzer, args);
    analyzer.printResults();
    System.exit(res);
  }
}
