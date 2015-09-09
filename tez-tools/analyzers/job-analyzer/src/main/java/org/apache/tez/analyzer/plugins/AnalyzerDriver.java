/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.analyzer.plugins;

import org.apache.hadoop.util.ProgramDriver;

public class AnalyzerDriver {

  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("CriticalPath", CriticalPathAnalyzer.class,
          "Find the critical path of a DAG");
      pgd.addClass("ContainerReuseAnalyzer", ContainerReuseAnalyzer.class,
          "Print container reuse details in a DAG");
      pgd.addClass("LocalityAnalyzer", LocalityAnalyzer.class,
          "Print locality details in a DAG");
      pgd.addClass("ShuffleTimeAnalyzer", ShuffleTimeAnalyzer.class,
          "Analyze the shuffle time details in a DAG");
      pgd.addClass("SkewAnalyzer", SkewAnalyzer.class,
          "Analyze the skew details in a DAG");
      pgd.addClass("SlowestVertexAnalyzer", SlowestVertexAnalyzer.class,
          "Print slowest vertex details in a DAG");
      pgd.addClass("SlowNodeAnalyzer", SlowNodeAnalyzer.class,
          "Print node details in a DAG");
      pgd.addClass("SlowTaskIdentifier", SlowTaskIdentifier.class,
          "Print slow task details in a DAG");
      pgd.addClass("SpillAnalyzer", SpillAnalyzerImpl.class,
          "Print spill details in a DAG");
      pgd.addClass("TaskConcurrencyAnalyzer", TaskConcurrencyAnalyzer.class,
          "Print the task concurrency details in a DAG");
      pgd.addClass("VertexLevelCriticalPathAnalyzer", VertexLevelCriticalPathAnalyzer.class,
          "Find critical path at vertex level in a DAG");
      exitCode = pgd.run(argv);
    } catch(Throwable e){
      e.printStackTrace();
    }

    System.exit(exitCode);
  }

}