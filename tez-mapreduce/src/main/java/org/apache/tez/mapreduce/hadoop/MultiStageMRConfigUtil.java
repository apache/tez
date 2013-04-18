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

package org.apache.tez.mapreduce.hadoop;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import com.google.common.base.Preconditions;

public class MultiStageMRConfigUtil {

  // TODO MRR FIXME based on conf format.
  // Returns a complete conf object including non-intermediate stage conf.
  public static Configuration getIntermediateStageConf(Configuration baseConf,
      int i) {
    String base = getPropertyNameForStage(i, "");
    Configuration conf = new Configuration(false);
    Iterator<Entry<String, String>> confEntries = baseConf.iterator();
    while (confEntries.hasNext()) {
      Entry<String, String> entry = confEntries.next();
      String key = entry.getKey();
      if (key.startsWith(base)) {
        conf.set(key.replace(base, ""), entry.getValue());
      } else {
        conf.set(key, entry.getValue());
      }
    }
    return conf;
  }

  // TODO MRR FIXME based on conf format.
  // Returns config settings specific to stage-i only.
  public static Configuration getBasicIntermediateStageConf(
      Configuration baseConf, int i) {
    String base = getPropertyNameForStage(i, "");
    Configuration conf = new Configuration(false);
    Iterator<Entry<String, String>> confEntries = baseConf.iterator();
    while (confEntries.hasNext()) {
      Entry<String, String> entry = confEntries.next();
      String key = entry.getKey();
      if (key.startsWith(base)) {
        conf.set(key.replace(base, ""), entry.getValue());
      }
    }
    return conf;
  }

  // TODO MRR FIXME based on conf format.
  public static int getNumIntermediateStages(Configuration conf) {
    return conf.getInt(MRJobConfig.MRR_INTERMEDIATE_STAGES, 0);
  }

  // TODO MRR FIXME based on conf format.
  public static String getPropertyNameForStage(int intermediateStage,
      String originalPropertyName) {
    return MRJobConfig.MRR_INTERMEDIATE_STAGE_PREFIX + intermediateStage + "."
        + originalPropertyName;
  }

  public static void main(String[] args) {
    Configuration baseConf = new Configuration();
    baseConf.setInt(MRJobConfig.MRR_INTERMEDIATE_STAGES, 1);
    baseConf.setClass(MultiStageMRConfigUtil.getPropertyNameForStage(1,
        "mapreduce.job.combine.class"), IntSumReducer.class, Reducer.class);
    baseConf.setClass(MultiStageMRConfigUtil.getPropertyNameForStage(1,
        "mapreduce.job.reduce.class"), IntSumReducer.class, Reducer.class);

    Configuration conf = getBasicIntermediateStageConf(baseConf, 1);
    printConf(conf);
  }
  
  private static String IREDUCE_PREFIX = "ireduce";
  
  public static String getIntermediateReduceVertexName(int i) {
    return "ireduce" + i;
  }

  public static boolean isIntermediateReduceStage(String vertexName) {
    return vertexName.startsWith(IREDUCE_PREFIX);
  }
  
  public static int getIntermediateReduceStageNum(String vertexName) {
    Preconditions.checkArgument(vertexName.startsWith(IREDUCE_PREFIX),
        "IntermediateReduce vertex name must start with prefix: "
            + IREDUCE_PREFIX);
    String stageNumString = vertexName.substring(IREDUCE_PREFIX.length());
    return Integer.valueOf(stageNumString);
  }

  // TODO Get rid of this. Temporary for testing.
  public static void printConf(Configuration conf) {
    Iterator<Entry<String, String>> confEntries = conf.iterator();
    while (confEntries.hasNext()) {
      Entry<String, String> entry = confEntries.next();
      String key = entry.getKey();
      String value = entry.getValue();
      System.err.println("Key: " + key + ", Value: " + value);
    }
  }
}
