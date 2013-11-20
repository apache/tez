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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;

public class MultiStageMRConfigUtil {

  //////////////////////////////////////////////////////////////////////////////
  //                    Methods based on Stage Num                            //
  //////////////////////////////////////////////////////////////////////////////

  // Returns config settings specific to stage
  public static Configuration getBasicIntermediateStageConf(
      Configuration baseConf, int i) {
    return getBasicIntermediateStageConfInternal(baseConf,
        getPropertyNameForIntermediateStage(i, ""), false, true);
  }

  // Returns and removes config settings specific to stage
  public static Configuration getAndRemoveBasicIntermediateStageConf(
      Configuration baseConf, int i) {
    return getBasicIntermediateStageConfInternal(baseConf,
        getPropertyNameForIntermediateStage(i, ""), true, true);
  }

  // TODO Get rid of this once YARNRunner starts using VertexNames.
  public static Configuration getIntermediateStageConf(Configuration baseConf,
      int i) {
    return getBasicIntermediateStageConfInternal(baseConf,
        getPropertyNameForIntermediateStage(i, ""), false, false);
  }

  // FIXME small perf hit. Change this to parse through all keys once and
  // generate objects per
  // stage instead of scanning through conf multiple times.
  public static Configuration getAndRemoveBasicNonIntermediateStageConf(
      Configuration baseConf) {
    Configuration newConf = new Configuration(false);
    for (String key : DeprecatedKeys.getMRToTezRuntimeParamMap().keySet()) {
      if (baseConf.get(key) != null) {
        newConf.set(key, baseConf.get(key));
        baseConf.unset(key);
      }
    }

    for (String key : DeprecatedKeys.getMultiStageParamMap().keySet()) {
      if (baseConf.get(key) != null) {
        newConf.set(key, baseConf.get(key));
        baseConf.unset(key);
      }
    }
    return newConf;
  }

  // TODO MRR FIXME based on conf format.
  public static int getNumIntermediateStages(Configuration conf) {
    return conf.getInt(MRJobConfig.MRR_INTERMEDIATE_STAGES, 0);
  }

  // TODO MRR FIXME based on conf format.
  // Intermediate stage numbers should start from 1.
  public static String getPropertyNameForIntermediateStage(
      int intermediateStage, String originalPropertyName) {
    return MRJobConfig.MRR_INTERMEDIATE_STAGE_PREFIX + intermediateStage + "."
        + originalPropertyName;
  }
 
 //////////////////////////////////////////////////////////////////////////////
 //                  Methods based on Vertex Name                            //
 //////////////////////////////////////////////////////////////////////////////
  
  private static final String INITIAL_MAP_VERTEX_NAME = "initialmap";
  private static final String FINAL_REDUCE_VERTEX_NAME = "finalreduce";
  private static final String INTERMEDIATE_TASK_VERTEX_NAME_PREFIX = "ivertex";

  public static String getInitialMapVertexName() {
    return INITIAL_MAP_VERTEX_NAME;
  }
  
  public boolean isInitialMapVertex(String vertexName) {
    return vertexName.equals(INITIAL_MAP_VERTEX_NAME);
  }

  public static String getFinalReduceVertexName() {
    return FINAL_REDUCE_VERTEX_NAME;
  }

  public boolean isFinalReduceVertex(String vertexName) {
    return vertexName.equals(FINAL_REDUCE_VERTEX_NAME);
  }

  public static String getIntermediateStageVertexName(int stageNum) {
    return INTERMEDIATE_TASK_VERTEX_NAME_PREFIX + stageNum;
  }
  
  public static int getIntermediateStageNum(String vertexName) {
    if (vertexName.matches(INTERMEDIATE_TASK_VERTEX_NAME_PREFIX + "\\d+")) {
      return Integer.parseInt(vertexName
          .substring(INTERMEDIATE_TASK_VERTEX_NAME_PREFIX.length()));
    } else {
      return -1;
    }
  }

  // Returns config settings specific to named vertex
  public static Configuration getBasicConfForVertex(Configuration baseConf,
      String vertexName) {
    return getBasicIntermediateStageConfInternal(baseConf,
        getPropertyNameForVertex(vertexName, ""), false, true);
  }

  // Returns and removes config settings specific to named vertex
  public static Configuration getAndRemoveBasicConfForVertex(
      Configuration baseConf, String vertexName) {
    return getBasicIntermediateStageConfInternal(baseConf,
        getPropertyNameForVertex(vertexName, ""), true, true);
  }

  // Returns a config with all parameters, and vertex specific params moved to
  // the top level.
  public static Configuration getConfForVertex(Configuration baseConf,
      String vertexName) {
    return getBasicIntermediateStageConfInternal(baseConf,
        getPropertyNameForVertex(vertexName, ""), false, false);
  }

  public static void addConfigurationForVertex(Configuration baseConf,
      String vertexName, Configuration vertexConf) {
    Iterator<Entry<String, String>> confEntries = vertexConf.iterator();
    while (confEntries.hasNext()) {
      Entry<String, String> entry = confEntries.next();
      baseConf.set(getPropertyNameForVertex(vertexName, entry.getKey()),
          entry.getValue());
    }
  }

  // TODO This is TezEngineLand
  public static String getPropertyNameForVertex(String vertexName,
      String originalPropertyName) {
    return MRJobConfig.MRR_VERTEX_PREFIX + vertexName + "."
        + originalPropertyName;
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

  @Private
  static Configuration extractStageConf(Configuration baseConf,
      String prefix) {
    Configuration strippedConf = new Configuration(false);
    Configuration conf = new Configuration(false);
    Iterator<Entry<String, String>> confEntries = baseConf.iterator();
    while (confEntries.hasNext()) {
      Entry<String, String> entry = confEntries.next();
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        // Ignore keys for other intermediate stages in case of an initial or final stage.
        if (prefix.equals("") && key.startsWith(MRJobConfig.MRR_INTERMEDIATE_STAGE_PREFIX)) {
          continue;
        }
        String newKey = key.replace(prefix, "");
        strippedConf.set(newKey, entry.getValue());
      } else {
        // Ignore keys for other intermediate stages.
        if (key.startsWith(MRJobConfig.MRR_INTERMEDIATE_STAGE_PREFIX)) {
          continue;
        }
        // Set all base keys in the new conf
        conf.set(key, entry.getValue());
      }
    }
    // Replace values from strippedConf into the finalConf. Override values
    // which may have been copied over from the baseConf root level.
    Iterator<Entry<String, String>> entries = strippedConf.iterator();
    while (entries.hasNext()) {
      Entry<String, String> entry = entries.next();
      if (!Configuration.isDeprecated(entry.getKey())) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }
  
  // TODO MRR FIXME based on conf format.
  private static Configuration getBasicIntermediateStageConfInternal(
      Configuration baseConf, String prefix, boolean remove, boolean stageOnly) {
    Configuration strippedConf = new Configuration(false);
    Configuration conf = new Configuration(false);
    Iterator<Entry<String, String>> confEntries = baseConf.iterator();
    while (confEntries.hasNext()) {
      Entry<String, String> entry = confEntries.next();
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        if (remove) {
          baseConf.unset(key);
        }
        String newKey = key.replace(prefix, "");
        strippedConf.set(newKey, entry.getValue());
      } else if (!stageOnly) {
        conf.set(key, entry.getValue());
      }
    }
    // Replace values from strippedConf into the finalConf. Override values
    // which may have been copied over from the baseConf root level.
    if (stageOnly) {
      conf = strippedConf;
    } else {
      Iterator<Entry<String, String>> entries = strippedConf.iterator();
      while (entries.hasNext()) {
        Entry<String, String> entry = entries.next();
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }
}
