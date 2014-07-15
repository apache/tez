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

@Private
public class MultiStageMRConfigUtil {

  //////////////////////////////////////////////////////////////////////////////
  //                    Methods based on Stage Num                            //
  //////////////////////////////////////////////////////////////////////////////

  @Private
  public static int getNumIntermediateStages(Configuration conf) {
    return conf.getInt(MRJobConfig.MRR_INTERMEDIATE_STAGES, 0);
  }

  // TODO MRR FIXME based on conf format.
  // Intermediate stage numbers should start from 1.
  @Private
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

  @Private
  public static String getInitialMapVertexName() {
    return INITIAL_MAP_VERTEX_NAME;
  }

  @Private
  public static String getFinalReduceVertexName() {
    return FINAL_REDUCE_VERTEX_NAME;
  }

  @Private
  public static String getIntermediateStageVertexName(int stageNum) {
    return INTERMEDIATE_TASK_VERTEX_NAME_PREFIX + stageNum;
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
}
