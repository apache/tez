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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys.MultiStageKeys;

public class MultiStageMRConfToTezTranslator {

  private enum DeprecationReason {
    DEPRECATED_DIRECT_TRANSLATION, DEPRECATED_MULTI_STAGE
  }

  // FIXME Add unit tests.
  // This will convert configs to tez.<vertexName>.<OriginalProperty> for
  // properties which it understands. Doing this for the initial and final task
  // as well to verify functionality.
  //

  // TODO Set the cause properly.
  public static Configuration convertMRToLinearTez(Configuration srcConf) {
    Configuration newConf = new Configuration(srcConf);

    int numIntermediateStages = MultiStageMRConfigUtil
        .getNumIntermediateStages(srcConf);
    boolean hasFinalReduceStage = (srcConf.getInt(MRJobConfig.NUM_REDUCES, 0) > 0);

    // Assuming no 0 map jobs, and the first stage is always a map.
    int totalStages = numIntermediateStages + (hasFinalReduceStage ? 2 : 1);
    int numEdges = totalStages - 1;

    Configuration[] allConfs = extractStageConfs(newConf, numEdges);

    for (int i = 0; i < allConfs.length; i++) {
      processDirectConversion(allConfs[i]);
    }
    for (int i = 0; i < allConfs.length - 1; i++) {
      processMultiStageDepreaction(allConfs[i], allConfs[i + 1]);
    }
    // Unset unnecessary keys in the last stage. Will end up being called for
    // single stage as well which should be harmless.
    processMultiStageDepreaction(allConfs[allConfs.length - 1], null);

    for (int i = 0; i < allConfs.length; i++) {
      String vertexName;
      if (i == 0) {
        vertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
      } else if (i == allConfs.length - 1) {
        vertexName = MultiStageMRConfigUtil.getFinalReduceVertexName();
      } else {
        // Intermediate vertices start at 1
        vertexName = MultiStageMRConfigUtil.getIntermediateStageVertexName(i);
      }
      MultiStageMRConfigUtil.addConfigurationForVertex(newConf, vertexName,
          allConfs[i]);
    }

    return newConf;
  }

  /**
   * Constructs a list containing individual configuration for each stage of the
   * linear MR job, including the first map and last reduce if applicable.
   */
  private static Configuration[] extractStageConfs(Configuration conf,
      int totalEdges) {
    int numStages = totalEdges + 1;
    Configuration confs[] = new Configuration[numStages];
    // TODO Make moer efficient instead of multiple scans.
    Configuration nonIntermediateConf = MultiStageMRConfigUtil
        .getAndRemoveBasicNonIntermediateStageConf(conf);
    if (numStages == 1) {
      confs[0] = nonIntermediateConf;
    } else {
      confs[0] = nonIntermediateConf;
      confs[numStages - 1] = new Configuration(nonIntermediateConf);
    }
    if (numStages > 2) {
      for (int i = 1; i < numStages - 1; i++) {
        confs[i] = MultiStageMRConfigUtil
            .getAndRemoveBasicIntermediateStageConf(conf, i);
      }
    } else {

    }

    return confs;
  }

  private static void processDirectConversion(Configuration conf) {
    for (Entry<String, String> dep : DeprecatedKeys.getMRToEngineParamMap()
        .entrySet()) {
      if (conf.get(dep.getKey()) != null) {
        // TODO Deprecation reason does not seem to reflect in the config ?
        conf.set(dep.getValue(), conf.get(dep.getKey()),
            DeprecationReason.DEPRECATED_DIRECT_TRANSLATION.name());
        conf.unset(dep.getKey());
      }
    }
  }

  private static void processMultiStageDepreaction(Configuration srcConf,
      Configuration destConf) {

    // All MR keys which need such translation are specified at src - hence,
    // this is ok.
    // No key exists in which the map is inferring something based on the reduce
    // value.
    for (Entry<String, Map<MultiStageKeys, String>> dep : DeprecatedKeys
        .getMultiStageParamMap().entrySet()) {
      if (srcConf.get(dep.getKey()) != null) {
        if (destConf != null) {
          srcConf.set(dep.getValue().get(MultiStageKeys.OUTPUT),
              srcConf.get(dep.getKey()));
          destConf.set(dep.getValue().get(MultiStageKeys.INPUT),
              srcConf.get(dep.getKey()));
          srcConf.unset(dep.getKey());
        } else { // Last stage. Just remove the key reference.
          srcConf.unset(dep.getKey());
        }
      }
    }
  }
}
