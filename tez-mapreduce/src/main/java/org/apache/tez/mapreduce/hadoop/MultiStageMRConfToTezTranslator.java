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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;


public class MultiStageMRConfToTezTranslator {

  /**
   * Given a single base MRR config, returns a list of complete stage
   * configurations.
   * 
   * @param conf
   * @return list of complete stage configurations given Conifiguration
   */
  @Private
  public static Configuration[] getStageConfs(Configuration conf) {
    int numIntermediateStages = MultiStageMRConfigUtil
        .getNumIntermediateStages(conf);
    boolean hasFinalReduceStage = (conf.getInt(MRJobConfig.NUM_REDUCES, 0) > 0);
    // Assuming no 0 map jobs, and the first stage is always a map.
    int numStages = numIntermediateStages + (hasFinalReduceStage ? 2 : 1);

    // Read split info from HDFS
    conf.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, false);
    
    // Setup Tez partitioner class
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
        MRPartitioner.class.getName());
    
    // Setup Tez Combiner class if required.
    // This would already have been set since the call is via JobClient
    boolean useNewApi = conf.getBoolean("mapred.mapper.new-api", false);
    if (useNewApi) {
      if (conf.get(MRJobConfig.COMBINE_CLASS_ATTR) != null) {
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, MRCombiner.class.getName());
      }
    } else {
      if (conf.get("mapred.combiner.class") != null) {
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, MRCombiner.class.getName());
      }
    }

    Configuration confs[] = new Configuration[numStages];
    Configuration nonItermediateConf = MultiStageMRConfigUtil.extractStageConf(
        conf, "");
    if (numStages == 1) {
      confs[0] = nonItermediateConf;
      confs[0].setBoolean(MRConfig.IS_MAP_PROCESSOR, true);
    } else {
      confs[0] = nonItermediateConf;
      confs[numStages - 1] = new Configuration(nonItermediateConf);
      confs[numStages -1].setBoolean(MRConfig.IS_MAP_PROCESSOR, false);
    }
    if (numStages > 2) {
      for (int i = 1; i < numStages - 1; i++) {
        confs[i] = MultiStageMRConfigUtil.extractStageConf(conf,
            MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(i, ""));
        confs[i].setBoolean(MRConfig.IS_MAP_PROCESSOR, false);
      }
    }
    return confs;
  }
}