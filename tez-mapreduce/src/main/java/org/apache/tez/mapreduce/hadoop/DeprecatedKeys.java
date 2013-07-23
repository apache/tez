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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.Constants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.TezConfiguration;
import com.google.common.collect.Maps;

public class DeprecatedKeys {

  
  
  // This could be done via deprecation.
  /**
   * Keys used by the DAG - mainly the AM. 
   */
  private static Map<String, String> mrParamToDAGParamMap = new HashMap<String, String>();

  
  public static enum MultiStageKeys {
    INPUT, OUTPUT
  }
  /**
   * Keys which are used across an edge. i.e. by an Output-Input pair.
   */
  private static Map<String, Map<MultiStageKeys, String>> multiStageParamMap =
      new HashMap<String, Map<MultiStageKeys, String>>();
  
  
  /**
   * Keys used by the engine.
   */
  private static Map<String, String> mrParamToEngineParamMap =
      new HashMap<String, String>();

  
 
  static {
    populateMRToEngineParamMap();
    populateMRToDagParamMap();
    populateMultiStageParamMap();
    addDeprecatedKeys();
  }
  
  
  private static void populateMultiStageParamMap() {
    
    multiStageParamMap.put(
        MRJobConfig.KEY_COMPARATOR,
        getDeprecationMap(
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_INPUT_KEY_COMPARATOR_CLASS,
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_OUTPUT_KEY_COMPARATOR_CLASS));
    
    multiStageParamMap.put(
        MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        getDeprecationMap(
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_INPUT_KEY_CLASS,
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_OUTPUT_KEY_CLASS));
    
    multiStageParamMap.put(
        MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        getDeprecationMap(
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_INPUT_VALUE_CLASS,
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_OUTPUT_VALUE_CLASS));
    
    multiStageParamMap.put(
        MRJobConfig.MAP_OUTPUT_COMPRESS,
        getDeprecationMap(
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_INPUT_IS_COMPRESSED,
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS));
    
    multiStageParamMap.put(
        MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC,
        getDeprecationMap(
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_INPUT_COMPRESS_CODEC,
            TezJobConfig.TEZ_ENGINE_INTERMEDIATE_OUTPUT_COMPRESS_CODEC));
  }
  
  private static Map<MultiStageKeys, String> getDeprecationMap(String inputKey, String outputKey) {
    Map<MultiStageKeys, String>  m = Maps.newEnumMap(MultiStageKeys.class);
    m.put(MultiStageKeys.INPUT, inputKey);
    m.put(MultiStageKeys.OUTPUT, outputKey);
    return m;
  }
  
  private static void populateMRToDagParamMap() {
    mrParamToDAGParamMap.put(MRJobConfig.JOB_SUBMIT_DIR,
        TezConfiguration.JOB_SUBMIT_DIR);
    mrParamToDAGParamMap.put(MRJobConfig.APPLICATION_TOKENS_FILE,
        TezConfiguration.APPLICATION_TOKENS_FILE);

    // TODO Default value handling.
    mrParamToDAGParamMap.put(MRJobConfig.MR_AM_TASK_LISTENER_THREAD_COUNT,
        TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT);
    
    mrParamToDAGParamMap.put(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER,
        TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE);
    mrParamToDAGParamMap.put(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE,
        TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED);
    mrParamToDAGParamMap.put(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT,
        TezConfiguration.TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD);
  }

  // TODO TEZAM4 Sometime, make sure this gets loaded by default. Insteaf of the current initialization in MRAppMaster, TezChild.
  // Maybe define in an TEZConfiguration / TEZ JobConf variant.
  
  public static void init() {
  }
  
  private static void populateMRToEngineParamMap() {
    
    registerMRToEngineKeyTranslation(MRConfig.MAPRED_IFILE_READAHEAD, TezJobConfig.TEZ_ENGINE_IFILE_READAHEAD);

    registerMRToEngineKeyTranslation(MRConfig.MAPRED_IFILE_READAHEAD_BYTES, TezJobConfig.TEZ_ENGINE_IFILE_READAHEAD_BYTES);
    
    registerMRToEngineKeyTranslation(MRJobConfig.RECORDS_BEFORE_PROGRESS, TezJobConfig.RECORDS_BEFORE_PROGRESS);

    registerMRToEngineKeyTranslation(MRJobConfig.IO_SORT_FACTOR, TezJobConfig.TEZ_ENGINE_IO_SORT_FACTOR);
    
    registerMRToEngineKeyTranslation(MRJobConfig.MAP_SORT_SPILL_PERCENT, TezJobConfig.TEZ_ENGINE_SORT_SPILL_PERCENT);
    
    registerMRToEngineKeyTranslation(MRJobConfig.IO_SORT_MB, TezJobConfig.TEZ_ENGINE_IO_SORT_MB);
    
    registerMRToEngineKeyTranslation(MRJobConfig.INDEX_CACHE_MEMORY_LIMIT, TezJobConfig.TEZ_ENGINE_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    
    registerMRToEngineKeyTranslation(MRJobConfig.MAP_COMBINE_MIN_SPILLS, TezJobConfig.TEZ_ENGINE_COMBINE_MIN_SPILLS);
    
    // Counter replacement will work in this manner, as long as TezCounters
    // extends MRCounters and is used directly by the Mapper/Reducer.
    // When these counters are eventually translated over to MRCounters, this
    // may break.
    // Framework counters, like FILESYSTEM will likely be incompatible since
    // they enum key belongs to a different package.
    registerMRToEngineKeyTranslation(MRJobConfig.COUNTERS_MAX_KEY, TezJobConfig.COUNTERS_MAX_KEY);
    
    registerMRToEngineKeyTranslation(MRJobConfig.COUNTER_GROUP_NAME_MAX_KEY, TezJobConfig.COUNTER_GROUP_NAME_MAX_KEY);
    
    registerMRToEngineKeyTranslation(MRJobConfig.COUNTER_NAME_MAX_KEY, TezJobConfig.COUNTER_NAME_MAX_KEY);
    
    registerMRToEngineKeyTranslation(MRJobConfig.COUNTER_GROUPS_MAX_KEY, TezJobConfig.COUNTER_GROUPS_MAX_KEY);
    
    registerMRToEngineKeyTranslation(MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES, Constants.TEZ_ENGINE_TASK_MEMORY);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_PARALLEL_COPIES, TezJobConfig.TEZ_ENGINE_SHUFFLE_PARALLEL_COPIES);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_FETCH_FAILURES, TezJobConfig.TEZ_ENGINE_SHUFFLE_FETCH_FAILURES);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_NOTIFY_READERROR, TezJobConfig.TEZ_ENGINE_SHUFFLE_NOTIFY_READERROR);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_CONNECT_TIMEOUT, TezJobConfig.TEZ_ENGINE_SHUFFLE_CONNECT_TIMEOUT);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_READ_TIMEOUT, TezJobConfig.TEZ_ENGINE_SHUFFLE_READ_TIMEOUT);
    
    registerMRToEngineKeyTranslation(MRConfig.SHUFFLE_SSL_ENABLED_KEY, TezJobConfig.TEZ_ENGINE_SHUFFLE_ENABLE_SSL);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, TezJobConfig.TEZ_ENGINE_SHUFFLE_INPUT_BUFFER_PERCENT);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, TezJobConfig.TEZ_ENGINE_SHUFFLE_MEMORY_LIMIT_PERCENT);
    
    registerMRToEngineKeyTranslation(MRJobConfig.SHUFFLE_MERGE_PERCENT, TezJobConfig.TEZ_ENGINE_SHUFFLE_MERGE_PERCENT);
    
    registerMRToEngineKeyTranslation(MRJobConfig.REDUCE_MEMTOMEM_THRESHOLD, TezJobConfig.TEZ_ENGINE_SHUFFLE_MEMTOMEM_SEGMENTS);
    
    registerMRToEngineKeyTranslation(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, TezJobConfig.TEZ_ENGINE_SHUFFLE_ENABLE_MEMTOMEM);
    
    registerMRToEngineKeyTranslation(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, TezJobConfig.TEZ_ENGINE_INPUT_BUFFER_PERCENT);

    registerMRToEngineKeyTranslation(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, TezJobConfig.DAG_CREDENTIALS_BINARY);
    
    registerMRToEngineKeyTranslation("map.sort.class", TezJobConfig.TEZ_ENGINE_INTERNAL_SORTER_CLASS);
    
    registerMRToEngineKeyTranslation(MRJobConfig.GROUP_COMPARATOR_CLASS, TezJobConfig.TEZ_ENGINE_GROUP_COMPARATOR_CLASS);
    
    registerMRToEngineKeyTranslation(MRJobConfig.GROUP_COMPARATOR_CLASS, TezJobConfig.TEZ_ENGINE_INTERMEDIATE_INPUT_KEY_SECONDARY_COMPARATOR_CLASS);

  }
  
  private static void addDeprecatedKeys() {
  }

  private static void registerMRToEngineKeyTranslation(String mrKey,
      String tezKey) {
    mrParamToEngineParamMap.put(mrKey, tezKey);
  }
  
  @SuppressWarnings("unused")
  private static void _(String mrKey, String tezKey) {
    Configuration.addDeprecation(mrKey, tezKey);
  }

  public static Map<String, String> getMRToDAGParamMap() {
    return Collections.unmodifiableMap(mrParamToDAGParamMap);
  }

  public static Map<String, String> getMRToEngineParamMap() {
    return Collections.unmodifiableMap(mrParamToEngineParamMap);
  }

  // TODO Ideally, multi-stage should not be exposed.
  public static Map<String, Map<MultiStageKeys, String>> getMultiStageParamMap() {
    return Collections.unmodifiableMap(multiStageParamMap);
  }

}
