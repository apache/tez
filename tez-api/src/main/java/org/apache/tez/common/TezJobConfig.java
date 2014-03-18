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
package org.apache.tez.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;


/**
 * Meant for user configurable job properties.
 */

// TODO EVENTUALLY A description for each property.
@Private
@Evolving
public class TezJobConfig {




  /** The number of milliseconds between progress reports. */
  public static final int PROGRESS_INTERVAL = 3000;

  public static final long DEFAULT_COMBINE_RECORDS_BEFORE_PROGRESS = 10000;

  /**
   * Configuration key to enable/disable IFile readahead.
   */
  public static final String TEZ_RUNTIME_IFILE_READAHEAD =
      "tez.runtime.ifile.readahead";
  public static final boolean TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT = true;

  /**
   * Configuration key to set the IFile readahead length in bytes.
   */
  public static final String TEZ_RUNTIME_IFILE_READAHEAD_BYTES =
      "tez.runtime.ifile.readahead.bytes";
  public static final int TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT =
      4 * 1024 * 1024;

  /**
   * TODO Maybe move this over from IFile into this file. -1 for now means ignore.
   */
  public static final int TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT = -1;
  
  /**
   * 
   */
  public static final String RECORDS_BEFORE_PROGRESS = 
      "tez.task.merge.progress.records";
  public static final long DEFAULT_RECORDS_BEFORE_PROGRESS = 10000; 

  /**
   * List of directories avialble to the Runtime. 
   */
  @Private
  public static final String LOCAL_DIRS = "tez.runtime.local.dirs";
  public static final String DEFAULT_LOCAL_DIRS = "/tmp";

  /**
   * One local dir for the speicfic job.
   */
  public static final String JOB_LOCAL_DIR = "tez.runtime.job.local.dir";
  
  /**
   * The directory which contains the localized files for this task.
   */
  @Private
  public static final String TASK_LOCAL_RESOURCE_DIR = "tez.runtime.task-local-resource.dir";
  public static final String DEFAULT_TASK_LOCAL_RESOURCE_DIR = "/tmp";
  
  public static final String TEZ_TASK_WORKING_DIR = "tez.runtime.task.working.dir";

  /**
   * 
   */
  public static final String TEZ_RUNTIME_IO_SORT_FACTOR = 
      "tez.runtime.io.sort.factor";
  public static final int DEFAULT_TEZ_RUNTIME_IO_SORT_FACTOR = 100;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SORT_SPILL_PERCENT = 
      "tez.runtime.sort.spill.percent";
  public static float DEFAULT_TEZ_RUNTIME_SORT_SPILL_PERCENT = 0.8f; 

  /**
   * 
   */
  public static final String TEZ_RUNTIME_IO_SORT_MB = "tez.runtime.io.sort.mb";
  public static final int DEFAULT_TEZ_RUNTIME_IO_SORT_MB = 100;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES = 
      "tez.runtime.index.cache.memory.limit.bytes";
  public static final int DEFAULT_TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES = 
      1024 * 1024;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_COMBINE_MIN_SPILLS = 
      "tez.runtime.combine.min.spills";
  public static final int  DEFAULT_TEZ_RUNTIME_COMBINE_MIN_SPILLS = 3;
  
  /**
   * 
   */
  public static final String TEZ_RUNTIME_SORT_THREADS = 
	      "tez.runtime.sort.threads";
  public static final int DEFAULT_TEZ_RUNTIME_SORT_THREADS = 1;

  /**
   * Specifies a partitioner class, which is used in Tez Runtime components
   * like OnFileSortedOutput
   */
  public static final String TEZ_RUNTIME_PARTITIONER_CLASS = "tez.runtime.partitioner.class";
  
  /**
   * Specifies a combiner class (primarily for Shuffle)
   */
  public static final String TEZ_RUNTIME_COMBINER_CLASS = "tez.runtime.combiner.class";
  
  public static final String TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS = "tez.runtime.num.expected.partitions";
  
  /**
   * 
   */
  public static final String COUNTERS_MAX_KEY = "tez.runtime.job.counters.max";
  public static final int COUNTERS_MAX_DEFAULT = 1200;

  /**
   * 
   */
  public static final String COUNTER_GROUP_NAME_MAX_KEY = "tez.runtime.job.counters.group.name.max";
  public static final int COUNTER_GROUP_NAME_MAX_DEFAULT = 128;

  /**
   * 
   */
  public static final String COUNTER_NAME_MAX_KEY = "tez.runtime.job.counters.counter.name.max";
  public static final int COUNTER_NAME_MAX_DEFAULT = 64;

  /**
   * 
   */
  public static final String COUNTER_GROUPS_MAX_KEY = "tez.runtime.job.counters.groups.max";
  public static final int COUNTER_GROUPS_MAX_DEFAULT = 500;

  
  /**
   * Temporary interface for MR only (not chained Tez) to indicate whether
   * in-memory shuffle should be used.
   */
  @Private
  public static final String TEZ_RUNTIME_SHUFFLE_USE_IN_MEMORY =
      "tez.runtime.shuffle.use.in-memory";
  public static final boolean DEFAULT_TEZ_RUNTIME_SHUFFLE_USE_IN_MEMORY = false;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES = 
      "tez.runtime.shuffle.parallel.copies";
  public static final int DEFAULT_TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES = 20;

  /**
   * TODO Is this user configurable.
   */
  public static final String TEZ_RUNTIME_METRICS_SESSION_ID = 
      "tez.runtime.metrics.session.id";
  public static final String DEFAULT_TEZ_RUNTIME_METRICS_SESSION_ID = "";

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES = 
      "tez.runtime.shuffle.fetch.failures.limit";
  public final static int DEFAULT_TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT = 5;


  /**
   *
   */
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE =
    "tez.runtime.shuffle.fetch.max.task.output.at.once";
  public final static int DEFAULT_TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE
          = 20;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR = 
      "tez.runtime.shuffle.notify.readerror";
  public static final boolean DEFAULT_TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR = true;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT = 
      "tez.runtime.shuffle.connect.timeout";
  public static final int DEFAULT_TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT = 
      3 * 60 * 1000;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT = "tez.runtime.shuffle.read.timeout";
  public final static int DEFAULT_TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT = 
      3 * 60 * 1000;
  
  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE = "tez.runtime.shuffle.buffersize";
  public final static int DEFAULT_TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE = 
      8 * 1024;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_ENABLE_SSL = 
      "tez.runtime.shuffle.ssl.enable";
  public static final boolean DEFAULT_TEZ_RUNTIME_SHUFFLE_ENABLE_SSL = false;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT = 
      "tez.runtime.shuffle.input.buffer.percent";
  public static final float DEFAULT_TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT =
      0.90f;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT = 
      "tez.runtime.shuffle.memory.limit.percent";
  public static final float DEFAULT_TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT = 
      0.25f;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT = 
      "tez.runtime.shuffle.merge.percent";
  public static final float DEFAULT_TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT = 0.90f;
  
  /**
   * TODO TEZAM3 default value ?
   */
  public static final String TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS = 
      "tez.runtime.shuffle.memory-to-memory.segments";

  /**
   * 
   */
  public static final String TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM = 
      "tez.runtime.shuffle.memory-to-memory.enable";
  public static final boolean DEFAULT_TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM = 
      false;

  /**
   * 
   */
  public static final String TEZ_RUNTIME_INPUT_BUFFER_PERCENT = 
      "tez.runtime.task.input.buffer.percent";
  public static final float DEFAULT_TEZ_RUNTIME_INPUT_BUFFER_PERCENT = 0.0f;

  // TODO Rename. 
  public static final String TEZ_RUNTIME_GROUP_COMPARATOR_CLASS = 
      "tez.runtime.group.comparator.class";
  
  // TODO Better name.
  public static final String TEZ_RUNTIME_INTERNAL_SORTER_CLASS = 
      "tez.runtime.internal.sorter.class";
  
  public static final String TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_COMPARATOR_CLASS = 
      "tez.runtime.intermediate-output.key.comparator.class";
  public static final String TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_COMPARATOR_CLASS = 
      "tez.runtime.intermediate-input.key.comparator.class";

  public static final String TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS = 
      "tez.runtime.intermediate-output.key.class";
  public static final String TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS = 
      "tez.runtime.intermediate-input.key.class";
  
  public static final String TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS = 
      "tez.runtime.intermediate-output.value.class";
  public static final String TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS = 
      "tez.runtime.intermediate-input.value.class";


  /** Whether intermediate output should be compressed or not */
  public static final String TEZ_RUNTIME_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS = 
      "tez.runtime.intermediate-output.should-compress";
  /** Whether intermediate input is compressed */
  public static final String TEZ_RUNTIME_INTERMEDIATE_INPUT_IS_COMPRESSED = 
      "tez.runtime.intermediate-input.is-compressed";
  /**
   * The coded to be used if compressing intermediate output. Only applicable if
   * tez.runtime.intermediate-output.should-compress is enabled.
   */
  public static final String TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC = 
      "tez.runtime.intermediate-output.compress.codec";
  /**
   * The coded to be used when reading intermediate compressed input. Only
   * applicable if tez.runtime.intermediate-input.is-compressed is enabled.
   */
  public static final String TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC = 
      "tez.runtime.intermediate-input.compress.codec";


  public static final String TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_SECONDARY_COMPARATOR_CLASS = 
      "tez.runtime.intermediate-input.key.secondary.comparator.class";

  public static final String TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED = "tez.runtime.broadcast.data-via-events.enabled";
  public static final boolean TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED_DEFAULT = false;
  
  public static final String TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE = "tez.runtime.broadcast.data-via-events.max-size";
  public static final int TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT = 200 << 10;// 200KB
  
  /** Defines the ProcessTree implementation which will be used to collect resource utilization. */
  public static final String TEZ_RESOURCE_CALCULATOR_PROCESS_TREE_CLASS = "tez.resource.calculator.process-tree.class";
}
