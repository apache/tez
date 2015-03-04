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
package org.apache.tez.runtime.library.api;



import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;

/**
 * Meant for user configurable job properties.
 * <p/>
 * Note for developers: Whenever a new key is added to this file, it must also be added to the set of
 * known tezRuntimeKeys.
 */

// TODO EVENTUALLY A description for each property.
@Public
@Evolving
public class TezRuntimeConfiguration {

  private static final String TEZ_RUNTIME_PREFIX = "tez.runtime.";

  private static final Set<String> tezRuntimeKeys = new HashSet<String>();
  private static Set<String> umnodifiableTezRuntimeKeySet;
  private static final Set<String> otherKeys = new HashSet<String>();
  private static Set<String> unmodifiableOtherKeySet;
  private static Configuration defaultConf = new Configuration(false);
  private static final Map<String, String> tezRuntimeConfMap = new HashMap<String, String>();
  private static final Map<String, String> otherConfMap = new HashMap<String, String>();

  /**
   * Prefixes from Hadoop configuration which are allowed.
   */
  private static final List<String> allowedPrefixes = new ArrayList<String>();
  private static List<String> unmodifiableAllowedPrefixes;


  /**
   * Configuration key to enable/disable IFile readahead.
   */
  public static final String TEZ_RUNTIME_IFILE_READAHEAD = TEZ_RUNTIME_PREFIX +
      "ifile.readahead";
  public static final boolean TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT = true;

  /**
   * Configuration key to set the IFile readahead length in bytes.
   */
  public static final String TEZ_RUNTIME_IFILE_READAHEAD_BYTES = TEZ_RUNTIME_PREFIX +
      "ifile.readahead.bytes";
  public static final int TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT =
      4 * 1024 * 1024;

  public static final int TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT = -1;

  /**
   * This is copy of io.file.buffer.size from Hadoop, which is used in several places such
   * as compression codecs, buffer sizes in IFile, while fetching etc.
   * Variable exists so that it can be referenced, instead of using the string name directly.
   */
  public static final String TEZ_RUNTIME_IO_FILE_BUFFER_SIZE = "io.file.buffer.size";


  public static final String TEZ_RUNTIME_IO_SORT_FACTOR = TEZ_RUNTIME_PREFIX +
      "io.sort.factor";
  public static final int TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT = 100;


  public static final String TEZ_RUNTIME_SORT_SPILL_PERCENT = TEZ_RUNTIME_PREFIX +
      "sort.spill.percent";
  public static final float TEZ_RUNTIME_SORT_SPILL_PERCENT_DEFAULT = 0.8f;


  public static final String TEZ_RUNTIME_IO_SORT_MB = TEZ_RUNTIME_PREFIX + "io.sort.mb";
  public static final int TEZ_RUNTIME_IO_SORT_MB_DEFAULT = 100;


  public static final String TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES = TEZ_RUNTIME_PREFIX +
      "index.cache.memory.limit.bytes";
  public static final int TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES_DEFAULT =
      1024 * 1024;


  // TODO Use the default value
  public static final String TEZ_RUNTIME_COMBINE_MIN_SPILLS = TEZ_RUNTIME_PREFIX +
      "combine.min.spills";
  public static final int TEZ_RUNTIME_COMBINE_MIN_SPILLS_DEFAULT = 3;


  public static final String TEZ_RUNTIME_SORT_THREADS = TEZ_RUNTIME_PREFIX +
      "sort.threads";
  public static final int TEZ_RUNTIME_SORT_THREADS_DEFAULT = 2;

  /**
   * Size of the buffer to use if not writing directly to disk.
   */
  public static final String TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB = TEZ_RUNTIME_PREFIX +
      "unordered.output.buffer.size-mb";
  public static final int TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB_DEFAULT = 100;

  /**
   * Maximum size for individual buffers used in the UnsortedPartitionedOutput.
   * This is only meant to be used by unit tests for now.
   */
  @Private
  public static final String TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES =
      TEZ_RUNTIME_PREFIX +
          "unordered.output.max-per-buffer.size-bytes";

  /**
   * Specifies a partitioner class, which is used in Tez Runtime components
   * like OnFileSortedOutput
   */
  public static final String TEZ_RUNTIME_PARTITIONER_CLASS =
      TEZ_RUNTIME_PREFIX + "partitioner.class";

  /**
   * Specifies a combiner class (primarily for Shuffle)
   */
  public static final String TEZ_RUNTIME_COMBINER_CLASS = TEZ_RUNTIME_PREFIX + "combiner.class";

  public static final String TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES = TEZ_RUNTIME_PREFIX +
      "shuffle.parallel.copies";
  public static final int TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT = 20;


  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT = TEZ_RUNTIME_PREFIX +
      "shuffle.fetch.failures.limit";
  public static final int TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT_DEFAULT = 5;


  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE =
      TEZ_RUNTIME_PREFIX +
          "shuffle.fetch.max.task.output.at.once";
  public final static int TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT
      = 20;


  public static final String TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR = TEZ_RUNTIME_PREFIX +
      "shuffle.notify.readerror";
  public static final boolean TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR_DEFAULT = true;


  public static final String TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT = TEZ_RUNTIME_PREFIX +
      "shuffle.connect.timeout";
  public static final int TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT_DEFAULT =
      3 * 60 * 1000;


  public static final String TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED = TEZ_RUNTIME_PREFIX +
      "shuffle.keep-alive.enabled";
  public static final boolean TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED_DEFAULT = false;


  public static final String TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS = TEZ_RUNTIME_PREFIX +
      "shuffle.keep-alive.max.connections";
  public static final int TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS_DEFAULT = 20;


  public static final String TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT =
      TEZ_RUNTIME_PREFIX + "shuffle.read.timeout";
  public final static int TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT_DEFAULT =
      3 * 60 * 1000;


  public static final String TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE =
      TEZ_RUNTIME_PREFIX + "shuffle.buffersize";
  public final static int TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE_DEFAULT =
      8 * 1024;


  public static final String TEZ_RUNTIME_SHUFFLE_ENABLE_SSL = TEZ_RUNTIME_PREFIX +
      "shuffle.ssl.enable";
  public static final boolean TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT = false;


  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT = TEZ_RUNTIME_PREFIX +
      "shuffle.fetch.buffer.percent";
  public static final float TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT =
      0.90f;


  public static final String TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT = TEZ_RUNTIME_PREFIX +
      "shuffle.memory.limit.percent";
  public static final float TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT =
      0.25f;

  // Rename to fraction
  public static final String TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT = TEZ_RUNTIME_PREFIX +
      "shuffle.merge.percent";
  public static final float TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT_DEFAULT = 0.90f;


  public static final String TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS = TEZ_RUNTIME_PREFIX +
      "shuffle.memory-to-memory.segments";


  public static final String TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM = TEZ_RUNTIME_PREFIX +
      "shuffle.memory-to-memory.enable";
  public static final boolean TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM_DEFAULT =
      false;


  public static final String TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT = TEZ_RUNTIME_PREFIX +
      "task.input.post-merge.buffer.percent";
  public static final float TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT = 0.0f;


  public static final String TEZ_RUNTIME_GROUP_COMPARATOR_CLASS = TEZ_RUNTIME_PREFIX +
      "group.comparator.class";

  public static final String TEZ_RUNTIME_INTERNAL_SORTER_CLASS = TEZ_RUNTIME_PREFIX +
      "internal.sorter.class";

  public static final String TEZ_RUNTIME_KEY_COMPARATOR_CLASS =
      TEZ_RUNTIME_PREFIX + "key.comparator.class";

  public static final String TEZ_RUNTIME_KEY_CLASS = TEZ_RUNTIME_PREFIX + "key.class";

  public static final String TEZ_RUNTIME_VALUE_CLASS = TEZ_RUNTIME_PREFIX + "value.class";

  public static final String TEZ_RUNTIME_COMPRESS = TEZ_RUNTIME_PREFIX + "compress";

  public static final String TEZ_RUNTIME_COMPRESS_CODEC = TEZ_RUNTIME_PREFIX + "compress.codec";

  // TODO Move this key to MapReduce
  public static final String TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS =
      TEZ_RUNTIME_PREFIX + "key.secondary.comparator.class";

  public static final String TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED =
      TEZ_RUNTIME_PREFIX +
          "empty.partitions.info-via-events.enabled";
  public static final boolean TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT = true;

  @Private
  public static final String TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED =
      TEZ_RUNTIME_PREFIX + "transfer.data-via-events.enabled";
  @Private
  public static final boolean TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED_DEFAULT = false;

  @Private
  public static final String TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE =
      TEZ_RUNTIME_PREFIX + "transfer.data-via-events.max-size";
  @Private
  public static final int TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT = 200 << 10; // 200KB

  /**
   * If the shuffle input is on the local host bypass the http fetch and access the files directly
   */
  public static final String TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH = TEZ_RUNTIME_PREFIX + "optimize.local.fetch";

  /**
   * local mode bypassing the http fetch is not enabled by default till we have unit tests in.
   */
  public static final boolean TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT = false;

  //TODO: Change description when we start supporting pipelined shuffle in unordered cases
  /**
   * Enable pipelined shuffle in ordered producer/consumer. Expert knob.
   * Works only with PipelinedSorter. Set tez.runtime.sort.threads > 2 for enabling
   * PipelinedSorter.  Ensure to set tez.runtime.disable.final-merge.in.sorter=true.
   * Speculative execution needs to be turned off when using this parameter. //TODO: TEZ-2132
   */
  public static final String TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED =
      TEZ_RUNTIME_PREFIX +
          "pipelined-shuffle.enabled";
  public static final boolean TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT = false;

  /**
   * final merge in defaultsorter/pipelinedsorter.
   * speculative execution needs to be turned off when disabling this parameter. //TODO: TEZ-2132
   */
  public static final String TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_SORTER =
      TEZ_RUNTIME_PREFIX + "enable.final-merge.in.sorter";
  public static final boolean TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_SORTER_DEFAULT = true;

  /**
   * Share data fetched between tasks running on the same host if applicable
   */
  public static final String TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH = TEZ_RUNTIME_PREFIX
      + "optimize.shared.fetch";

  /**
   * shared mode bypassing the http fetch is not enabled by default till we have unit tests in.
   */
  public static final boolean TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT = false;

  // TODO TEZ-1233 - allow this property to be set per vertex
  // TODO TEZ-1231 - move these properties out since they are not relevant for Inputs / Outputs

  /**
   * Value: Boolean
   * Whether to publish configuration information to History logger. Default false.
   */
  public static final String TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT =
      TEZ_RUNTIME_PREFIX + "convert.user-payload.to.history-text";
  public static final boolean TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT_DEFAULT = false;

  @Unstable
  @Private
  public static final String TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS = TEZ_RUNTIME_PREFIX +
      "merge.progress.records";
  public static final long TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT = 10000;

  static {
    tezRuntimeKeys.add(TEZ_RUNTIME_IFILE_READAHEAD);
    tezRuntimeKeys.add(TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    tezRuntimeKeys.add(TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    tezRuntimeKeys.add(TEZ_RUNTIME_IO_SORT_FACTOR);
    tezRuntimeKeys.add(TEZ_RUNTIME_SORT_SPILL_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_IO_SORT_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SORT_THREADS);
    tezRuntimeKeys.add(TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
    tezRuntimeKeys.add(TEZ_RUNTIME_PARTITIONER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMBINER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_ENABLE_SSL);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM);
    tezRuntimeKeys.add(TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_GROUP_COMPARATOR_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_INTERNAL_SORTER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_COMPARATOR_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_VALUE_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMPRESS);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMPRESS_CODEC);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_SORTER);
    tezRuntimeKeys.add(TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE);
    tezRuntimeKeys.add(TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS);
    tezRuntimeKeys.add(TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH);
    tezRuntimeKeys.add(TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH);
    tezRuntimeKeys.add(TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);

    defaultConf.addResource("core-default.xml");
    defaultConf.addResource("core-site.xml");
    defaultConf.addResource("tez-site.xml");

    for (Map.Entry<String, String> confEntry : defaultConf) {
      if (tezRuntimeKeys.contains(confEntry.getKey())) {
        tezRuntimeConfMap.put(confEntry.getKey(), confEntry.getValue());
      } else {
        // TODO TEZ-1232 Filter out parameters from TezConfiguration, and Task specific confs
        otherConfMap.put(confEntry.getKey(), confEntry.getValue());
        otherKeys.add(confEntry.getKey());
      }
    }

    // Do NOT need all prefixes from the following list. Only specific ones are allowed
    // "hadoop.", "hadoop.security", "io.", "fs.", "ipc.", "net.", "file.", "dfs.", "ha.", "s3.", "nfs3.", "rpc."
    allowedPrefixes.add("io.");
    allowedPrefixes.add("file.");
    allowedPrefixes.add("fs.");

    umnodifiableTezRuntimeKeySet = Collections.unmodifiableSet(tezRuntimeKeys);
    unmodifiableOtherKeySet = Collections.unmodifiableSet(otherKeys);
    unmodifiableAllowedPrefixes = Collections.unmodifiableList(allowedPrefixes);
  }

  @Private
  public static Set<String> getRuntimeConfigKeySet() {
    return umnodifiableTezRuntimeKeySet;
  }

  @Private
  public static Set<String> getRuntimeAdditionalConfigKeySet() {
    return unmodifiableOtherKeySet;
  }

  @Private
  public static List<String> getAllowedPrefixes() {
    return allowedPrefixes;
  }

  @Private
  public static Map<String, String> getTezRuntimeConfigDefaults() {
    return Collections.unmodifiableMap(tezRuntimeConfMap);
  }

  @Private
  public static Map<String, String> getOtherConfigDefaults() {
    return Collections.unmodifiableMap(otherConfMap);
  }
}