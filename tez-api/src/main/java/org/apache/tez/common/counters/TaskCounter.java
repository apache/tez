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

package org.apache.tez.common.counters;

import org.apache.hadoop.classification.InterfaceAudience;

// Counters used by Task classes
@InterfaceAudience.Private

public enum TaskCounter {
  // TODO Eventually, rename counters to be non-MR specific and map them to MR equivalent.

  /**
   * Number of Input Groups seen by ShuffledMergedInput.
   * Alternately the number of Input Groups seen by a Reduce task.
   */
  REDUCE_INPUT_GROUPS,

  /**
   * Number of records (across all Groups) seen by ShuffledMergedInput
   * Alternately number of records seen by a ReduceProcessor
   */
  REDUCE_INPUT_RECORDS,
  
  REDUCE_OUTPUT_RECORDS, // Not used at the moment.
  REDUCE_SKIPPED_GROUPS, // Not used at the moment.
  REDUCE_SKIPPED_RECORDS, // Not used at the moment.
  SPLIT_RAW_BYTES,
  
  COMBINE_INPUT_RECORDS,
  COMBINE_OUTPUT_RECORDS, // Not used at the moment.

  /**
   * Number of records written to disk in case of OnFileSortedOutput.
   * 
   * Number of additional records writtent out to disk in case of
   * ShuffledMergedInput; this represents the number of unnecessary spills to
   * disk caused by lac of memory.
   */
  SPILLED_RECORDS,

  /**
   * Number of Inputs from which data is copied. Represents physical Inputs. 
   */
  NUM_SHUFFLED_INPUTS,
  
  /**
   * Number of Inputs from which data was not copied - typically due to an empty Input
   */
  NUM_SKIPPED_INPUTS,

  /**
   * Number of failed copy attempts (physical inputs)
   */
  NUM_FAILED_SHUFFLE_INPUTS,
  
  MERGED_MAP_OUTPUTS,
  GC_TIME_MILLIS,
  CPU_MILLISECONDS,
  PHYSICAL_MEMORY_BYTES,
  VIRTUAL_MEMORY_BYTES,
  COMMITTED_HEAP_BYTES,

  /**
   * Represents the number of Input Records that were actually processed.
   * Used by MRInput and ShuffledUnorderedKVInput
   * 
   */
  INPUT_RECORDS_PROCESSED,
  
  // 
  /**
   * Represents the number of actual output records.
   * Used by MROutput, OnFileSortedOutput, and OnFileUnorderedKVOutput
   */
  OUTPUT_RECORDS,
  
  SKIPPED_RECORDS, // Not used at the moment.

  /**
   * Represents the serialized output size (uncompressed) of data being written.
   */
  OUTPUT_BYTES,

  /**
   * Represents serialized output size (uncompressed) along with any overhead
   * added by the format being used.
   */
  OUTPUT_BYTES_WITH_OVERHEAD,

  /**
   * Represents the actual physical size of the Output generated. This factors
   * in Compression if it is enabled. (Will include actual serialized output
   * size + overhead)
   */
  OUTPUT_BYTES_PHYSICAL,
  
  /**
   * Bytes written to disk due to unnecessary spills (lac of adequate memory).
   * Used by OnFileSortedOutput and ShuffledMergedInput
   */
  ADDITIONAL_SPILLS_BYTES_WRITTEN,
  
  /**
   * Bytes read from disk due to previous spills (lac of adequate memory).
   * Used by OnFileSortedOutput and ShuffledMergedInput
   */
  ADDITIONAL_SPILLS_BYTES_READ,
  
  /**
   * Actual number of unnecessary spills. (lac of adequate memory)
   * Used by OnFileSortedOutput
   */
  ADDITIONAL_SPILL_COUNT,
  
  INPUT_GROUPS, // Not used at the moment. Will eventually replace REDUCE_INPUT_GROUPS

  /**
   * Amount of physical data moved over the wire. Used by Shuffled*Input. Should
   * be a combination of SHUFFLE_BYTES_TO_MEM and SHUFFLE_BYTES_TO_DISK
   */
  SHUFFLE_BYTES,

  /**
   * Uncompressed size of the data being processed by the relevant Shuffle.
   * Includes serialization, file format etc overheads.
   */
  SHUFFLE_BYTES_DECOMPRESSED, 

  /**
   * Number of bytes which were shuffled directly to memory. 
   */
  SHUFFLE_BYTES_TO_MEM,

  /**
   * Number of bytes which were shuffled directly to disk 
   */
  SHUFFLE_BYTES_TO_DISK,

  /**
   * Number of Memory to Disk merges performed during sort-merge.
   * Used by ShuffledMergedInput
   */
  NUM_MEM_TO_DISK_MERGES,

  /**
   * Number of disk to disk merges performed during the sort-merge
   */
  NUM_DISK_TO_DISK_MERGES,
}