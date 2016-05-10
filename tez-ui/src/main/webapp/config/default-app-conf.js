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

var buildInfo = require('./build-info');

module.exports = { // Tez App configurations
  buildVersion: buildInfo.version || "",
  isStandalone: true, // Must be set false while running in wrapped mode
  rowLoadLimit: 9007199254740991,
  pollingInterval: 3000,

  hosts: {
    timeline: 'localhost:8188',
    rm: 'localhost:8088',
  },
  namespaces: {
    webService: {
      timeline: 'ws/v1/timeline',
      appHistory: 'ws/v1/applicationhistory',
      rm: 'ws/v1/cluster',
      am: 'proxy/{app_id}/ws/v{version:2}/tez',
    },
    web: {
      rm: 'cluster'
    },
  },
  paths: {
    timeline: {
      dag: 'TEZ_DAG_ID',
      vertex: 'TEZ_VERTEX_ID',
      task: 'TEZ_TASK_ID',
      attempt: 'TEZ_TASK_ATTEMPT_ID',

      "hive-query": 'HIVE_QUERY_ID',

      app: 'TEZ_APPLICATION'
    },
    am: {
      "dag-am": 'dagInfo',
      "vertex-am": 'verticesInfo',
      "task-am": 'tasksInfo',
      "attempt-am": 'attemptsInfo',
    },
    rm: {
      "app-rm": "apps"
    }
  },
  hrefs: {
    help: "https://tez.apache.org/tez_ui_user_data.html",
    license: "http://www.apache.org/licenses/LICENSE-2.0"
  },

  tables: {
    defaultColumns: {
      counters: [
        // File System Counters
        {
          counterName: 'FILE_BYTES_READ',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'FILE_BYTES_WRITTEN',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'FILE_READ_OPS',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'FILE_LARGE_READ_OPS',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'FILE_WRITE_OPS',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'HDFS_BYTES_READ',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'HDFS_BYTES_WRITTEN',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'HDFS_READ_OPS',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'HDFS_LARGE_READ_OPS',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },
        {
          counterName: 'HDFS_WRITE_OPS',
          counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        },

        // Task Counters
        {
          counterName: "NUM_SPECULATIONS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "REDUCE_INPUT_GROUPS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "REDUCE_INPUT_RECORDS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SPLIT_RAW_BYTES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "COMBINE_INPUT_RECORDS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SPILLED_RECORDS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "NUM_SHUFFLED_INPUTS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "NUM_SKIPPED_INPUTS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "NUM_FAILED_SHUFFLE_INPUTS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "MERGED_MAP_OUTPUTS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "GC_TIME_MILLIS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "CPU_MILLISECONDS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "PHYSICAL_MEMORY_BYTES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "VIRTUAL_MEMORY_BYTES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "COMMITTED_HEAP_BYTES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "INPUT_RECORDS_PROCESSED",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "OUTPUT_RECORDS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "OUTPUT_LARGE_RECORDS",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "OUTPUT_BYTES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "OUTPUT_BYTES_WITH_OVERHEAD",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "OUTPUT_BYTES_PHYSICAL",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "ADDITIONAL_SPILLS_BYTES_WRITTEN",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "ADDITIONAL_SPILLS_BYTES_READ",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "ADDITIONAL_SPILL_COUNT",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SHUFFLE_BYTES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SHUFFLE_BYTES_DECOMPRESSED",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SHUFFLE_BYTES_TO_MEM",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SHUFFLE_BYTES_TO_DISK",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SHUFFLE_BYTES_DISK_DIRECT",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "NUM_MEM_TO_DISK_MERGES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "NUM_DISK_TO_DISK_MERGES",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "SHUFFLE_PHASE_TIME",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "MERGE_PHASE_TIME",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "FIRST_EVENT_RECEIVED",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
        {
          counterName: "LAST_EVENT_RECEIVED",
          counterGroupName: "org.apache.tez.common.counters.TaskCounter",
        },
      ],

      dagCounters: [
        {
          counterName :"NUM_FAILED_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"NUM_KILLED_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"NUM_SUCCEEDED_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"TOTAL_LAUNCHED_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"OTHER_LOCAL_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"DATA_LOCAL_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"RACK_LOCAL_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"SLOTS_MILLIS_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"FALLOW_SLOTS_MILLIS_TASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"TOTAL_LAUNCHED_UBERTASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"NUM_UBER_SUBTASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },
        {
          counterName :"NUM_FAILED_UBERTASKS",
          counterGroupName :"org.apache.tez.common.counters.DAGCounter",
        },

        {
          counterName: "REDUCE_OUTPUT_RECORDS",
          counterGroupName: "REDUCE_OUTPUT_RECORDS",
        },
        {
          counterName: "REDUCE_SKIPPED_GROUPS",
          counterGroupName: "REDUCE_SKIPPED_GROUPS",
        },
        {
          counterName: "REDUCE_SKIPPED_RECORDS",
          counterGroupName: "REDUCE_SKIPPED_RECORDS",
        },
        {
          counterName: "COMBINE_OUTPUT_RECORDS",
          counterGroupName: "COMBINE_OUTPUT_RECORDS",
        },
        {
          counterName: "SKIPPED_RECORDS",
          counterGroupName: "SKIPPED_RECORDS",
        },
        {
          counterName: "INPUT_GROUPS",
          counterGroupName: "INPUT_GROUPS",
        }
      ]
    }
  }
};
