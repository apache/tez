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

App.Configs.setProperties({

  // Default environment configurations
  envDefaults: {
    // Host URLs: Change the following URLs for pointing tez-ui to the respective servers.
    timelineBaseUrl: 'http://localhost:8188', // ip:po at which time;line server is running
    RMWebUrl: 'http://localhost:8088', // Location of RM web url
  },

  restNamespace: {
    timeline: 'ws/v1/timeline',
    applicationHistory: 'ws/v1/applicationhistory'
  },

  table: {
    commonColumns: {
      /*
       * More counters can be added into the tables by adding an entry into the following array.
       * Believe counterId and groupId are self descriptive, value in headerCellName would be
       * displayed as the column header.
       */
      counters: [
        {
          counterId: 'FILE_BYTES_READ',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'File Bytes Read'
        },
        {
          counterId: 'FILE_BYTES_WRITTEN',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'File Bytes Written'
        },
        {
          counterId: 'FILE_READ_OPS',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'File Read Ops'
        },
        {
          counterId: 'FILE_LARGE_READ_OPS',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'File Large Read Ops'
        },
        {
          counterId: 'FILE_WRITE_OPS',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'File Write Ops'
        },
        {
          counterId: 'HDFS_BYTES_READ',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'HDFS Bytes Read'
        },
        {
          counterId: 'HDFS_BYTES_WRITTEN',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'HDFS Bytes Written'
        },
        {
          counterId: 'HDFS_READ_OPS',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'HDFS Read Ops'
        },
        {
          counterId: 'HDFS_LARGE_READ_OPS',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'HDFS Large Read Ops'
        },
        {
          counterId: 'HDFS_WRITE_OPS',
          groupId: 'org.apache.tez.common.counters.FileSystemCounter',
          headerCellName: 'HDFS Write Ops'
        }
      ]
    }
  }
});