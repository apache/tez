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

var getDefaultTimelineUrl = function() {
  var location = window.location;
  var protocol = App.env.isStandalone ? location.protocol : 'http:';
  var hostname = App.env.isStandalone ? location.hostname : 'localhost';
  return '%@//%@:8188'.fmt(protocol, hostname);
};

var getDefaultRMWebUrl = function() {
  var location = window.location;
  var protocol = App.env.isStandalone ? location.protocol : 'http:';
  var hostname = App.env.isStandalone ? location.hostname : 'localhost';
  return '%@//%@:8088'.fmt(protocol, hostname);
};

$.extend(true, App.Configs, {
  envDefaults: {
    version: "0.7.0",

    timelineBaseUrl: getDefaultTimelineUrl(),
    RMWebUrl: getDefaultRMWebUrl(),
  },

  restNamespace: {
    timeline: 'ws/v1/timeline',
    applicationHistory: 'ws/v1/applicationhistory'
  },

  defaultCounters: [
    // File System Counters
    {
      counterId: 'FILE_BYTES_READ',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'File Bytes Read'
    },
    {
      counterId: 'FILE_BYTES_WRITTEN',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'File Bytes Written'
    },
    {
      counterId: 'FILE_READ_OPS',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'File Read Ops'
    },
    {
      counterId: 'FILE_LARGE_READ_OPS',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'File Large Read Ops'
    },
    {
      counterId: 'FILE_WRITE_OPS',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'File Write Ops'
    },
    {
      counterId: 'HDFS_BYTES_READ',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'HDFS Bytes Read'
    },
    {
      counterId: 'HDFS_BYTES_WRITTEN',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'HDFS Bytes Written'
    },
    {
      counterId: 'HDFS_READ_OPS',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'HDFS Read Ops'
    },
    {
      counterId: 'HDFS_LARGE_READ_OPS',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'HDFS Large Read Ops'
    },
    {
      counterId: 'HDFS_WRITE_OPS',
      groupId: 'org.apache.tez.common.counters.FileSystemCounter',
      headerText: 'HDFS Write Ops'
    }
  ]
});
