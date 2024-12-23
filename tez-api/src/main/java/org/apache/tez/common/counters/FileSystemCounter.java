/*
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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.StorageStatistics.CommonStatisticNames;

/**
 * FileSystemCounter is an enum for defining which filesystem/storage statistics are exposed in Tez.
 */
@Private
public enum FileSystemCounter {
  BYTES_READ("bytesRead"),
  BYTES_WRITTEN("bytesWritten"),
  READ_OPS("readOps"),
  LARGE_READ_OPS("largeReadOps"),
  WRITE_OPS("writeOps"),

  // Additional counters from HADOOP-13305
  OP_APPEND(CommonStatisticNames.OP_APPEND),
  OP_COPY_FROM_LOCAL_FILE(CommonStatisticNames.OP_COPY_FROM_LOCAL_FILE),
  OP_CREATE(CommonStatisticNames.OP_CREATE),
  OP_CREATE_NON_RECURSIVE(CommonStatisticNames.OP_CREATE_NON_RECURSIVE),
  OP_DELETE(CommonStatisticNames.OP_DELETE),
  OP_EXISTS(CommonStatisticNames.OP_EXISTS),
  OP_GET_CONTENT_SUMMARY(CommonStatisticNames.OP_GET_CONTENT_SUMMARY),
  OP_GET_DELEGATION_TOKEN(CommonStatisticNames.OP_GET_DELEGATION_TOKEN),
  OP_GET_FILE_CHECKSUM(CommonStatisticNames.OP_GET_FILE_CHECKSUM),
  OP_GET_FILE_STATUS(CommonStatisticNames.OP_GET_FILE_STATUS),
  OP_GET_STATUS(CommonStatisticNames.OP_GET_STATUS),
  OP_GLOB_STATUS(CommonStatisticNames.OP_GLOB_STATUS),
  OP_IS_FILE(CommonStatisticNames.OP_IS_FILE),
  OP_IS_DIRECTORY(CommonStatisticNames.OP_IS_DIRECTORY),
  OP_LIST_FILES(CommonStatisticNames.OP_LIST_FILES),
  OP_LIST_LOCATED_STATUS(CommonStatisticNames.OP_LIST_LOCATED_STATUS),
  OP_LIST_STATUS(CommonStatisticNames.OP_LIST_STATUS),
  OP_MKDIRS(CommonStatisticNames.OP_MKDIRS),
  OP_MODIFY_ACL_ENTRIES(CommonStatisticNames.OP_MODIFY_ACL_ENTRIES),
  OP_OPEN(CommonStatisticNames.OP_OPEN),
  OP_REMOVE_ACL(CommonStatisticNames.OP_REMOVE_ACL),
  OP_REMOVE_ACL_ENTRIES(CommonStatisticNames.OP_REMOVE_ACL_ENTRIES),
  OP_REMOVE_DEFAULT_ACL(CommonStatisticNames.OP_REMOVE_DEFAULT_ACL),
  OP_RENAME(CommonStatisticNames.OP_RENAME),
  OP_SET_ACL(CommonStatisticNames.OP_SET_ACL),
  OP_SET_OWNER(CommonStatisticNames.OP_SET_OWNER),
  OP_SET_PERMISSION(CommonStatisticNames.OP_SET_PERMISSION),
  OP_SET_TIMES(CommonStatisticNames.OP_SET_TIMES),
  OP_TRUNCATE(CommonStatisticNames.OP_TRUNCATE),

  // counters below are not needed in production, as the scheme_countername expansion is taken care of by the
  // FileSystemCounterGroup, the only reason they are here is that some analyzers still depend on them
  @Deprecated
  HDFS_BYTES_READ("hdfsBytesRead"),
  @Deprecated
  HDFS_BYTES_WRITTEN("hdfsBytesWritten"),
  @Deprecated
  FILE_BYTES_READ("fileBytesRead"),
  @Deprecated
  FILE_BYTES_WRITTEN("fileBytesWritten");

  private final String opName;

  FileSystemCounter(String opName) {
    this.opName = opName;
  }

  public String getOpName() {
    return opName;
  }
}
