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

@Private
public enum FileSystemCounter {
  BYTES_READ("bytesRead"),
  BYTES_WRITTEN("bytesWritten"),
  READ_OPS("readOps"),
  LARGE_READ_OPS("largeReadOps"),
  WRITE_OPS("writeOps"),
  HDFS_BYTES_READ("hdfsBytesRead"),
  HDFS_BYTES_WRITTEN("hdfsBytesWritten"),
  FILE_BYTES_READ("fileBytesRead"),
  FILE_BYTES_WRITTEN("fileBytesWritten"),

  // Additional counters from HADOOP-13305
  OP_APPEND(CommonStatisticNames.OP_APPEND),
  OP_CREATE(CommonStatisticNames.OP_CREATE),
  OP_DELETE(CommonStatisticNames.OP_DELETE),
  OP_GET_FILE_STATUS(CommonStatisticNames.OP_GET_FILE_STATUS),
  OP_LIST_FILES(CommonStatisticNames.OP_LIST_FILES),
  OP_LIST_LOCATED_STATUS(CommonStatisticNames.OP_LIST_LOCATED_STATUS),
  OP_MKDIRS(CommonStatisticNames.OP_MKDIRS),
  OP_OPEN(CommonStatisticNames.OP_OPEN),
  OP_RENAME(CommonStatisticNames.OP_RENAME),
  OP_SET_ACL(CommonStatisticNames.OP_SET_ACL),
  OP_SET_OWNER(CommonStatisticNames.OP_SET_OWNER),
  OP_SET_PERMISSION(CommonStatisticNames.OP_SET_PERMISSION),
  OP_GET_FILE_BLOCK_LOCATIONS("op_get_file_block_locations");

  private final String opName;

  FileSystemCounter(String opName) {
    this.opName = opName;
  }

  public String getOpName() {
    return opName;
  }
}
