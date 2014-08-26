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

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;

/**
 * Information obtained by using the InputFormat class to generate
 * the required InputSplit information files that in turn can be used to
 * setup a DAG.
 *
 * The splitsFile and splitsMetaInfoFile need to be provided as LocalResources
 * to the vertex in question. The numTasks represents the parallelism for
 * the vertex and the taskLocationHints define the possible nodes on which the
 * tasks should be run based on the location of the splits that will be
 * processed by each task.
 * 
 * The getSplitsProto method is not supported by this implementation.
 */
public class InputSplitInfoDisk implements InputSplitInfo {

  /// Splits file
  private final Path splitsFile;
  /// Meta info file for all the splits information
  private final Path splitsMetaInfoFile;
  /// Location hints to determine where to run the tasks
  private final List<TaskLocationHint> taskLocationHints;
  /// The num of tasks - same as number of splits generated.
  private final int numTasks;
  private final Credentials credentials;

  public InputSplitInfoDisk(Path splitsFile, Path splitsMetaInfoFile, int numTasks,
      List<TaskLocationHint> taskLocationHints, Credentials credentials) {
    this.splitsFile = splitsFile;
    this.splitsMetaInfoFile = splitsMetaInfoFile;
    this.taskLocationHints = taskLocationHints;
    this.numTasks = numTasks;
    this.credentials = credentials;
  }

  /* (non-Javadoc)
   * @see org.apache.tez.mapreduce.hadoop.MRSplitInfo#getTaskLocationHints()
   */
  @Override
  public List<TaskLocationHint> getTaskLocationHints() {
    return taskLocationHints;
  }

  /* (non-Javadoc)
   * @see org.apache.tez.mapreduce.hadoop.MRSplitInfo#getSplitsMetaInfoFile()
   */
  @Override
  public Path getSplitsMetaInfoFile() {
    return splitsMetaInfoFile;
  }

  /* (non-Javadoc)
   * @see org.apache.tez.mapreduce.hadoop.MRSplitInfo#getSplitsFile()
   */
  @Override
  public Path getSplitsFile() {
    return splitsFile;
  }

  /* (non-Javadoc)
   * @see org.apache.tez.mapreduce.hadoop.MRSplitInfo#getNumTasks()
   */
  @Override
  public int getNumTasks() {
    return numTasks;
  }

  @Override
  public Type getType() {
    return Type.DISK;
  }

  @Override
  public MRSplitsProto getSplitsProto() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }
  
  @Override
  public Credentials getCredentials() {
    return this.credentials;
  }

  @Override
  public boolean holdsNewFormatSplits() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }

  @Override
  public InputSplit[] getNewFormatSplits() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }

  @Override
  public org.apache.hadoop.mapred.InputSplit[] getOldFormatSplits() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }

}
