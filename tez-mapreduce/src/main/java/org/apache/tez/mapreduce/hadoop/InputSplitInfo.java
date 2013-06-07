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

import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;

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
 */
public class InputSplitInfo {

  /// Splits file
  private final Path splitsFile;
  /// Meta info file for all the splits information
  private final Path splitsMetaInfoFile;
  /// Location hints to determine where to run the tasks
  private final TaskLocationHint[] taskLocationHints;
  /// The num of tasks - same as number of splits generated.
  private final int numTasks;

  public InputSplitInfo(Path splitsFile, Path splitsMetaInfoFile, int numTasks,
      TaskLocationHint[] taskLocationHints) {
    this.splitsFile = splitsFile;
    this.splitsMetaInfoFile = splitsMetaInfoFile;
    this.taskLocationHints = taskLocationHints;
    this.numTasks = numTasks;
  }

  /**
   * Get the TaskLocationHints for each task
   */
  public TaskLocationHint[] getTaskLocationHints() {
    return taskLocationHints;
  }

  /**
   * Get the path to the splits meta info file
   */
  public Path getSplitsMetaInfoFile() {
    return splitsMetaInfoFile;
  }

  /**
   * Get the path to the splits file
   */
  public Path getSplitsFile() {
    return splitsFile;
  }

  /**
   * Get the number of splits that were generated. Same as number of tasks that
   * should be run for the vertex processing these set of splits.
   */
  public int getNumTasks() {
    return numTasks;
  }

}
