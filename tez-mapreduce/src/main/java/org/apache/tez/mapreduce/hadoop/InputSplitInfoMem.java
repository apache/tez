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
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;

/**
 * Represents InputSplitInfo for splits generated to memory. </p>
 * 
 * Since splits are generated in memory, the getSplitsMetaInfoFile and
 * getSplitsFile are not supported.
 * 
 */
public class InputSplitInfoMem implements InputSplitInfo {

  private final MRSplitsProto splitsProto;
  private final List<TaskLocationHint> taskLocationHints;
  private final int numTasks;

  public InputSplitInfoMem(MRSplitsProto splitsProto,
      List<TaskLocationHint> taskLocationHints, int numTasks) {
    this.splitsProto = splitsProto;
    this.taskLocationHints = taskLocationHints;
    this.numTasks = numTasks;
  }

  @Override
  public List<TaskLocationHint> getTaskLocationHints() {
    return this.taskLocationHints;
  }

  @Override
  public Path getSplitsMetaInfoFile() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }

  @Override
  public Path getSplitsFile() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }

  @Override
  public int getNumTasks() {
    return this.numTasks;
  }

  @Override
  public Type getType() {
    return Type.MEM;
  }

  @Override
  public MRSplitsProto getSplitsProto() {
    return this.splitsProto;
  }

}
