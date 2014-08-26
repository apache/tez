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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;

// TODO Fix this to be more usable. Interface is broken since half the methods apply to only a specific type.

/**
 * Provides information about input splits.</p>
 * 
 * The get*Path methods are only applicable when generating splits to disk. The
 * getSplitsProto method is only applicable when generating splits to memory.
 * 
 */
@Private
@Unstable
public interface InputSplitInfo {

  public enum Type {
    DISK, MEM
  }
  /**
   * Get the TaskLocationHints for each task
   */
  public abstract List<TaskLocationHint> getTaskLocationHints();

  /**
   * Get the path to the splits meta info file
   */
  public abstract Path getSplitsMetaInfoFile();

  /**
   * Get the path to the splits file
   */
  public abstract Path getSplitsFile();

  /**
   * Get the splits proto
   */
  public abstract MRSplitsProto getSplitsProto();
  
  /**
   * Get the number of splits that were generated. Same as number of tasks that
   * should be run for the vertex processing these set of splits.
   */
  public abstract int getNumTasks();
  
  /**
   * Get the {@link Type} of the InputSplitInfo
   */
  public abstract Type getType();
  
  /**
   * Get {@link Credentials} which may be required to access the splits.
   * @return {@link Credentials} which may be required to access the splits.
   */
  public abstract Credentials getCredentials();

  /**
   * Check whether the current instance is using old / new format splits
   * @return true if using new format splits, false otherwise
   */
  public boolean holdsNewFormatSplits();

  /**
   * Get new format splits. Should only be used if the mapreduce API is being used
   * @return array of mapreduce format splits
   */
  public org.apache.hadoop.mapreduce.InputSplit[] getNewFormatSplits();

  /**
   * Get old format splits. Should only be used if the mapred API is being used
   * @return array of mapred format splits
   */
  public org.apache.hadoop.mapred.InputSplit[] getOldFormatSplits();
}
