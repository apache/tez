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

package org.apache.tez.runtime.library.common.task.local.output;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

/**
 * Manipulate the working area for the transient store for maps and reduces.
 *
 * This class is used by map and reduce tasks to identify the directories that
 * they need to write to/read from for intermediate files. The callers of
 * these methods are from child space and see mapreduce.cluster.local.dir as
 * taskTracker/jobCache/jobId/attemptId
 * This class should not be used from TaskTracker space.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class TezTaskOutput {

  protected Configuration conf;
  protected String uniqueId;

  public TezTaskOutput(Configuration conf, String uniqueId) {
    this.conf = conf;
    this.uniqueId = uniqueId;
  }

  /**
   * Return the path to local map output file created earlier
   *
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputFile() throws IOException;

  /**
   * Create a local map output file name.
   *
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputFileForWrite(long size) throws IOException;

  /**
   * Create a local output file name. This method is meant to be used *only* if
   * the size of the file is not know up front.
   * 
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputFileForWrite() throws IOException;
  
  /**
   * Create a local map output file name on the same volume.
   */
  public abstract Path getOutputFileForWriteInVolume(Path existing);

  /**
   * Return the path to a local map output index file created earlier
   *
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputIndexFile() throws IOException;

  /**
   * Create a local map output index file name.
   *
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputIndexFileForWrite(long size) throws IOException;

  /**
   * Create a local map output index file name on the same volume.
   */
  public abstract Path getOutputIndexFileForWriteInVolume(Path existing);

  /**
   * Return a local map spill file created earlier.
   *
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillFile(int spillNumber) throws IOException;

  /**
   * Create a local map spill file name.
   *
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * Return a local map spill index file created earlier
   *
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillIndexFile(int spillNumber) throws IOException;

  /**
   * Create a local map spill index file name.
   *
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * Return a local reduce input file created earlier
   *
   * @param attemptIdentifier The identifier for the source task
   * @return path
   * @throws IOException
   */
  public abstract Path getInputFile(InputAttemptIdentifier attemptIdentifier) throws IOException;

  /**
   * Create a local reduce input file name.
   *
   * @param taskIdentifier The identifier for the source task
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getInputFileForWrite(
      int taskIdentifier, long size) throws IOException;

  /**
   * Construct a spill file name, given a spill number
   * @param spillNum
   * @return
   */
  public abstract String getSpillFileName(int spillNum);

  /** Removes all of the files related to a task. */
  public abstract void removeAll() throws IOException;
}
