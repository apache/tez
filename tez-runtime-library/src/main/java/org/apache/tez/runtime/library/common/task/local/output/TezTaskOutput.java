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
 * Manipulate the working area for the transient store for components in tez-runtime-library
 *
 * This class is used by Inputs and Outputs in tez-runtime-library to identify the directories
 * that they need to write to / read from for intermediate files.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class TezTaskOutput {

  protected final Configuration conf;
  protected final String uniqueId;

  /**
   * @param conf     the configuration from which local-dirs will be picked up
   * @param uniqueId a unique identifier for the specific input / output. This is expected to be
   *                 unique for all the Inputs / Outputs within a container - i.e. even if the
   *                 container is used for multiple tasks, this id should be unique for inputs /
   *                 outputs spanning across tasks. This is also expected to be unique across all
   *                 tasks for a vertex.
   */
  public TezTaskOutput(Configuration conf, String uniqueId) {
    this.conf = conf;
    this.uniqueId = uniqueId;
  }

  /**
   * Return the path to local output file created earlier.
   *
   * TODO TEZ-1855: Remove this. Leads to an extra localdir scan just to update counters.
   *
   * @return path the path of the local output file
   * @throws IOException
   */
  public abstract Path getOutputFile() throws IOException;

  /**
   * Create a local output file name.
   *
   * @param size the size of the file
   * @return path the path to write to
   * @throws IOException
   */
  public abstract Path getOutputFileForWrite(long size) throws IOException;

  /**
   * Create a local output file name. This method is meant to be used *only* if
   * the size of the file is not know up front.
   * 
   * @return path the path to write to
   * @throws IOException
   */
  public abstract Path getOutputFileForWrite() throws IOException;

  /**
   * Create a local output file name on the same volume.
   * This is only meant to be used to rename temporary files to their final destination within the
   * same volume.
   *
   * @return path the path of the output file within the same volume
   */
  public abstract Path getOutputFileForWriteInVolume(Path existing);

  /**
   * Return the path to a local output index file created earlier
   *
   * TODO TEZ-1855: Remove this. Leads to an additional scan to find empty partitions.
   *
   * @return path the path of the index file
   * @throws IOException
   */
  public abstract Path getOutputIndexFile() throws IOException;

  /**
   * Create a local output index file name.
   *
   * @param size the size of the file
   * @return path the path to write the index file to
   * @throws IOException
   */
  public abstract Path getOutputIndexFileForWrite(long size) throws IOException;

  /**
   * Create a local output index file name on the same volume.
   * The intended usage of this method is to write the index file on the same volume as the
   * associated data file.
   * @return path the path of the index file within the same volume
   */
  public abstract Path getOutputIndexFileForWriteInVolume(Path existing);

  /**
   * Return a local output spill file created earlier.
   *
   * @param spillNumber the spill number
   * @return path the path of the previously written spill file corresponding to the spillNumber
   * @throws IOException
   * // KKK Try removing this. Unnecessary file scans - can be stored in memory instead.
   */
  public abstract Path getSpillFile(int spillNumber) throws IOException;

  /**
   * Create a local output spill file name.
   *
   * @param spillNumber the spill number
   * @param size the size of the file
   * @return path the path to write the spill file for the specific spillNumber
   * @throws IOException
   */
  public abstract Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * Return a local output spill index file created earlier
   *
   * TODO TEZ-1855: Remove this. Should be possible to cache this instead of requiring a directory scan.
   *
   * @param spillNumber the spill number
   * @return path the path of the previously written spill index file corresponding to the spillNumber
   * @throws IOException
   */
  public abstract Path getSpillIndexFile(int spillNumber) throws IOException;

  /**
   * Create a local output spill index file name.
   *
   * @param spillNumber the spill number
   * @param size the size of the spill file
   * @return path the path to write the spill index file for the specific spillNumber
   * @throws IOException
   */
  public abstract Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * Return a local input file created earlier
   *
   * TODO: TEZ-1855. Remove this.
   *
   * @param attemptIdentifier The identifier for the source
   * @return path the path to the input file
   * @throws IOException
   */
  public abstract Path getInputFile(InputAttemptIdentifier attemptIdentifier) throws IOException;

  /**
   * Create a local input file name.
   *
   * @param srcIdentifier The identifier for the source
   * @param size the size of the file
   * @return path the path to the input file.
   * @throws IOException
   */
  public abstract Path getInputFileForWrite(
      int srcIdentifier, long size) throws IOException;

  /**
   * Construct a spill file name, given a spill number
   * @param spillNum
   * @return a spill file name independent of the unique identifier and local directories
   */
  public abstract String getSpillFileName(int spillNum);

}
