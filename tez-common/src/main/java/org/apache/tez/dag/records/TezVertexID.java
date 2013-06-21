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

package org.apache.tez.dag.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * TezVertexID represents the immutable and unique identifier for
 * a Vertex in a Tez DAG. Each TezVertexID encompasses multiple Tez Tasks.
 *
 * TezVertezID consists of 2 parts. The first part is the {@link TezDAGID},
 * that is the Tez DAG that this vertex belongs to. The second part is
 * the vertex number.
 *
 * @see TezDAGID
 * @see TezTaskID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TezVertexID extends TezID {
  public static final String VERTEX = "vertex";
  protected static final NumberFormat idFormat =
      NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(2);
  }

  private TezDAGID dagId;

  public TezVertexID() {
  }

  /**
   * Constructs a TaskID object from given {@link TezDAGID}.
   * @param applicationId JobID that this tip belongs to
   * @param type the {@link TezTaskType} of the task
   * @param id the tip number
   */
  public TezVertexID(TezDAGID dagId, int id) {
    super(id);
    if(dagId == null) {
      throw new IllegalArgumentException("dagId cannot be null");
    }
    this.dagId = dagId;
  }

  /** Returns the {@link TezDAGID} object that this tip belongs to */
  public TezDAGID getDAGId() {
    return dagId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TezVertexID that = (TezVertexID)o;
    return this.dagId.equals(that.dagId);
  }

  /**Compare TaskInProgressIds by first jobIds, then by tip numbers and type.*/
  @Override
  public int compareTo(TezID o) {
    TezVertexID that = (TezVertexID)o;
    return this.dagId.compareTo(that.dagId);
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(VERTEX)).toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    dagId = new TezDAGID();
    dagId.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    dagId.write(out);
    super.write(out);
  }

  /**
   * Add the unique string to the given builder.
   * @param builder the builder to append to
   * @return the builder that was passed in
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return dagId.appendTo(builder).
        append(SEPARATOR).
        append(idFormat.format(id));
  }

  @Override
  public int hashCode() {
    return dagId.hashCode() * 530017 + id;
  }

}
