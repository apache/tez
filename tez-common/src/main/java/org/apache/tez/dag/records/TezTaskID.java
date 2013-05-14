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
import org.apache.tez.dag.records.TezVertexID;


/**
 * TaskID represents the immutable and unique identifier for 
 * a Tez Task. Each TaskID encompasses multiple attempts made to
 * execute the Tez Task, each of which are uniquely identified by
 * their TezTaskAttemptID.
 * 
 * @see TezTaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TezTaskID extends TezID {
  public static final String TASK = "task";
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }
  
  private TezVertexID vertexId;
  
  public TezTaskID() {
    vertexId = new TezVertexID();
  }
  
  /**
   * Constructs a TaskID object from given {@link MRxApplicationID}.  
   * @param jobId JobID that this tip belongs to 
   * @param type the {@link TezTaskType} of the task 
   * @param id the tip number
   */
  public TezTaskID(TezVertexID vertexId, int id) {
    super(id);
    if(vertexId == null) {
      throw new IllegalArgumentException("vertexId cannot be null");
    }
    this.vertexId = vertexId;
  }
  
  /** Returns the {@link TezVertexID} object that this task belongs to */
  public TezVertexID getVertexID() {
    return vertexId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TezTaskID that = (TezTaskID)o;
    return this.vertexId.equals(that.vertexId);
  }

  /**Compare TaskInProgressIds by first jobIds, then by tip numbers and type.*/
  @Override
  public int compareTo(TezID o) {
    TezTaskID that = (TezTaskID)o;
    int vertexComp = this.vertexId.compareTo(that.vertexId);
    if(vertexComp == 0) {
      return this.id - that.id;
    }
    else return vertexComp;
  }
  @Override
  public String toString() { 
    return appendTo(new StringBuilder(TASK)).toString();
  }

  /**
   * Add the unique string to the given builder.
   * @param builder the builder to append to
   * @return the builder that was passed in
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return vertexId.appendTo(builder).
                 append(SEPARATOR).
                 append(idFormat.format(id));
  }
  
  @Override
  public int hashCode() {
    return vertexId.hashCode() * 535013 + id;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    vertexId.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    vertexId.write(out);
    super.write(out);
  }
  
}
