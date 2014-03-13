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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;

/**
 * A general identifier, which internally stores the id
 * as an integer. This is the super class of {@link TezDAGID}, 
 * {@link TezVertexID}, {@link TezTaskID}, and {@link TezTaskAttemptID}.
 * 
 * @see TezTaskID
 * @see TezTaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class TezID implements WritableComparable<TezID> {
  public static final char SEPARATOR = '_';
  protected int id;

  /** constructs an ID object from the given int */
  public TezID(int id) {
    this.id = id;
  }

  protected TezID() {
  }

  /** returns the int which represents the identifier */
  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return String.valueOf(id);
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if(o == null)
      return false;
    if (o.getClass() == this.getClass()) {
      TezID that = (TezID) o;
      return this.id == that.id;
    }
    else
      return false;
  }

  /** Compare IDs by associated numbers */
  public int compareTo(TezID that) {
    return this.id - that.id;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
  }
  
  public static int readID(DataInput in) throws IOException {
    return in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
  }
  
}
