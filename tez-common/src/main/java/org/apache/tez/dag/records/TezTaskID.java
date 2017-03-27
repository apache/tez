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

import com.google.common.base.Preconditions;
import org.apache.tez.util.FastNumberFormat;

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
  private final int serializingHash;
  
  static final ThreadLocal<FastNumberFormat> tezTaskIdFormat = new ThreadLocal<FastNumberFormat>() {
    @Override
    public FastNumberFormat initialValue() {
      FastNumberFormat fmt = FastNumberFormat.getInstance();
      fmt.setMinimumIntegerDigits(6);
      return fmt;
    }
  };

  private static TezIDCache<TezTaskID> tezTaskIDCache = new TezIDCache<>();
  private TezVertexID vertexId;

  /**
   * Constructs a TezTaskID object from given {@link TezVertexID}.
   * @param vertexID the vertexID object for this TezTaskID
   * @param id the tip number
   */
  public static TezTaskID getInstance(TezVertexID vertexID, int id) {
    Preconditions.checkArgument(vertexID != null, "vertexID cannot be null");
    return tezTaskIDCache.getInstance(new TezTaskID(vertexID, id));
  }

  @InterfaceAudience.Private
  public static void clearCache() {
    tezTaskIDCache.clear();
  }

  private TezTaskID(TezVertexID vertexID, int id) {
    super(id);
    Preconditions.checkArgument(vertexID != null, "vertexID cannot be null");
    this.vertexId = vertexID;
    this.serializingHash = getHashCode(true);
  }
  
  public int getSerializingHash() {
    return serializingHash;
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
    vertexId.appendTo(builder);
    builder.append(SEPARATOR);
    return tezTaskIdFormat.get().format(id, builder);
  }

  @Override
  public int hashCode() {
    return getHashCode(false);
  }

  public int getHashCode(boolean makePositive) {
    int code = vertexId.hashCode() * 535013 + id;
    if (makePositive) {
      code = (code < 0 ? -code : code);
    }
    return code;
  }

  @Override
  // Can't do much about this instance if used via the RPC layer. Any downstream
  // users can however avoid using this method.
  public void readFields(DataInput in) throws IOException {
    vertexId = TezVertexID.readTezVertexID(in);
    super.readFields(in);
  }
  
  public static TezTaskID readTezTaskID(DataInput in) throws IOException {
    TezVertexID vertexID = TezVertexID.readTezVertexID(in);
    int taskIdInt = TezID.readID(in);
    return getInstance(vertexID, taskIdInt);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    vertexId.write(out);
    super.write(out);
  }

  public static TezTaskID fromString(String taskIdStr) {
    try {
      int pos1 = taskIdStr.indexOf(SEPARATOR);
      int pos2 = taskIdStr.indexOf(SEPARATOR, pos1 + 1);
      int pos3 = taskIdStr.indexOf(SEPARATOR, pos2 + 1);
      int pos4 = taskIdStr.indexOf(SEPARATOR, pos3 + 1);
      int pos5 = taskIdStr.indexOf(SEPARATOR, pos4 + 1);
      String rmId = taskIdStr.substring(pos1 + 1, pos2);
      int appId = Integer.parseInt(taskIdStr.substring(pos2 + 1, pos3));
      int dagId = Integer.parseInt(taskIdStr.substring(pos3 + 1, pos4));
      int vId = Integer.parseInt(taskIdStr.substring(pos4 + 1, pos5));
      int id = Integer.parseInt(taskIdStr.substring(pos5 + 1));

      return TezTaskID.getInstance(
              TezVertexID.getInstance(
                  TezDAGID.getInstance(rmId, appId, dagId),
                  vId), id);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

}
