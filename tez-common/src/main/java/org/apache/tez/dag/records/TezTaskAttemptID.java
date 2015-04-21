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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * TezTaskAttemptID represents the immutable and unique identifier for
 * a task attempt. Each task attempt is one particular instance of a Tez Task
 * identified by its TezTaskID.
 *
 * TezTaskAttemptID consists of 2 parts. First part is the
 * {@link TezTaskID}, that this TaskAttemptID belongs to.
 * Second part is the task attempt number. <br>
 * <p>
 * Applications should never construct or parse TaskAttemptID strings
 * , but rather use appropriate constructors or {@link Class#forName(String)}
 * method.
 *
 * @see TezTaskID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TezTaskAttemptID extends TezID {
  public static final String ATTEMPT = "attempt";
  private TezTaskID taskId;
  
  private static LoadingCache<TezTaskAttemptID, TezTaskAttemptID> taskAttemptIDCache = CacheBuilder.newBuilder().softValues().
      build(
          new CacheLoader<TezTaskAttemptID, TezTaskAttemptID>() {
            @Override
            public TezTaskAttemptID load(TezTaskAttemptID key) throws Exception {
              return key;
            }
          }
      );
  
  // Public for Writable serialization. Verify if this is actually required.
  public TezTaskAttemptID() {
  }
  
  /**
   * Constructs a TaskAttemptID object from given {@link TezTaskID}.  
   * @param taskID TaskID that this task belongs to  
   * @param id the task attempt number
   */
  public static TezTaskAttemptID getInstance(TezTaskID taskID, int id) {
    return taskAttemptIDCache.getUnchecked(new TezTaskAttemptID(taskID, id));
  }

  @InterfaceAudience.Private
  public static void clearCache() {
    taskAttemptIDCache.invalidateAll();
    taskAttemptIDCache.cleanUp();
  }

  private TezTaskAttemptID(TezTaskID taskId, int id) {
    super(id);
    if(taskId == null) {
      throw new IllegalArgumentException("taskId cannot be null");
    }
    this.taskId = taskId;
  }

  /** Returns the {@link TezTaskID} object that this task attempt belongs to */
  public TezTaskID getTaskID() {
    return taskId;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TezTaskAttemptID that = (TezTaskAttemptID)o;
    return this.taskId.equals(that.taskId);
  }
  
  /**
   * Add the unique string to the StringBuilder
   * @param builder the builder to append ot
   * @return the builder that was passed in.
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return taskId.appendTo(builder).append(SEPARATOR).append(id);
  }
  
  @Override
  public int hashCode() {
    return taskId.hashCode() * 539501 + id;
  }
  
  /**Compare TaskIds by first tipIds, then by task numbers. */
  @Override
  public int compareTo(TezID o) {
    TezTaskAttemptID that = (TezTaskAttemptID)o;
    int tipComp = this.taskId.compareTo(that.taskId);
    if(tipComp == 0) {
      return this.id - that.id;
    }
    else return tipComp;
  }
  @Override
  public String toString() { 
    return appendTo(new StringBuilder(ATTEMPT)).toString();
  }
  
  @Override
  // Can't do much about this instance if used via the RPC layer. Any downstream
  // users can however avoid using this method.
  public void readFields(DataInput in) throws IOException {
    taskId = TezTaskID.readTezTaskID(in);
    super.readFields(in);
  }
  
  public static TezTaskAttemptID readTezTaskAttemptID(DataInput in) throws IOException {
    TezTaskID taskID = TezTaskID.readTezTaskID(in);
    int attemptIdInt = TezID.readID(in);
    return getInstance(taskID, attemptIdInt);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskId.write(out);
    super.write(out);
  }

  protected static final ThreadLocal<NumberFormat> tezTaskAttemptIdFormat = new ThreadLocal<NumberFormat>() {
    @Override
    public NumberFormat initialValue() {
      NumberFormat fmt = NumberFormat.getInstance();
      fmt.setGroupingUsed(false);
      fmt.setMinimumIntegerDigits(1);
      return fmt;
    }
  };

  public static TezTaskAttemptID fromString(String taIdStr) {
    try {
      String[] split = taIdStr.split("_");
      String rmId = split[1];
      int appId = TezDAGID.tezAppIdFormat.get().parse(split[2]).intValue();
      int dagId = TezDAGID.tezDagIdFormat.get().parse(split[3]).intValue();
      int vId = TezVertexID.tezVertexIdFormat.get().parse(split[4]).intValue();
      int taskId = TezTaskID.tezTaskIdFormat.get().parse(split[5]).intValue();
      int id = tezTaskAttemptIdFormat.get().parse(split[6]).intValue();

      return TezTaskAttemptID.getInstance(
          TezTaskID.getInstance(
              TezVertexID.getInstance(
                  TezDAGID.getInstance(rmId, appId, dagId),
                  vId), taskId), id);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

}
