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

package org.apache.tez.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableUtils;


/**
 * TaskID represents the immutable and unique identifier for 
 * a Map or Reduce Task. Each TaskID encompasses multiple attempts made to
 * execute the Map or Reduce Task, each of which are uniquely indentified by
 * their TaskAttemptID.
 * 
 * TaskID consists of 3 parts. First part is the {@link TezJobID}, that this 
 * TaskInProgress belongs to. Second part of the TaskID is either 'm' or 'r' 
 * representing whether the task is a map task or a reduce task. 
 * And the third part is the task number. <br> 
 * An example TaskID is : 
 * <code>task_200707121733_0003_m_000005</code> , which represents the
 * fifth map task in the third job running at the jobtracker 
 * started at <code>200707121733</code>. 
 * <p>
 * Applications should never construct or parse TaskID strings
 * , but rather use appropriate constructors or {@link #forName(String)} 
 * method. 
 * 
 * @see TezJobID
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
  
  private TezJobID jobId;
  private String type;
  
  /**
   * Constructs a TaskID object from given {@link TezJobID}.  
   * @param jobId JobID that this tip belongs to 
   * @param type the {@link TezTaskType} of the task 
   * @param id the tip number
   */
  public TezTaskID(TezJobID jobId, String type, int id) {
    super(id);
    if(jobId == null) {
      throw new IllegalArgumentException("jobId cannot be null");
    }
    this.jobId = jobId;
    this.type = type;
  }
  
  /**
   * Constructs a TaskInProgressId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number 
   * @param type the TaskType 
   * @param id the tip number
   */
  public TezTaskID(String jtIdentifier, int jobId, String type, int id) {
    this(new TezJobID(jtIdentifier, jobId), type, id);
  }
  
  public TezTaskID() { 
    jobId = new TezJobID();
  }
  
  /** Returns the {@link TezJobID} object that this tip belongs to */
  public TezJobID getJobID() {
    return jobId;
  }

  /**
   * Get the type of the task
   */
  public String getTaskType() {
    return type;
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    jobId.readFields(in);
    type = WritableUtils.readString(in);
//    String typeClassName = WritableUtils.readString(in);
//    String enumVal = WritableUtils.readString(in);
//    
//    // TODO EVENTUALLY Move this into RelcetionUtils
//    try {
//      Class<?> cls = Class.forName(typeClassName);
//      if (TezTaskType.class.isAssignableFrom(cls)) {
//        Method m = cls.getMethod("valueOf", String.class);
//        m.setAccessible(true);
//        type = (TezTaskType) m.invoke(null, enumVal);
//
//      } else {
//        throw new RuntimeException("Type: + " + typeClassName
//            + " should be a subclass of " + TezTaskType.class.getName());
//      }
//    } catch (ClassNotFoundException e) {
//      throw new RuntimeException("Unable to load typeClass: " + typeClassName,
//          e);
//    } catch (Exception e) {
//      throw new RuntimeException("Failed to create instance of: "
//          + typeClassName, e);
//    }
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    jobId.write(out);
    WritableUtils.writeString(out, type);
//    WritableUtils.writeString(out, type.getClass().getName());
//    // TODO Maybe define a method for default instantiation.
//    WritableUtils.writeString(out, type.toSerializedString());
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TezTaskID that = (TezTaskID)o;
    return this.type.equals(that.type) && this.jobId.equals(that.jobId);
  }

  /**Compare TaskInProgressIds by first jobIds, then by tip numbers and type.*/
  @Override
  public int compareTo(TezID o) {
    TezTaskID that = (TezTaskID)o;
    int jobComp = this.jobId.compareTo(that.jobId);
    if(jobComp == 0) {
      if(this.type == that.type) {
        return this.id - that.id;
      }
      else {
        return this.type.toString().compareTo(that.type.toString());
      }
    }
    else return jobComp;
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
    return jobId.appendTo(builder).
                 append(SEPARATOR).
                 append(type.toString()).
                 append(SEPARATOR).
                 append(idFormat.format(id));
  }
  
  @Override
  public int hashCode() {
    return jobId.hashCode() * 524287 + id;
  }
  
}
