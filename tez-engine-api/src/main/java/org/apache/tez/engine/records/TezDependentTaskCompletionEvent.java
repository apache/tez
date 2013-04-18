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

package org.apache.tez.engine.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This is used to track task completion events on 
 * job tracker. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
// TODO TEZAM3 This needs to be more generic. Maybe some kind of a serialized
// blob - which can be interpretted by the Input plugin.
public class TezDependentTaskCompletionEvent implements Writable {
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  // TODO EVENTUALLY - Remove TIPFAILED state ?
  static public enum Status {FAILED, KILLED, SUCCEEDED, OBSOLETE, TIPFAILED};
    
  private int eventId;
  // TODO EVENTUALLY - rename.
  private String taskTrackerHttp;
  private int taskRunTime; // using int since runtime is the time difference
  private TezTaskAttemptID taskAttemptId;
  Status status;
  // TODO TEZAM2 Get rid of the isMap field. Job specific type information can be determined from TaskAttemptId.getTaskType
//  boolean isMap = false;
  public static final TezDependentTaskCompletionEvent[] EMPTY_ARRAY = 
    new TezDependentTaskCompletionEvent[0];

  public TezDependentTaskCompletionEvent() {
    taskAttemptId = new TezTaskAttemptID();
  }
  
  /**
   * Constructor. eventId should be created externally and incremented
   * per event for each job. 
   * @param eventId event id, event id should be unique and assigned in
   *  incrementally, starting from 0. 
   * @param taskId task id
   * @param status task's status 
   * @param taskTrackerHttp task tracker's host:port for http. 
   */
  public TezDependentTaskCompletionEvent(int eventId, 
                             TezTaskAttemptID taskId,
//                             boolean isMap,
                             Status status, 
                             String taskTrackerHttp,
                             int runTime){
      
    this.taskAttemptId = taskId;
//    this.isMap = isMap;
    this.eventId = eventId; 
    this.status =status; 
    this.taskTrackerHttp = taskTrackerHttp;
    this.taskRunTime = runTime;
  }
  /**
   * Returns event Id. 
   * @return event id
   */
  public int getEventId() {
    return eventId;
  }

  /**
   * Returns task id. 
   * @return task id
   */
  public TezTaskAttemptID getTaskAttemptID() {
    return taskAttemptId;
  }
  
  /**
   * Returns enum Status.SUCESS or Status.FAILURE.
   * @return task tracker status
   */
  public Status getStatus() {
    return status;
  }
  /**
   * http location of the tasktracker where this task ran. 
   * @return http location of tasktracker user logs
   */
  public String getTaskTrackerHttp() {
    return taskTrackerHttp;
  }

  /**
   * Returns time (in millisec) the task took to complete. 
   */
  public int getTaskRunTime() {
    return taskRunTime;
  }

  /**
   * Set the task completion time
   * @param taskCompletionTime time (in millisec) the task took to complete
   */
  protected void setTaskRunTime(int taskCompletionTime) {
    this.taskRunTime = taskCompletionTime;
  }

  /**
   * set event Id. should be assigned incrementally starting from 0. 
   * @param eventId
   */
  public void setEventId(int eventId) {
    this.eventId = eventId;
  }

  /**
   * Sets task id. 
   * @param taskId
   */
  public void setTaskAttemptID(TezTaskAttemptID taskId) {
    this.taskAttemptId = taskId;
  }
  
  /**
   * Set task status. 
   * @param status
   */
  public void setTaskStatus(Status status) {
    this.status = status;
  }
  
  /**
   * Set task tracker http location. 
   * @param taskTrackerHttp
   */
  public void setTaskTrackerHttp(String taskTrackerHttp) {
    this.taskTrackerHttp = taskTrackerHttp;
  }
    
  @Override
  public String toString(){
    StringBuffer buf = new StringBuffer(); 
    buf.append("Task Id : "); 
    buf.append(taskAttemptId); 
    buf.append(", Status : ");  
    buf.append(status.name());
    return buf.toString();
  }
    
  @Override
  public boolean equals(Object o) {
    if(o == null)
      return false;
    if(o.getClass().equals(this.getClass())) {
      TezDependentTaskCompletionEvent event = (TezDependentTaskCompletionEvent) o;
      return this.eventId == event.getEventId()
             && this.status.equals(event.getStatus())
             && this.taskAttemptId.equals(event.getTaskAttemptID()) 
             && this.taskRunTime == event.getTaskRunTime()
             && this.taskTrackerHttp.equals(event.getTaskTrackerHttp());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toString().hashCode(); 
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskAttemptId.write(out);
//    out.writeBoolean(isMap);
    WritableUtils.writeEnum(out, status);
    WritableUtils.writeString(out, taskTrackerHttp);
    WritableUtils.writeVInt(out, taskRunTime);
    WritableUtils.writeVInt(out, eventId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskAttemptId.readFields(in);
//    isMap = in.readBoolean();
    status = WritableUtils.readEnum(in, Status.class);
    taskTrackerHttp = WritableUtils.readString(in);
    taskRunTime = WritableUtils.readVInt(in);
    eventId = WritableUtils.readVInt(in);
    
  }
  
}
