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

package org.apache.tez.common;

import org.apache.tez.records.TezID;
import org.apache.tez.records.TezTaskAttemptID;
import org.apache.tez.records.TezTaskID;

public class IDUtils {

  /** Construct a TaskID object from given string 
   * @return constructed TaskID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   */
  public static TezTaskID toTaskId(String str) 
    throws IllegalArgumentException {
    if(str == null)
      return null;
    String exceptionMsg = null;
    try {
      String[] parts = str.split("_");
      if(parts.length == 5) {
        if(parts[0].equals(TezTaskID.TASK)) {
          String type = parts[3];
          // FIXME
          // TezTaskType.valueOf(String.valueOf(type.charAt(0)));
          if(type != null) {
            return new TezTaskID(
                parts[1], 
                Integer.parseInt(parts[2]),
                type, Integer.parseInt(parts[4]));
          } else
            exceptionMsg = "Bad TaskType identifier. TaskId string : " + str
                + " is not properly formed.";
        }
      }
    }catch (Exception ex) {//fall below
    }
    if (exceptionMsg == null) {
      exceptionMsg = "TaskId string : " + str + " is not properly formed";
    }
    throw new IllegalArgumentException(exceptionMsg);
  }

  /** Construct a TaskAttemptID object from given string 
   * @return constructed TaskAttemptID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   */
  public static TezTaskAttemptID toTaskAttemptId(String str
                                      ) throws IllegalArgumentException {
    if(str == null)
      return null;
    String exceptionMsg = null;
    try {
      String[] parts = str.split(Character.toString(TezID.SEPARATOR));
      if(parts.length == 6) {
        if(parts[0].equals(TezTaskAttemptID.ATTEMPT)) {
          String type = parts[3];         
          if(type != null) {
            return new TezTaskAttemptID(
                parts[1],
                Integer.parseInt(parts[2]),
                type, Integer.parseInt(parts[4]), 
                Integer.parseInt(parts[5]));  
          } else
            exceptionMsg = "Bad TaskType identifier. TaskAttemptId string : "
                + str + " is not properly formed.";
        }
      }
    } catch (Exception ex) {
      //fall below
    }
    if (exceptionMsg == null) {
      exceptionMsg = "TaskAttemptId string : " + str
          + " is not properly formed";
    }
    throw new IllegalArgumentException(exceptionMsg);
  }


}
