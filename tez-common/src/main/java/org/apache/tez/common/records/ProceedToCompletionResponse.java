/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class ProceedToCompletionResponse implements Writable{

  private boolean shouldDie;
  private boolean readyToProceed;

  public ProceedToCompletionResponse() {
  }
  
  public ProceedToCompletionResponse(boolean shouldDie, boolean readyToProceed) {
    this.shouldDie = shouldDie;
    this.readyToProceed = readyToProceed;
  }

  /**
   * Indicates whether the task is required to proceed to completion, or should
   * terminate.
   * 
   * @return
   */
  public boolean shouldDie() {
    return this.shouldDie;
  }
  
  /**
   * Indicates whether the task is ready to proceed. Valid only if shouldDie is
   * false.
   * 
   * @return
   */
  public boolean readyToProceed() {
    return this.readyToProceed;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(shouldDie);
    out.writeBoolean(readyToProceed);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    shouldDie = in.readBoolean();
    readyToProceed = in.readBoolean();
  }

  @Override
  public String toString() {
    return "shouldDie: " + shouldDie + ", readyToProceed: " + readyToProceed;
  }
}
