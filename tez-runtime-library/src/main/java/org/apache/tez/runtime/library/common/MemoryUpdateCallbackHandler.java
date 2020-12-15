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

package org.apache.tez.runtime.library.common;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.tez.runtime.api.MemoryUpdateCallback;

import org.apache.tez.common.Preconditions;

@Public
@Evolving
public class MemoryUpdateCallbackHandler extends MemoryUpdateCallback {

  private long assignedMemory;
  private boolean updated = false;

  @Override
  public synchronized void memoryAssigned(long assignedSize) {
    updated = true;
    this.assignedMemory = assignedSize;
  }

  public synchronized long getMemoryAssigned() {
    return this.assignedMemory;
  }

  public synchronized void validateUpdateReceived() {
    Preconditions.checkState(updated == true, "Iniital memory update not received");
  }
}