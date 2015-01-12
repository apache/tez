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

package org.apache.tez.runtime.api.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;

import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.MergedInputContext;


public class TezMergedInputContextImpl implements MergedInputContext {

  private final UserPayload userPayload;
  private final String groupInputName;
  private final Map<String, MergedLogicalInput> groupInputsMap;
  private final InputReadyTracker inputReadyTracker;
  private final String[] workDirs;

  public TezMergedInputContextImpl(@Nullable UserPayload userPayload, String groupInputName,
                                   Map<String, MergedLogicalInput> groupInputsMap,
                                   InputReadyTracker inputReadyTracker, String[] workDirs) {
    checkNotNull(groupInputName, "groupInputName is null");
    checkNotNull(groupInputsMap, "input-group map is null");
    checkNotNull(inputReadyTracker, "inputReadyTracker is null");
    this.groupInputName = groupInputName;
    this.groupInputsMap = groupInputsMap;
    this.userPayload = userPayload;
    this.inputReadyTracker = inputReadyTracker;
    this.workDirs = workDirs;
  }

  @Override
  public UserPayload getUserPayload() {
    return userPayload;
  }
  
  @Override
  public void inputIsReady() {
    inputReadyTracker.setInputIsReady(groupInputsMap.get(groupInputName));
  }

  @Override
  public String[] getWorkDirs() {
    return Arrays.copyOf(workDirs, workDirs.length);
  }

}
