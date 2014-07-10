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

public enum EventType {
  TASK_ATTEMPT_COMPLETED_EVENT,
  TASK_ATTEMPT_FAILED_EVENT,
  DATA_MOVEMENT_EVENT,
  INPUT_READ_ERROR_EVENT,
  INPUT_FAILED_EVENT,
  TASK_STATUS_UPDATE_EVENT,
  VERTEX_MANAGER_EVENT,
  ROOT_INPUT_DATA_INFORMATION_EVENT,
  COMPOSITE_DATA_MOVEMENT_EVENT,
  ROOT_INPUT_INITIALIZER_EVENT,
}
