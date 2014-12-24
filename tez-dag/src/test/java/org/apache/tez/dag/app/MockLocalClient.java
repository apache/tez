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

package org.apache.tez.dag.app;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.client.LocalClient;

public class MockLocalClient extends LocalClient {
  MockDAGAppMaster mockApp;
  AtomicBoolean mockAppLauncherGoFlag;
  Clock mockClock;
  final boolean initFailFlag;
  final boolean startFailFlag;

  public MockLocalClient(AtomicBoolean mockAppLauncherGoFlag, Clock clock) {
    this.mockAppLauncherGoFlag = mockAppLauncherGoFlag;
    this.mockClock = clock;
    this.initFailFlag = false;
    this.startFailFlag = false;
  }
  
  public MockLocalClient(AtomicBoolean mockAppLauncherGoFlag, Clock clock,
      boolean initFailFlag, boolean startFailFlag) {
    this.mockAppLauncherGoFlag = mockAppLauncherGoFlag;
    this.mockClock = clock;
    this.initFailFlag = initFailFlag;
    this.startFailFlag = startFailFlag;
  }

  protected DAGAppMaster createDAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId cId, String currentHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime, boolean isSession, String userDir) {
    mockApp = new MockDAGAppMaster(applicationAttemptId, cId, currentHost, nmPort, nmHttpPort,
        (mockClock!=null ? mockClock : clock), appSubmitTime, isSession, userDir, mockAppLauncherGoFlag,
        initFailFlag, startFailFlag);
    return mockApp;
  }
  
  public MockDAGAppMaster getMockApp() {
    return mockApp;
  }
}
