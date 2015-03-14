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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;

public class MockTezClient extends TezClient {
  MockLocalClient client;
  
  MockTezClient(String name, TezConfiguration tezConf, boolean isSession,
      Map<String, LocalResource> localResources, Credentials credentials,
      Clock clock, AtomicBoolean mockAppLauncherGoFlag) {
    super(name, tezConf, isSession, localResources, credentials);
    this.client = new MockLocalClient(mockAppLauncherGoFlag, clock);
  }
  
  MockTezClient(String name, TezConfiguration tezConf, boolean isSession,
      Map<String, LocalResource> localResources, Credentials credentials,
      Clock clock, AtomicBoolean mockAppLauncherGoFlag,
 boolean initFailFlag, boolean startFailFlag) {
    this(name, tezConf, isSession, localResources, credentials, clock, mockAppLauncherGoFlag,
        initFailFlag, startFailFlag, 1, 1);
  }
  
  MockTezClient(String name, TezConfiguration tezConf, boolean isSession,
      Map<String, LocalResource> localResources, Credentials credentials,
      Clock clock, AtomicBoolean mockAppLauncherGoFlag,
      boolean initFailFlag, boolean startFailFlag, int concurrency, int containers) {
    super(name, tezConf, isSession, localResources, credentials);
    this.client = new MockLocalClient(mockAppLauncherGoFlag, clock, initFailFlag, startFailFlag, 
        concurrency, containers);
  }

  protected FrameworkClient createFrameworkClient() {
    return client;
  }
  
  public MockLocalClient getLocalClient() {
    return client;
  }

}
