/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.frameworkplugins.yarn;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.TezYarnClient;
import org.apache.tez.frameworkplugins.ClientFrameworkService;

/**
 * YARN-based client framework service implementation.
 * Provides default YARN framework client functionality.
 */
public class YarnClientFrameworkService implements ClientFrameworkService {

  @Override
  public FrameworkClient newFrameworkClient() {
    return new TezYarnClient(YarnClient.createYarnClient());
  }
}
