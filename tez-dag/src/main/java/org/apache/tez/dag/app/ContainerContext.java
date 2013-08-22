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

package org.apache.tez.dag.app;

import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;

public class ContainerContext {

  private final Map<String, LocalResource> localResources;
  private final Credentials credentials;
  private final Map<String, String> environment;
  private final String javaOpts;

  public ContainerContext(
      Map<String, LocalResource> localResources, Credentials credentials,
      Map<String, String> environment, String javaOpts) {
    this.localResources = localResources;
    this.credentials = credentials;
    this.environment = environment;
    this.javaOpts = javaOpts;
  }

  public Map<String, LocalResource> getLocalResources() {
    return this.localResources;
  }
  
  public Credentials getCredentials() {
    return this.credentials;
  }
  
  public Map<String, String> getEnvironment() {
    return this.environment;
  }
  
  public String getJavaOpts() {
    return this.javaOpts;
  }
}
