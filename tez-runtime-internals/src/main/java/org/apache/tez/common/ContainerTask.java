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

package org.apache.tez.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.runtime.api.impl.TaskSpec;

import com.google.common.collect.Maps;

public class ContainerTask implements Writable {

  TaskSpec taskSpec;
  boolean shouldDie;
  private Map<String, TezLocalResource> additionalResources;
  private Credentials credentials;
  private boolean credentialsChanged;

  public ContainerTask() {
  }

  public ContainerTask(TaskSpec taskSpec, boolean shouldDie,
      Map<String, TezLocalResource> additionalResources, Credentials credentials, boolean credentialsChanged) {
    this.taskSpec = taskSpec;
    this.shouldDie = shouldDie;
    this.additionalResources = additionalResources;
    this.credentials = credentials;
    this.credentialsChanged = credentialsChanged;
  }

  public TaskSpec getTaskSpec() {
    return taskSpec;
  }

  public boolean shouldDie() {
    return shouldDie;
  }
  
  public Map<String, TezLocalResource> getAdditionalResources() {
    return this.additionalResources;
  }
  
  public Credentials getCredentials() {
    return this.credentials;
  }
  
  public boolean haveCredentialsChanged() {
    return this.credentialsChanged;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(shouldDie);
    if (taskSpec != null) {
      out.writeBoolean(true);
      taskSpec.write(out);
    } else {
      out.writeBoolean(false);
    }
    if (additionalResources != null) {
      out.writeInt(additionalResources.size());
      for (Entry<String, TezLocalResource> lrEntry : additionalResources.entrySet()) {
        out.writeUTF(lrEntry.getKey());
        lrEntry.getValue().write(out);
      }
    } else {
      out.writeInt(-1);
    }
    out.writeBoolean(credentialsChanged);
    if (credentialsChanged) {
      out.writeBoolean(credentials != null);
      if (credentials != null) {
        credentials.write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    shouldDie = in.readBoolean();
    boolean taskComing = in.readBoolean();
    if (taskComing) {
      taskSpec = new TaskSpec();
      taskSpec.readFields(in);
    }
    int numAdditionalResources = in.readInt();
    additionalResources = Maps.newHashMap();
    if (numAdditionalResources != -1) {
      for (int i = 0 ; i < numAdditionalResources ; i++) {
        String resourceName = in.readUTF();
        TezLocalResource localResource = new TezLocalResource();
        localResource.readFields(in);
        additionalResources.put(resourceName, localResource);
      }
    }
    credentialsChanged = in.readBoolean();
    if (credentialsChanged) {
      boolean hasCredentials = in.readBoolean();
      if (hasCredentials) {
        credentials = new Credentials();
        credentials.readFields(in);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("shouldDie: ").append(shouldDie);
    sb.append(", TaskSpec: ");
    if (taskSpec == null) {
      sb.append("none");
    } else {
      sb.append(taskSpec);
    }
    sb.append(", additionalResources: ");
    if (additionalResources == null) {
      sb.append("none");
    } else {
      sb.append(additionalResources);
    }
    sb.append(", credentialsChanged: ").append(credentialsChanged);
    sb.append(", credentials: ");
    if (credentials == null) {
      sb.append("none");
    } else {
      sb.append("#tokens=").append(credentials.numberOfTokens())
      .append(", #secretKeys: ").append(credentials.numberOfSecretKeys());
    }
    return sb.toString();
  }

}
