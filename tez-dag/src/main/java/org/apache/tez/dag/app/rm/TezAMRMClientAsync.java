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

package org.apache.tez.dag.app.rm;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;

public class TezAMRMClientAsync<T extends ContainerRequest> extends AMRMClientAsyncImpl<T> {

  private Multiset<Priority> knownPriorities = LinkedHashMultiset.create();

  public static <T extends ContainerRequest> TezAMRMClientAsync<T> createAMRMClientAsync(
      int intervalMs, CallbackHandler callbackHandler) {
    return new TezAMRMClientAsync<T>(intervalMs, callbackHandler);
  }

  public TezAMRMClientAsync(int intervalMs, CallbackHandler callbackHandler) {
    super(new AMRMClientImpl<T>(), intervalMs, callbackHandler);
  }

  public TezAMRMClientAsync(
      AMRMClient<T> client,
      int intervalMs,
      AMRMClientAsync.CallbackHandler callbackHandler) {
    super(client, intervalMs, callbackHandler);
  }

  @Override
  public void addContainerRequest(T req) {
    super.addContainerRequest(req);
    knownPriorities.add(req.getPriority());
  }

  @Override
  public void removeContainerRequest(T req) {
    super.removeContainerRequest(req);
    knownPriorities.remove(req.getPriority());
  }

  public List<List<? extends Collection<T>>> getMatchingRequestsWithoutPriority(
      String resourceName, Resource capability) {
    List<List<? extends Collection<T>>> result = null;
    for (Priority p : knownPriorities) {
      if (result == null) {
        result = new LinkedList<List<? extends Collection<T>>>();
      }
      result.add(getMatchingRequests(p, resourceName, capability));
    }
    if (result == null) {
      result = Collections.emptyList();
    }
    return result;
  }
}
