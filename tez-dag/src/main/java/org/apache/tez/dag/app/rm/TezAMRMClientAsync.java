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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;

public class TezAMRMClientAsync<T extends ContainerRequest> extends AMRMClientAsyncImpl<T> {

  private SortedMultiset<Priority> knownPriorities = TreeMultiset.create();

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
  public synchronized void addContainerRequest(T req) {
    super.addContainerRequest(req);
    knownPriorities.add(req.getPriority());
  }

  @Override
  public synchronized void removeContainerRequest(T req) {
    super.removeContainerRequest(req);
    knownPriorities.remove(req.getPriority());
  }

  public synchronized List<? extends Collection<T>>
    getMatchingRequestsForTopPriority(
        String resourceName, Resource capability) {
    // Sort based on reverse order. By default, Priority ordering is based on
    // highest numeric value being considered to be lowest priority.
    Iterator<Priority> iter = knownPriorities.descendingMultiset().iterator();
    if (!iter.hasNext()) {
      return Collections.emptyList();
    }
    List<? extends Collection<T>> matched =
      getMatchingRequests(iter.next(), resourceName, capability);
    if (matched != null && !matched.isEmpty()) {
      return matched;
    }
    return Collections.emptyList();
  }

}
