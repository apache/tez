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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;

public class TezAMRMClientAsync<T extends ContainerRequest> extends AMRMClientAsyncImpl<T> {

  private static final Logger LOG = LoggerFactory.getLogger(TezAMRMClientAsync.class);

  /**
   * Used to track the type of requests at a given priority.
   */
  private static class LocalityRequestCounter {
    final AtomicInteger localityRequests;
    final AtomicInteger noLocalityRequests;

    public LocalityRequestCounter() {
      this.localityRequests = new AtomicInteger(0);
      this.noLocalityRequests = new AtomicInteger(0);
    }
  }

  private TreeMap<Priority, LocalityRequestCounter> knownRequestsByPriority =
    new TreeMap<Priority, LocalityRequestCounter>();

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
  
  public synchronized Priority getTopPriority() {
    Iterator<Priority> iter =
        knownRequestsByPriority.descendingKeySet().iterator();
    if (!iter.hasNext()) {
      return null;
    }
    return iter.next();
  }
  
  // Remove after YARN-1723 is fixed
  public synchronized void addNodeToBlacklist(NodeId nodeId) {
    client.updateBlacklist(Collections.singletonList(nodeId.getHost()), null);
  }
  
  //Remove after YARN-1723 is fixed
   public synchronized void removeNodeFromBlacklist(NodeId nodeId) {
     client.updateBlacklist(null, Collections.singletonList(nodeId.getHost()));
   }

  @Override
  public synchronized void addContainerRequest(T req) {
    super.addContainerRequest(req);
    boolean hasLocality = (req.getNodes() != null && !req.getNodes().isEmpty())
      || (req.getRacks() != null && !req.getRacks().isEmpty());
    LocalityRequestCounter lrc = knownRequestsByPriority.get(req.getPriority());
    if (lrc == null) {
      lrc = new LocalityRequestCounter();
      knownRequestsByPriority.put(req.getPriority(), lrc);
    }
    if (hasLocality) {
      lrc.localityRequests.incrementAndGet();
    } else {
      lrc.noLocalityRequests.incrementAndGet();
    }
  }

  @Override
  public synchronized void removeContainerRequest(T req) {
    super.removeContainerRequest(req);
    boolean hasLocality = (req.getNodes() != null && !req.getNodes().isEmpty())
      || (req.getRacks() != null && !req.getRacks().isEmpty());
    LocalityRequestCounter lrc = knownRequestsByPriority.get(
      req.getPriority());
    if (hasLocality) {
      lrc.localityRequests.decrementAndGet();
    } else {
      lrc.noLocalityRequests.decrementAndGet();
    }
    if (lrc.localityRequests.get() == 0
        && lrc.noLocalityRequests.get() == 0) {
      knownRequestsByPriority.remove(req.getPriority());
    }
  }

  public synchronized List<? extends Collection<T>>
    getMatchingRequestsForTopPriority(
        String resourceName, Resource capability) {
    // Sort based on reverse order. By default, Priority ordering is based on
    // highest numeric value being considered to be lowest priority.
    Iterator<Priority> iter =
      knownRequestsByPriority.descendingKeySet().iterator();
    if (!iter.hasNext()) {
      return Collections.emptyList();
    }
    Priority p = iter.next();
    LocalityRequestCounter lrc = knownRequestsByPriority.get(p);
    if (lrc.localityRequests.get() == 0) {
      // Fallback to ANY if there are no pending requests that require
      // locality matching
      if (LOG.isDebugEnabled()) {
        LOG.debug("Over-ridding location request for matching containers as"
          + " there are no pending requests that require locality at this"
          + " priority"
          + ", priority=" + p
          + ", localityRequests=" + lrc.localityRequests
          + ", noLocalityRequests=" + lrc.noLocalityRequests);
      }
      resourceName = ResourceRequest.ANY;
    }
    List<? extends Collection<T>> matched =
      getMatchingRequests(p, resourceName, capability);
    if (matched != null && !matched.isEmpty()) {
      return matched;
    }
    return Collections.emptyList();
  }

}
