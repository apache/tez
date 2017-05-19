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

package org.apache.tez.dag.app.rm.container;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;

public class AMContainerMap extends AbstractService implements EventHandler<AMContainerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(AMContainerMap.class);

  private final ContainerHeartbeatHandler chh;
  private final TaskCommunicatorManagerInterface tal;
  private final AppContext context;
  private final ContainerSignatureMatcher containerSignatureMatcher;
  @VisibleForTesting
  final ConcurrentHashMap<ContainerId, AMContainer> containerMap;
  private String auxiliaryService;

  public AMContainerMap(ContainerHeartbeatHandler chh, TaskCommunicatorManagerInterface tal,
      ContainerSignatureMatcher containerSignatureMatcher, AppContext context) {
    super("AMContainerMaps");
    this.chh = chh;
    this.tal = tal;
    this.context = context;
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.containerMap = new ConcurrentHashMap<ContainerId, AMContainer>();
    this.auxiliaryService = context.getAMConf().get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
  }

  @Override
  public void handle(AMContainerEvent event) {
    AMContainer container = containerMap.get(event.getContainerId());
    if (container != null) {
      container.handle(event);
    } else {
      LOG.info("Event for unknown container: " + event.getContainerId());
    }
  }

  public boolean addContainerIfNew(Container container, int schedulerId, int launcherId, int taskCommId) {
    AMContainer amc = createAmContainer(container, chh, tal,
        containerSignatureMatcher, context, schedulerId, launcherId, taskCommId, auxiliaryService);
    return (containerMap.putIfAbsent(container.getId(), amc) == null);
  }

  AMContainer createAmContainer(Container container,
                                ContainerHeartbeatHandler chh,
                                TaskCommunicatorManagerInterface tal,
                                ContainerSignatureMatcher signatureMatcher,
                                AppContext appContext, int schedulerId,
                                int launcherId, int taskCommId, String auxiliaryService) {
    AMContainer amc = new AMContainerImpl(container, chh, tal,
        signatureMatcher, appContext, schedulerId, launcherId, taskCommId, auxiliaryService);
    return amc;
  }

  public AMContainer get(ContainerId containerId) {
    return containerMap.get(containerId);
  }

  public Collection<AMContainer> values() {
    return containerMap.values();
  }

  public void dagComplete(DAG dag){
    AMContainerHelpers.dagComplete(dag.getID());
    // Cleanup completed containers after a query completes.
    cleanupCompletedContainers();
  }

  private void cleanupCompletedContainers() {
    Iterator<Map.Entry<ContainerId, AMContainer>> iterator = containerMap.entrySet().iterator();
    int count = 0;
    while (iterator.hasNext()) {
      Map.Entry<ContainerId, AMContainer> entry = iterator.next();
      AMContainer amContainer = entry.getValue();
      if (AMContainerState.COMPLETED.equals(amContainer.getState()) || amContainer.isInErrorState()) {
        iterator.remove();
        count++;
      }
    }
    LOG.info(
        "Cleaned up completed containers on dagComplete. Removed={}, Remaining={}",
        count, containerMap.size());
  }

}
