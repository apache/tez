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

import java.util.Collections;
import java.util.List;

import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.TezNamedEntityDescriptorProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for AM service plugins.
 *
 * This component parses the configured plugins for TaskSchedulers,
 * ContainerLaunchers, and TaskCommunicators from the AM configuration,
 * maintains their name-to-identifier mappings, and provides the parsed
 * descriptor lists used to initialize the corresponding managers.
 */
public class PluginManager {

  private static final Logger LOG = LoggerFactory.getLogger(PluginManager.class);

  private final AMPluginDescriptorProto amPluginDescriptorProto;

  // Plugin maps for task schedulers, container launchers, and task communicators
  private final BiMap<String, Integer> taskSchedulers = HashBiMap.create();
  private final BiMap<String, Integer> containerLaunchers = HashBiMap.create();
  private final BiMap<String, Integer> taskCommunicators = HashBiMap.create();

  /**
   * Wrapper for parsed plugin descriptors.
   *
   * The descriptor lists exposed by this class are unmodifiable snapshots
   * created at parse time. Callers must not attempt to modify these
   * collections; any modification attempts will throw an exception.
   */
  public static final class PluginDescriptors {
    private final List<NamedEntityDescriptor> taskSchedulerDescriptors;
    private final List<NamedEntityDescriptor> containerLauncherDescriptors;
    private final List<NamedEntityDescriptor> taskCommunicatorDescriptors;

    public PluginDescriptors(List<NamedEntityDescriptor> taskSchedulerDescriptors,
                             List<NamedEntityDescriptor> containerLauncherDescriptors,
                             List<NamedEntityDescriptor> taskCommunicatorDescriptors) {
      this.taskSchedulerDescriptors = Collections.unmodifiableList(taskSchedulerDescriptors);
      this.containerLauncherDescriptors = Collections.unmodifiableList(containerLauncherDescriptors);
      this.taskCommunicatorDescriptors = Collections.unmodifiableList(taskCommunicatorDescriptors);
    }

    public List<NamedEntityDescriptor> getTaskSchedulerDescriptors() {
      return taskSchedulerDescriptors;
    }

    public List<NamedEntityDescriptor> getContainerLauncherDescriptors() {
      return containerLauncherDescriptors;
    }

    public List<NamedEntityDescriptor> getTaskCommunicatorDescriptors() {
      return taskCommunicatorDescriptors;
    }
  }

  public PluginManager() {
    this(null);
  }

  public PluginManager(AMPluginDescriptorProto amPluginDescriptorProto) {
    this.amPluginDescriptorProto = amPluginDescriptorProto;
  }

  /**
   * Parse all plugins for task schedulers, container launchers, and task communicators.
   */
  public PluginDescriptors parseAllPlugins(boolean isLocal, UserPayload defaultPayload) {

    List<NamedEntityDescriptor> taskSchedulerDescriptors = Lists.newLinkedList();
    List<NamedEntityDescriptor> containerLauncherDescriptors = Lists.newLinkedList();
    List<NamedEntityDescriptor> taskCommDescriptors = Lists.newLinkedList();

    boolean tezYarnEnabled;
    boolean uberEnabled;
    if (!isLocal) {
      if (amPluginDescriptorProto == null) {
        tezYarnEnabled = true;
        uberEnabled = false;
      } else {
        tezYarnEnabled = amPluginDescriptorProto.getContainersEnabled();
        uberEnabled = amPluginDescriptorProto.getUberEnabled();
      }
    } else {
      tezYarnEnabled = false;
      uberEnabled = true;
    }

    // parse task scheduler plugins
    parsePlugin(taskSchedulerDescriptors, taskSchedulers,
        (amPluginDescriptorProto == null || amPluginDescriptorProto.getTaskSchedulersCount() == 0 ?
            null :
            amPluginDescriptorProto.getTaskSchedulersList()),
        tezYarnEnabled, uberEnabled, defaultPayload);

    // post-process task scheduler plugin descriptors
    processSchedulerDescriptors(taskSchedulerDescriptors, isLocal, defaultPayload, taskSchedulers);

    // parse container launcher plugins
    parsePlugin(containerLauncherDescriptors, containerLaunchers,
        (amPluginDescriptorProto == null ||
            amPluginDescriptorProto.getContainerLaunchersCount() == 0 ? null :
            amPluginDescriptorProto.getContainerLaunchersList()),
        tezYarnEnabled, uberEnabled, defaultPayload);

    // parse task communicator plugins
    parsePlugin(taskCommDescriptors, taskCommunicators,
        (amPluginDescriptorProto == null ||
            amPluginDescriptorProto.getTaskCommunicatorsCount() == 0 ? null :
            amPluginDescriptorProto.getTaskCommunicatorsList()),
        tezYarnEnabled, uberEnabled, defaultPayload);

    // Log plugin component information
    LOG.info(buildPluginComponentLog(taskSchedulerDescriptors, taskSchedulers, "TaskSchedulers"));
    LOG.info(buildPluginComponentLog(containerLauncherDescriptors, containerLaunchers, "ContainerLaunchers"));
    LOG.info(buildPluginComponentLog(taskCommDescriptors, taskCommunicators, "TaskCommunicators"));

    return new PluginDescriptors(taskSchedulerDescriptors, containerLauncherDescriptors, taskCommDescriptors);
  }

  /**
   * Parse a specific plugin type.
   */
  @VisibleForTesting
  public static void parsePlugin(List<NamedEntityDescriptor> resultList,
      BiMap<String, Integer> pluginMap, List<TezNamedEntityDescriptorProto> namedEntityDescriptorProtos,
      boolean tezYarnEnabled, boolean uberEnabled, UserPayload defaultPayload) {

    if (tezYarnEnabled) {
      // Default classnames will be populated by individual components
      NamedEntityDescriptor descriptor = new NamedEntityDescriptor(
          TezConstants.getTezYarnServicePluginName(), null).setUserPayload(defaultPayload);
      addDescriptor(resultList, pluginMap, descriptor);
    }

    if (uberEnabled) {
      // Default classnames will be populated by individual components
      NamedEntityDescriptor descriptor = new NamedEntityDescriptor(
          TezConstants.getTezUberServicePluginName(), null).setUserPayload(defaultPayload);
      addDescriptor(resultList, pluginMap, descriptor);
    }

    if (namedEntityDescriptorProtos != null) {
      for (TezNamedEntityDescriptorProto namedEntityDescriptorProto : namedEntityDescriptorProtos) {
        NamedEntityDescriptor descriptor = DagTypeConverters
            .convertNamedDescriptorFromProto(namedEntityDescriptorProto);
        addDescriptor(resultList, pluginMap, descriptor);
      }
    }
  }

  /**
   * Add a descriptor to the list and map.
   */
  public static void addDescriptor(List<NamedEntityDescriptor> list, BiMap<String, Integer> pluginMap,
                            NamedEntityDescriptor namedEntityDescriptor) {
    list.add(namedEntityDescriptor);
    pluginMap.put(list.get(list.size() - 1).getEntityName(), list.size() - 1);
  }

  /**
   * Process scheduler descriptors with framework-specific logic.
   */
  public void processSchedulerDescriptors(List<NamedEntityDescriptor> descriptors, boolean isLocal,
                                          UserPayload defaultPayload,
                                          BiMap<String, Integer> schedulerPluginMap) {
    if (isLocal) {
      boolean foundUberServiceName = false;
      for (NamedEntityDescriptor<?> descriptor : descriptors) {
        if (descriptor.getEntityName().equals(TezConstants.getTezUberServicePluginName())) {
          foundUberServiceName = true;
          break;
        }
      }
      Preconditions.checkState(foundUberServiceName);
    } else {
      boolean foundYarn = false;
      for (NamedEntityDescriptor descriptor : descriptors) {
        if (descriptor.getEntityName().equals(TezConstants.getTezYarnServicePluginName())) {
          foundYarn = true;
          break;
        }
      }
      if (!foundYarn) {
        NamedEntityDescriptor<?> yarnDescriptor =
            new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
                .setUserPayload(defaultPayload);
        addDescriptor(descriptors, schedulerPluginMap, yarnDescriptor);
      }
    }
  }

  /**
   * Get the task schedulers map.
   */
  public BiMap<String, Integer> getTaskSchedulers() {
    return taskSchedulers;
  }

  /**
   * Get the container launchers map.
   */
  public BiMap<String, Integer> getContainerLaunchers() {
    return containerLaunchers;
  }

  /**
   * Get the task communicators map.
   */
  public BiMap<String, Integer> getTaskCommunicators() {
    return taskCommunicators;
  }

  /**
   * Build a log message for plugin component information.
   */
  private String buildPluginComponentLog(List<NamedEntityDescriptor> namedEntityDescriptors, BiMap<String, Integer> map,
                                       String component) {
    StringBuilder sb = new StringBuilder();
    sb.append("AM Level configured ").append(component).append(": ");
    for (int i = 0; i < namedEntityDescriptors.size(); i++) {
      sb.append("[").append(i).append(":").append(map.inverse().get(i))
          .append(":").append(namedEntityDescriptors.get(i).getClassName()).append("]");
      if (i != namedEntityDescriptors.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }
}
