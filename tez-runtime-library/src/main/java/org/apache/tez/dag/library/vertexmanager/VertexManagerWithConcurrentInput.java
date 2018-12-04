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

package org.apache.tez.dag.library.vertexmanager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.ConcurrentEdgeTriggerType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tez.dag.api.EdgeProperty.SchedulingType.CONCURRENT;

public class VertexManagerWithConcurrentInput extends VertexManagerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(VertexManagerWithConcurrentInput.class);

  private final Map<String, Boolean> srcVerticesConfigured = Maps.newConcurrentMap();
  private int managedTasks;
  private AtomicBoolean tasksScheduled = new AtomicBoolean(false);
  private AtomicBoolean onVertexStartedDone = new AtomicBoolean(false);
  private Configuration vertexConfig;
  private String vertexName;
  private ConcurrentEdgeTriggerType edgeTriggerType;
  private volatile boolean allSrcVerticesConfigured;

  int completedUpstreamTasks;

  public VertexManagerWithConcurrentInput(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() {
    UserPayload userPayload = getContext().getUserPayload();
    if (userPayload == null || userPayload.getPayload() == null ||
        userPayload.getPayload().limit() == 0) {
      throw new TezUncheckedException("Could not initialize VertexManagerWithConcurrentInput"
          + " from provided user payload");
    }
    managedTasks = getContext().getVertexNumTasks(getContext().getVertexName());
    Map<String, EdgeProperty> edges = getContext().getInputVertexEdgeProperties();
    for (Map.Entry<String, EdgeProperty> entry : edges.entrySet()) {
      if (!CONCURRENT.equals(entry.getValue().getSchedulingType())) {
        throw new TezUncheckedException("All input edges to vertex " + vertexName +
            "  must be CONCURRENT.");
      }
      String srcVertex = entry.getKey();
      srcVerticesConfigured.put(srcVertex, false);
      getContext().registerForVertexStateUpdates(srcVertex, EnumSet.of(VertexState.CONFIGURED));
    }

    try {
      vertexConfig = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    edgeTriggerType = ConcurrentEdgeTriggerType.valueOf(
        vertexConfig.get(TezConfiguration.TEZ_CONCURRENT_EDGE_TRIGGER_TYPE,
            TezConfiguration.TEZ_CONCURRENT_EDGE_TRIGGER_TYPE_DEFAULT));
    if (!ConcurrentEdgeTriggerType.SOURCE_VERTEX_CONFIGURED.equals(edgeTriggerType)) {
      // pending TEZ-3999
      throw new TezUncheckedException("Only support SOURCE_VERTEX_CONFIGURED triggering type for now.");
    }
    LOG.info("VertexManagerWithConcurrentInput initialized with edgeTriggerType {}.", edgeTriggerType);

    vertexName = getContext().getVertexName();
    completedUpstreamTasks = 0;
  }

  @Override
  public synchronized void onVertexStarted(List<TaskAttemptIdentifier> completions) {
    onVertexStartedDone.set(true);
    scheduleTasks();
  }

  @Override
  public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    VertexState state = stateUpdate.getVertexState();
    String fromVertex = stateUpdate.getVertexName();
    if (!srcVerticesConfigured.containsKey(fromVertex)) {
      throw new IllegalArgumentException("Not expecting state update from vertex:" +
          fromVertex + " in vertex: " + this.vertexName);
    }

    if (!VertexState.CONFIGURED.equals(state)) {
      throw new IllegalArgumentException("Received incorrect state notification : " +
          state + " from vertex: " + fromVertex + " in vertex: " + this.vertexName);
    }

    LOG.info("Received configured notification: " + state + " for vertex: "
        + fromVertex + " in vertex: " + this.vertexName);
    srcVerticesConfigured.put(fromVertex, true);

    // check for source vertices completely configured
    boolean checkAllSrcVerticesConfigured = true;
    for (Map.Entry<String, Boolean> entry : srcVerticesConfigured.entrySet()) {
      if (!entry.getValue()) {
        // vertex not configured
        LOG.info("Waiting for vertex {} in vertex {} ", entry.getKey(), this.vertexName);
        checkAllSrcVerticesConfigured = false;
        break;
      }
    }
    allSrcVerticesConfigured = checkAllSrcVerticesConfigured;

    scheduleTasks();
  }

  @Override
  public synchronized void onSourceTaskCompleted(TaskAttemptIdentifier attempt) {
    completedUpstreamTasks ++;
    LOG.info("Source task attempt {} completion received at vertex {}", attempt, this.vertexName);
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName,
                                      InputDescriptor inputDescriptor, List<Event> events) {
  }

  private void scheduleTasks() {
    if (!onVertexStartedDone.get()) {
      // vertex not started yet
      return;
    }
    if (tasksScheduled.get()) {
      // already scheduled
      return;
    }

    if (!canScheduleTasks()) {
      return;
    }

    tasksScheduled.compareAndSet(false, true);
    List<VertexManagerPluginContext.ScheduleTaskRequest> tasksToStart = Lists.newArrayListWithCapacity(managedTasks);
    for (int i = 0; i < managedTasks; ++i) {
      tasksToStart.add(VertexManagerPluginContext.ScheduleTaskRequest.create(i, null));
    }

    if (!tasksToStart.isEmpty()) {
      LOG.info("Starting {} tasks in {}.", tasksToStart.size(), this.vertexName);
      getContext().scheduleTasks(tasksToStart);
    }
    // all tasks scheduled. Can call vertexManagerDone().
  }

  private boolean canScheduleTasks() {
    if (edgeTriggerType.equals(ConcurrentEdgeTriggerType.SOURCE_VERTEX_CONFIGURED)) {
      return allSrcVerticesConfigured;
    } else {
      // pending TEZ-3999
      throw new TezUncheckedException("Only support SOURCE_VERTEX_CONFIGURED triggering type for now.");
    }
  }


  /**
   * Create a {@link VertexManagerPluginDescriptor} builder that can be used to
   * configure the plugin.
   *
   * @param conf
   *          {@link Configuration} May be modified in place. May be null if the
   *          configuration parameters are to be set only via code. If
   *          configuration values may be changed at runtime via a config file
   *          then pass in a {@link Configuration} that is initialized from a
   *          config file. The parameters that are not overridden in code will
   *          be derived from the Configuration object.
   * @return {@link ConcurrentInputVertexManagerConfigBuilder}
   */
  public static ConcurrentInputVertexManagerConfigBuilder createConfigBuilder(
      @Nullable Configuration conf) {
    return new ConcurrentInputVertexManagerConfigBuilder(conf);
  }

  /**
   * Helper class to configure VertexManagerWithConcurrentInput
   */
  public static final class ConcurrentInputVertexManagerConfigBuilder {
    private final Configuration conf;

    private ConcurrentInputVertexManagerConfigBuilder(@Nullable Configuration conf) {
      if (conf == null) {
        this.conf = new Configuration(false);
      } else {
        this.conf = conf;
      }
    }

    public VertexManagerPluginDescriptor build() {
      VertexManagerPluginDescriptor desc =
          VertexManagerPluginDescriptor.create(
              VertexManagerWithConcurrentInput.class.getName());

      try {
        return desc.setUserPayload(TezUtils
            .createUserPayloadFromConf(this.conf));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
  }

}
