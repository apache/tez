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

package org.apache.tez.dag.app.dag.impl;

import java.io.IOException;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputUpdatePayloadEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

public class RootInputVertexManager extends VertexManagerPlugin {

  private static final Logger LOG = 
      LoggerFactory.getLogger(RootInputVertexManager.class);

  /**
   * Enables slow start for the vertex. Based on min/max fraction configs
   */
  public static final String TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START =
      "tez.root-input-vertex-manager.enable.slow-start";
  public static final boolean
      TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START_DEFAULT = false;

  /**
   * In case of a Broadcast connection, the fraction of source tasks which
   * should complete before tasks for the current vertex are scheduled
   */
  public static final String TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION =
      "tez.root-input-vertex-manager.min-src-fraction";
  public static final float
      TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT = 0.25f;

  /**
   * In case of a Broadcast connection, once this fraction of source tasks
   * have completed, all tasks on the current vertex can be scheduled. Number of
   * tasks ready for scheduling on the current vertex scales linearly between
   * min-fraction and max-fraction. Defaults to the greater of the default value
   * or tez.root-input-vertex-manager.min-src-fraction.
   */
  public static final String TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION =
      "tez.root-input-vertex-manager.max-src-fraction";
  public static final float
      TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT = 0.75f;
  
  private String configuredInputName;

  int totalNumSourceTasks = 0;
  int numSourceTasksCompleted = 0;
  private AtomicBoolean onVertexStartedDone = new AtomicBoolean(false);

  private final Map<String, SourceVertexInfo> srcVertexInfo = new
      ConcurrentHashMap<>();
  boolean sourceVerticesScheduled = false;
  List<PendingTaskInfo> pendingTasks = Lists.newLinkedList();
  int totalTasksToSchedule = 0;

  boolean slowStartEnabled = false;
  float slowStartMinFraction = 0;
  float slowStartMaxFraction = 0;

  @VisibleForTesting
  Configuration conf;

  static class PendingTaskInfo {
    final private int index;

    public PendingTaskInfo(int index) {
      this.index = index;
    }

    public String toString() {
      return "[index=" + index + "]";
    }
    public int getIndex() {
      return index;
    }
  }

  static class SourceVertexInfo {
    final EdgeProperty edgeProperty;
    boolean vertexIsConfigured;
    final BitSet finishedTaskSet;
    int numTasks;

    SourceVertexInfo(final EdgeProperty edgeProperty,
       int totalTasksToSchedule) {
      this.edgeProperty = edgeProperty;
      this.finishedTaskSet = new BitSet();
    }

    int getNumTasks() {
      return numTasks;
    }

    int getNumCompletedTasks() {
      return finishedTaskSet.cardinality();
    }
  }

  SourceVertexInfo createSourceVertexInfo(EdgeProperty edgeProperty,
      int numTasks) {
    return new SourceVertexInfo(edgeProperty, numTasks);
  }

  public RootInputVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void onVertexStarted(List<TaskAttemptIdentifier> completions) {
    Map<String, EdgeProperty> edges = getContext().
        getInputVertexEdgeProperties();
    for (Map.Entry<String, EdgeProperty> entry : edges.entrySet()) {
      String srcVertex = entry.getKey();
      //track vertices with task count > 0
      int numTasks = getContext().getVertexNumTasks(srcVertex);
      if (numTasks > 0) {
        LOG.info("Task count in " + srcVertex + ": " + numTasks);
        srcVertexInfo.put(srcVertex, createSourceVertexInfo(entry.getValue(),
            getContext().getVertexNumTasks(getContext().getVertexName())));
        getContext().registerForVertexStateUpdates(srcVertex,
            EnumSet.of(VertexState.CONFIGURED));
      } else {
        LOG.info("Vertex: " + getContext().getVertexName() + "; Ignoring "
            + srcVertex + " as it has " + numTasks + " tasks");
      }
    }
    if (completions != null) {
      for (TaskAttemptIdentifier attempt : completions) {
        onSourceTaskCompleted(attempt);
      }
    }
    onVertexStartedDone.set(true);
    // track the tasks in this vertex
    updatePendingTasks();
    processPendingTasks();
  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    Preconditions.checkArgument(stateUpdate.getVertexState() ==
        VertexState.CONFIGURED,
        "Received incorrect state notification : "
        + stateUpdate.getVertexState() + " for vertex: "
        + stateUpdate.getVertexName() + " in vertex: "
        + getContext().getVertexName());

    SourceVertexInfo vInfo = srcVertexInfo.get(stateUpdate.getVertexName());
    if(vInfo != null) {
      Preconditions.checkState(vInfo.vertexIsConfigured == false);
      vInfo.vertexIsConfigured = true;
      vInfo.numTasks = getContext().getVertexNumTasks(
          stateUpdate.getVertexName());
      totalNumSourceTasks += vInfo.numTasks;
      LOG.info("Received configured notification : {}" + " for vertex: {} in" +
          " vertex: {}" + " numjourceTasks: {}",
        stateUpdate.getVertexState(), stateUpdate.getVertexName(),
        getContext().getVertexName(), totalNumSourceTasks);
      processPendingTasks();
    }
  }

  @Override
  public void onSourceTaskCompleted(TaskAttemptIdentifier attempt) {
    String srcVertexName = attempt.getTaskIdentifier().getVertexIdentifier()
        .getName();
    int srcTaskId = attempt.getTaskIdentifier().getIdentifier();
    SourceVertexInfo srcInfo = srcVertexInfo.get(srcVertexName);
    if (srcInfo.vertexIsConfigured) {
      Preconditions.checkState(srcTaskId < srcInfo.numTasks,
          "Received completion for srcTaskId " + srcTaskId + " but Vertex: "
          + srcVertexName + " has only " + srcInfo.numTasks + " tasks");
    }
    // handle duplicate events and count task completions from
    // all source vertices
    BitSet completedSourceTasks = srcInfo.finishedTaskSet;
    // duplicate notifications tracking
    if (!completedSourceTasks.get(srcTaskId)) {
      completedSourceTasks.set(srcTaskId);
      // source task has completed
      numSourceTasksCompleted++;
    }
    processPendingTasks();
  }

  @Override
  public void initialize() {
    UserPayload userPayload = getContext().getUserPayload();
    if (userPayload == null || userPayload.getPayload() == null ||
        userPayload.getPayload().limit() == 0) {
      throw new RuntimeException("Could not initialize RootInputVertexManager"
          + " from provided user payload");
    }
    try {
      conf = TezUtils.createConfFromUserPayload(userPayload);
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize RootInputVertexManager"
        + " from provided user payload", e);
    }
    slowStartEnabled = conf.getBoolean(
      TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START,
      TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START_DEFAULT);

    if(slowStartEnabled) {
      slowStartMinFraction = conf.getFloat(
        TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION,
        TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
      slowStartMaxFraction = conf.getFloat(
        TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION,
        Math.max(slowStartMinFraction,
            TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT));
    } else {
      slowStartMinFraction = 0;
      slowStartMaxFraction = 0;
    }
    if (slowStartMinFraction < 0 || slowStartMaxFraction > 1
        || slowStartMaxFraction < slowStartMinFraction) {
      throw new IllegalArgumentException(
          "Invalid values for slowStartMinFraction"
          + "/slowStartMaxFraction. Min "
          + "cannot be < 0, max cannot be > 1, and max cannot be < min."
          + ", configuredMin=" + slowStartMinFraction
          + ", configuredMax=" + slowStartMaxFraction);
    }


    updatePendingTasks();
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
      List<Event> events) {
    List<InputDataInformationEvent> riEvents = Lists.newLinkedList();
    boolean dataInformationEventSeen = false;
    for (Event event : events) {
      if (event instanceof InputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        Preconditions.checkState(getContext().getVertexNumTasks(getContext().getVertexName()) == -1,
            "Parallelism for the vertex should be set to -1 if the InputInitializer is setting parallelism"
                + ", VertexName: " + getContext().getVertexName());
        Preconditions.checkState(configuredInputName == null,
            "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"
                + ", VertexName: " + getContext().getVertexName() + ", ConfiguredInput: "
                + configuredInputName + ", CurrentInput: " + inputName);
        configuredInputName = inputName;
        InputConfigureVertexTasksEvent cEvent = (InputConfigureVertexTasksEvent) event;
        Map<String, InputSpecUpdate> rootInputSpecUpdate = new HashMap<String, InputSpecUpdate>();
        rootInputSpecUpdate.put(
            inputName,
            cEvent.getInputSpecUpdate() == null ? InputSpecUpdate
                .getDefaultSinglePhysicalInputSpecUpdate() : cEvent.getInputSpecUpdate());
        getContext().reconfigureVertex(rootInputSpecUpdate, cEvent.getLocationHint(),
              cEvent.getNumTasks());
      }
      if (event instanceof InputUpdatePayloadEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        inputDescriptor.setUserPayload(UserPayload.create(
            ((InputUpdatePayloadEvent) event).getUserPayload()));
      } else if (event instanceof InputDataInformationEvent) {
        dataInformationEventSeen = true;
        // # Tasks should have been set by this point.
        Preconditions.checkState(getContext().getVertexNumTasks(getContext().getVertexName()) != 0);
        Preconditions.checkState(
            configuredInputName == null || configuredInputName.equals(inputName),
            "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"
                + ", VertexName:" + getContext().getVertexName() + ", ConfiguredInput: "
                + configuredInputName + ", CurrentInput: " + inputName);
        configuredInputName = inputName;
        
        InputDataInformationEvent rEvent = (InputDataInformationEvent)event;
        rEvent.setTargetIndex(rEvent.getSourceIndex()); // 1:1 routing
        riEvents.add(rEvent);
      }
    }
    getContext().addRootInputEvents(inputName, riEvents);
  }

  private boolean canScheduleTasks() {
    // check for source vertices completely configured
    for (Map.Entry<String, SourceVertexInfo> entry : srcVertexInfo.entrySet()) {
      if (!entry.getValue().vertexIsConfigured) {
        // vertex not configured
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for vertex: " + entry.getKey() + " in vertex: "
              + getContext().getVertexName());
        }
        return false;
      }
    }

    sourceVerticesScheduled = true;
    return sourceVerticesScheduled;
  }

  private boolean preconditionsSatisfied() {
    if (!onVertexStartedDone.get()) {
      // vertex not started yet
      return false;
    }

    if (!sourceVerticesScheduled && !canScheduleTasks()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Defer scheduling tasks for vertex: {} as one task needs " +
            "to be completed per source vertex", getContext().getVertexName());
      }
      return false;
    }
    return true;
  }

  void updatePendingTasks() {
    int tasks = getContext().getVertexNumTasks(getContext().getVertexName());
    if (tasks == pendingTasks.size() || tasks <= 0) {
      return;
    }
    pendingTasks.clear();
    for (int i = 0; i < tasks; ++i) {
      pendingTasks.add(new PendingTaskInfo(i));
    }
    totalTasksToSchedule = pendingTasks.size();
  }

  private void processPendingTasks() {
    if (!preconditionsSatisfied()) {
      return;
    }
    schedulePendingTasks();
  }

  private void schedulePendingTasks() {
    List<VertexManagerPluginContext.ScheduleTaskRequest> scheduledTasks =
        getTasksToSchedule();
    if (scheduledTasks != null && scheduledTasks.size() > 0) {
      getContext().scheduleTasks(scheduledTasks);
    }
  }

  float getMinSourceVertexCompletedTaskFraction() {
    float minSourceVertexCompletedTaskFraction = 1f;

    if (numSourceTasksCompleted != totalNumSourceTasks) {
      for (Map.Entry<String, SourceVertexInfo> vInfo :
          srcVertexInfo.entrySet()) {
        SourceVertexInfo srcInfo = vInfo.getValue();
        // canScheduleTasks check has already verified all
        // sources are configured
        Preconditions.checkState(srcInfo.vertexIsConfigured,
            "Vertex: " + vInfo.getKey());
        if (srcInfo.numTasks > 0) {
          int numCompletedTasks = srcInfo.getNumCompletedTasks();
          float completedFraction =
              (float) numCompletedTasks / srcInfo.numTasks;
          if (minSourceVertexCompletedTaskFraction > completedFraction) {
            minSourceVertexCompletedTaskFraction = completedFraction;
          }
        }
      }
    }
    return minSourceVertexCompletedTaskFraction;
  }

  List<VertexManagerPluginContext.ScheduleTaskRequest> getTasksToSchedule() {
    float minSourceVertexCompletedTaskFraction =
        getMinSourceVertexCompletedTaskFraction();
    int numTasksToSchedule = getNumOfTasksToScheduleAndLog(
        minSourceVertexCompletedTaskFraction);
    if (numTasksToSchedule > 0) {
      List<VertexManagerPluginContext.ScheduleTaskRequest> tasksToSchedule =
          Lists.newArrayListWithCapacity(numTasksToSchedule);

      while (!pendingTasks.isEmpty() && numTasksToSchedule > 0) {
        numTasksToSchedule--;
        Integer taskIndex = pendingTasks.get(0).getIndex();
        tasksToSchedule.add(VertexManagerPluginContext.ScheduleTaskRequest
            .create(taskIndex, null));
        pendingTasks.remove(0);
      }
      return tasksToSchedule;
    }
    return null;
  }

  int getNumOfTasksToScheduleAndLog(float minFraction) {
    int numTasksToSchedule = getNumOfTasksToSchedule(minFraction);
    if (numTasksToSchedule > 0) {
      // numTasksToSchedule can be -ve if minFraction
      // is less than slowStartMinSrcCompletionFraction.
      LOG.info("Scheduling {} tasks for vertex: {} with totalTasks: {}. " +
          "{} source tasks completed out of {}. " +
          "MinSourceTaskCompletedFraction: {} min: {} max: {}",
          numTasksToSchedule, getContext().getVertexName(),
          totalTasksToSchedule, numSourceTasksCompleted,
          totalNumSourceTasks, minFraction,
          slowStartMinFraction, slowStartMaxFraction);
    }
    return numTasksToSchedule;
  }

  int getNumOfTasksToSchedule(float minSourceVertexCompletedTaskFraction) {
    int numPendingTasks = pendingTasks.size();
    if (numSourceTasksCompleted == totalNumSourceTasks) {
      LOG.info("All source tasks completed. Ramping up {} remaining tasks" +
          " for vertex: {}", numPendingTasks, getContext().getVertexName());
      return numPendingTasks;
    }

    // start scheduling when source tasks completed fraction is more than min.
    // linearly increase the number of scheduled tasks such that all tasks are
    // scheduled when source tasks completed fraction reaches max
    float tasksFractionToSchedule = 1;
    float percentRange =
        slowStartMaxFraction - slowStartMinFraction;
    if (percentRange > 0) {
      tasksFractionToSchedule =
          (minSourceVertexCompletedTaskFraction -
              slowStartMinFraction) / percentRange;
    } else {
      // min and max are equal. schedule 100% on reaching min
      if(minSourceVertexCompletedTaskFraction <
          slowStartMinFraction) {
        tasksFractionToSchedule = 0;
      }
    }

    tasksFractionToSchedule =
        Math.max(0, Math.min(1, tasksFractionToSchedule));

    // round up to avoid the corner case that single task cannot be scheduled
    // until src completed fraction reach max
    return ((int)(Math.ceil(tasksFractionToSchedule * totalTasksToSchedule)) -
        (totalTasksToSchedule - numPendingTasks));
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
   * @return {@link RootInputVertexManagerConfigBuilder}
   */
  public static RootInputVertexManagerConfigBuilder createConfigBuilder(
      @Nullable Configuration conf) {
    return new RootInputVertexManagerConfigBuilder(conf);
  }

  /**
   * Helper class to configure RootInputVertexManager
   */
  public static final class RootInputVertexManagerConfigBuilder {
    private final Configuration conf;

    private RootInputVertexManagerConfigBuilder(@Nullable Configuration conf) {
      if (conf == null) {
        this.conf = new Configuration(false);
      } else {
        this.conf = conf;
      }
    }

    public RootInputVertexManagerConfigBuilder setSlowStart(
        boolean enabled) {
      conf.setBoolean(TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START,
          enabled);
      return this;
    }

    public RootInputVertexManagerConfigBuilder
        setSlowStartMinSrcCompletionFraction(float minFraction) {
      conf.setFloat(TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION,
          minFraction);
      return this;
    }

    public RootInputVertexManagerConfigBuilder
        setSlowStartMaxSrcCompletionFraction(float maxFraction) {
      conf.setFloat(TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION,
          maxFraction);
      return this;
    }

    public VertexManagerPluginDescriptor build() {
      VertexManagerPluginDescriptor desc =
          VertexManagerPluginDescriptor.create(
              RootInputVertexManager.class.getName());

      try {
        return desc.setUserPayload(TezUtils
            .createUserPayloadFromConf(this.conf));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
  }

}
