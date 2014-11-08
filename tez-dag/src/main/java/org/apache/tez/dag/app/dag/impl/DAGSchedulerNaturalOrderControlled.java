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

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.records.TezTaskAttemptID;

/**
 * Schedules task attempts belonging to downstream vertices only after all attempts belonging to
 * upstream vertices have been scheduled. If there's a slow start or delayed start of a particular
 * vertex, this ensures that downstream tasks are not started before this</p>
 * Some future enhancements
 * - consider cluster capacity - and be more aggressive about scheduling downstream tasks before
 * upstream tasks have completed. </p>
 * - generic slow start mechanism across all vertices - independent of the type of edges.
 */
@SuppressWarnings("rawtypes")
public class DAGSchedulerNaturalOrderControlled implements DAGScheduler {

  private static final Log LOG =
      LogFactory.getLog(DAGSchedulerNaturalOrderControlled.class);

  private final DAG dag;
  private final EventHandler handler;

  // Tracks pending events, in case they're not sent immediately.
  private final ListMultimap<String, TaskAttemptEventSchedule> pendingEvents =
      LinkedListMultimap.create();
  // Tacks vertices for which no additional scheduling checks are required. Once in this list, the
  // vertex is considered to be fully scheduled.
  private final Set<String> scheduledVertices = new HashSet<String>();
  // Tracks tasks scheduled for a vertex.
  private final Map<String, BitSet> vertexScheduledTasks = new HashMap<String, BitSet>();

  public DAGSchedulerNaturalOrderControlled(DAG dag, EventHandler dispatcher) {
    this.dag = dag;
    this.handler = dispatcher;
  }

  @Override
  public void vertexCompleted(Vertex vertex) {
  }

  // TODO Does ordering matter - it currently depends on the order returned by vertex.getOutput*
  @Override
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();

    // natural priority. Handles failures and retries.
    int priorityLowLimit = (vertexDistanceFromRoot + 1) * 3;
    int priorityHighLimit = priorityLowLimit - 2;

    TaskAttemptEventSchedule attemptEvent = new TaskAttemptEventSchedule(
        attempt.getID(), priorityLowLimit, priorityHighLimit);

    taskAttemptSeen(vertex.getName(), attempt.getID());

    if (vertexAlreadyScheduled(vertex)) {
      // Vertex previously marked ready for scheduling.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scheduling " + attempt.getID() + " between priorityLow: " + priorityLowLimit
            + " and priorityHigh: " + priorityHighLimit);
      }
      sendEvent(attemptEvent);
      // A new taks coming in here could send us over the enough tasks scheduled limit.
      processDownstreamVertices(vertex);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Attempting to schedule vertex: " + vertex.getLogIdentifier() +
            " due to schedule event");
      }
      boolean scheduled = trySchedulingVertex(vertex);
      if (scheduled) {
        LOG.info("Scheduled vertex: " + vertex.getLogIdentifier());
        // If ready to be scheduled, send out pending events and the current event.
        // Send events out for this vertex first. Then try scheduling downstream vertices.
        sendEventsForVertex(vertex.getName());
        sendEvent(attemptEvent);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Processing downstream vertices for vertex: " + vertex.getLogIdentifier());
        }
        processDownstreamVertices(vertex);
      } else {
        pendingEvents.put(vertex.getName(), attemptEvent);
      }
    }
  }

  private void taskAttemptSeen(String vertexName, TezTaskAttemptID taskAttemptID) {
    BitSet scheduledTasks = vertexScheduledTasks.get(vertexName);
    if (scheduledTasks == null) {
      scheduledTasks = new BitSet();
      vertexScheduledTasks.put(vertexName, scheduledTasks);
    }
    if (taskAttemptID != null) { // null for 0 task vertices
      scheduledTasks.set(taskAttemptID.getTaskID().getId());
    }
  }

  private void sendEventsForVertex(String vertexName) {
    for (TaskAttemptEventSchedule event : pendingEvents.removeAll(vertexName)) {
      sendEvent(event);
    }
  }

  /* Checks whether this vertex has been marked as ready to go in the past */
  private boolean vertexAlreadyScheduled(Vertex vertex) {
    return scheduledVertices.contains(vertex.getName());
  }

  private boolean scheduledTasksForwarded(Vertex vertex) {
    boolean canSchedule = false;
    BitSet scheduledTasks = vertexScheduledTasks.get(vertex.getName());
    if (scheduledTasks != null) {
      if (scheduledTasks.cardinality() >= vertex.getTotalTasks()) {
        canSchedule = true;
      }
    }
    return canSchedule;
  }

  private void processDownstreamVertices(Vertex vertex) {
    List<Vertex> newlyScheduledVertices = Lists.newLinkedList();
    Map<Vertex, Edge> outputVertexEdgeMap = vertex.getOutputVertices();
    for (Vertex destVertex : outputVertexEdgeMap.keySet()) {
      if (vertexAlreadyScheduled(destVertex)) { // Nothing to do if already scheduled.
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Attempting to schedule vertex: " + destVertex.getLogIdentifier() +
              " due to upstream event from " + vertex.getLogIdentifier());
        }
        boolean scheduled = trySchedulingVertex(destVertex);
        if (scheduled) {
          LOG.info("Scheduled vertex: " + destVertex.getLogIdentifier() +
              " due to upstream event from " + vertex.getLogIdentifier());
          sendEventsForVertex(destVertex.getName());
          newlyScheduledVertices.add(destVertex);
        }
      }
    }

    // Try scheduling all downstream vertices which were scheduled in this run.
    for (Vertex downStreamVertex : newlyScheduledVertices) {
      processDownstreamVertices(downStreamVertex);
    }
  }

  /* Process the specified vertex, and add it to the cache of scheduled vertices if it can be scheduled */
  private boolean trySchedulingVertex(Vertex vertex) {
    boolean canSchedule = true;
    if (vertexScheduledTasks.get(vertex.getName()) == null) {
      // No scheduled requests seen yet. Do not mark this as ready.
      // 0 task vertices handled elsewhere.
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "No schedule requests for vertex: " + vertex.getLogIdentifier() + ", Not scheduling");
      }
      canSchedule = false;
    } else {
      Map<Vertex, Edge> inputVertexEdgeMap = vertex.getInputVertices();
      if (inputVertexEdgeMap == null || inputVertexEdgeMap.isEmpty()) {
        // Nothing to wait for. Go ahead and scheduled.
        if (LOG.isDebugEnabled()) {
          LOG.debug("No sources for vertex: " + vertex.getLogIdentifier() + ", Scheduling now");
        }
      } else {
        // Check if all sources are scheduled.
        for (Vertex srcVertex : inputVertexEdgeMap.keySet()) {
          if (scheduledTasksForwarded(srcVertex)) {
            // Nothing to wait for. Go ahead and check the next source.
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to schedule: " + vertex.getLogIdentifier() +
                  ", All tasks forwarded for srcVertex: " + srcVertex.getLogIdentifier() +
                  ", count: " + srcVertex.getTotalTasks());
            }
          } else {
            // Special case for vertices with 0 tasks. 0 check is sufficient since parallelism cannot increase.
            if (srcVertex.getTotalTasks() == 0) {
              LOG.info(
                  "Vertex: " + srcVertex.getLogIdentifier() + " has 0 tasks. Marking as scheduled");
              scheduledVertices.add(srcVertex.getName());
              taskAttemptSeen(srcVertex.getName(), null);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Not all sources schedule requests complete while trying to schedule: " +
                        vertex.getLogIdentifier() + ", For source vertex: " +
                        srcVertex.getLogIdentifier() + ", Forwarded requests: " +
                        (vertexScheduledTasks.get(srcVertex.getName()) == null ? "null(0)" :
                            vertexScheduledTasks.get(srcVertex.getName()).cardinality()) +
                        " out of " + srcVertex.getTotalTasks());
              }
              canSchedule = false;
              break;
            }
          }
        }
      }
    }
    if (canSchedule) {
      scheduledVertices.add(vertex.getName());
    }
    return canSchedule;
  }

  @Override
  public void taskScheduled(DAGEventSchedulerUpdateTAAssigned event) {
  }

  @Override
  public void taskSucceeded(DAGEventSchedulerUpdate event) {
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(TaskAttemptEventSchedule event) {
    handler.handle(event);
  }

}
