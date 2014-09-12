/*
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

package org.apache.tez.dag.app.dag;


import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

/**
 * Tracks status updates from various components, and informs registered components about updates.
 */
@InterfaceAudience.Private
public class StateChangeNotifier {

  private final DAG dag;
  private final SetMultimap<TezVertexID, ListenerContainer> vertexListeners;
  private final ListMultimap<TezVertexID, VertexStateUpdate> lastKnowStatesMap;
  private final ReentrantReadWriteLock listenersLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = listenersLock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = listenersLock.writeLock();

  public StateChangeNotifier(DAG dag) {
    this.dag = dag;
    this.vertexListeners = Multimaps.synchronizedSetMultimap(
        HashMultimap.<TezVertexID, ListenerContainer>create());
    this.lastKnowStatesMap = LinkedListMultimap.create();
  }

  // -------------- VERTEX STATE CHANGE SECTION ---------------
  public void registerForVertexUpdates(String vertexName,
                                       Set<org.apache.tez.dag.api.event.VertexState> stateSet,
                                       VertexStateUpdateListener listener) {
    TezVertexID vertexId = validateAndGetVertexId(vertexName);
    writeLock.lock();
    // Read within the lock, to ensure a consistent view is seen.
    List<VertexStateUpdate> previousUpdates = lastKnowStatesMap.get(vertexId);
    try {
      ListenerContainer listenerContainer = new ListenerContainer(listener, stateSet);
      Set<ListenerContainer> listenerContainers = vertexListeners.get(vertexId);
      if (listenerContainers == null || !listenerContainers.contains(listenerContainer)) {
        vertexListeners.put(vertexId, listenerContainer);
        // Send the last known state immediately, if it isn't null.
        // Sent from within the lock to avoid duplicate events, and out of order events.
        if (previousUpdates != null && !previousUpdates.isEmpty()) {
          for (VertexStateUpdate update : previousUpdates) {
            listenerContainer.sendStateUpdate(update);
          }
        }
      } else {
        // Disallow multiple register calls.
        throw new TezUncheckedException(
            "Only allowed to register once for a listener. CurrentContext: vertexName=" +
                vertexName + ", Listener: " + listener);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void unregisterForVertexUpdates(String vertexName, VertexStateUpdateListener listener) {
    TezVertexID vertexId = validateAndGetVertexId(vertexName);
    writeLock.lock();
    try {
      ListenerContainer listenerContainer = new ListenerContainer(listener, null);
      vertexListeners.remove(vertexId, listenerContainer);
    } finally {
      writeLock.unlock();
    }
  }

  public void stateChanged(TezVertexID vertexId, VertexStateUpdate vertexStateUpdate) {
    readLock.lock();
    try {
      lastKnowStatesMap.put(vertexId, vertexStateUpdate);
      if (vertexListeners.containsKey(vertexId)) {
        sendStateUpdate(vertexId, vertexStateUpdate);
      }
    } finally {
      readLock.unlock();
    }
  }

  private void sendStateUpdate(TezVertexID vertexId,
                               VertexStateUpdate event) {
    for (ListenerContainer listenerContainer : vertexListeners.get(vertexId)) {
      listenerContainer.sendStateUpdate(event);
    }

  }


  private static final class ListenerContainer {
    final VertexStateUpdateListener listener;
    final Set<org.apache.tez.dag.api.event.VertexState> states;

    private ListenerContainer(VertexStateUpdateListener listener,
                              Set<org.apache.tez.dag.api.event.VertexState> states) {
      this.listener = listener;
      if (states == null) {
        this.states = EnumSet.allOf(org.apache.tez.dag.api.event.VertexState.class);
      } else {
        this.states = states;
      }
    }

    private void sendStateUpdate(VertexStateUpdate stateUpdate) {
      if (states.contains(stateUpdate.getVertexState())) {
        listener.onStateUpdated(stateUpdate);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ListenerContainer that = (ListenerContainer) o;

      // Explicit reference comparison
      return listener == that.listener;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(listener);
    }
  }

  // -------------- END OF VERTEX STATE CHANGE SECTION ---------------

  // -------------- TASK STATE CHANGE SECTION ---------------

  // Task updates are not buffered to avoid storing unnecessary information.
  // Components (non user facing) which use this will receive notifications after registration.
  // They will have to query task states, prior to registration.
  // Currently only handling Task SUCCESS events.
  private final SetMultimap<TezVertexID, TaskStateUpdateListener> taskListeners =
      Multimaps.synchronizedSetMultimap(HashMultimap.<TezVertexID, TaskStateUpdateListener>create());
  private final ReentrantReadWriteLock taskListenerLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock taskReadLock = taskListenerLock.readLock();
  private final ReentrantReadWriteLock.WriteLock taskWriteLock = taskListenerLock.writeLock();



  public void registerForTaskSuccessUpdates(String vertexName, TaskStateUpdateListener listener) {
    TezVertexID vertexId = validateAndGetVertexId(vertexName);
    Preconditions.checkNotNull(listener, "listener cannot be null");
    taskWriteLock.lock();
    try {
      taskListeners.put(vertexId, listener);
    } finally {
      taskWriteLock.unlock();
    }
  }

  public void unregisterForTaskSuccessUpdates(String vertexName, TaskStateUpdateListener listener) {
    TezVertexID vertexId = validateAndGetVertexId(vertexName);
    Preconditions.checkNotNull(listener, "listener cannot be null");
    taskWriteLock.lock();
    try {
      taskListeners.remove(vertexId, listener);
    } finally {
      taskWriteLock.unlock();
    }
  }

  public void taskSucceeded(String vertexName, TezTaskID taskId, int attemptId) {
    taskReadLock.lock();
    try {
      for (TaskStateUpdateListener listener : taskListeners.get(taskId.getVertexID())) {
        listener.onTaskSucceeded(vertexName, taskId, attemptId);
      }
    } finally {
      taskReadLock.unlock();
    }
  }

  // -------------- END OF TASK STATE CHANGE SECTION ---------------


  private TezVertexID validateAndGetVertexId(String vertexName) {
    Preconditions.checkNotNull(vertexName, "VertexName cannot be null");
    Vertex vertex = dag.getVertex(vertexName);
    Preconditions.checkNotNull(vertex, "Vertex does not exist: " + vertexName);
    return vertex.getVertexId();
  }

}
