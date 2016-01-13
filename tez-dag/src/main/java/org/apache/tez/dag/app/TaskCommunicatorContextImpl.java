/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.serviceplugins.api.TaskHeartbeatRequest;
import org.apache.tez.serviceplugins.api.TaskHeartbeatResponse;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexStateUpdateListener;
import org.apache.tez.dag.records.TezTaskAttemptID;

@InterfaceAudience.Private
public class TaskCommunicatorContextImpl implements TaskCommunicatorContext, VertexStateUpdateListener {

  // TODO TEZ-2003 (post) TEZ-2669 Propagate errors baack to the AM with proper error reporting

  private final AppContext context;
  private final TaskCommunicatorManager taskCommunicatorManager;
  private final int taskCommunicatorIndex;
  private final ReentrantReadWriteLock.ReadLock dagChangedReadLock;
  private final ReentrantReadWriteLock.WriteLock dagChangedWriteLock;
  private final UserPayload userPayload;

  private DAG dag;

  public TaskCommunicatorContextImpl(AppContext appContext,
                                     TaskCommunicatorManager taskCommunicatorManager,
                                     UserPayload userPayload,
                                     int taskCommunicatorIndex) {
    this.context = appContext;
    this.taskCommunicatorManager = taskCommunicatorManager;
    this.userPayload = userPayload;
    this.taskCommunicatorIndex = taskCommunicatorIndex;

    ReentrantReadWriteLock dagChangedLock = new ReentrantReadWriteLock();
    dagChangedReadLock = dagChangedLock.readLock();
    dagChangedWriteLock = dagChangedLock.writeLock();
  }

  @Override
  public UserPayload getInitialUserPayload() {
    return userPayload;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return context.getApplicationAttemptId();
  }

  @Override
  public Credentials getCredentials() {
    return context.getAppCredentials();
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException {
    return taskCommunicatorManager.canCommit(taskAttemptId);
  }

  @Override
  public TaskHeartbeatResponse heartbeat(TaskHeartbeatRequest request) throws IOException,
      TezException {
    return taskCommunicatorManager.heartbeat(request);
  }

  @Override
  public boolean isKnownContainer(ContainerId containerId) {
    AMContainer amContainer = context.getAllContainers().get(containerId);
    if (amContainer == null ||
        amContainer.getTaskCommunicatorIdentifier() != taskCommunicatorIndex) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public void taskAlive(TezTaskAttemptID taskAttemptId) {
    taskCommunicatorManager.taskAlive(taskAttemptId);
  }

  @Override
  public void containerAlive(ContainerId containerId) {
    if (isKnownContainer(containerId)) {
      taskCommunicatorManager.containerAlive(containerId);
    }
  }

  @Override
  public void taskStartedRemotely(TezTaskAttemptID taskAttemptId, ContainerId containerId) {
    taskCommunicatorManager.taskStartedRemotely(taskAttemptId, containerId);
  }

  @Override
  public void taskKilled(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         @Nullable String diagnostics) {
    taskCommunicatorManager.taskKilled(taskAttemptId, taskAttemptEndReason, diagnostics);
  }

  @Override
  public void taskFailed(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         @Nullable String diagnostics) {
    taskCommunicatorManager.taskFailed(taskAttemptId, taskAttemptEndReason, diagnostics);

  }

  @Override
  public void registerForVertexStateUpdates(String vertexName,
                                            @Nullable Set<VertexState> stateSet) {
    Preconditions.checkNotNull(vertexName, "VertexName cannot be null: " + vertexName);
    getDag().getStateChangeNotifier().registerForVertexUpdates(vertexName, stateSet,
        this);
  }

  @Override
  public String getCurrentDagName() {
    return getDag().getName();
  }

  @Override
  public String getCurrentAppIdentifier() {
    return context.getApplicationID().toString();
  }

  @Override
  public int getCurrentDagIdenitifer() {
    return getDag().getID().getId();
  }

  @Override
  public Iterable<String> getInputVertexNames(String vertexName) {
    Preconditions.checkNotNull(vertexName, "VertexName cannot be null: " + vertexName);
    Vertex vertex = getDag().getVertex(vertexName);
    Set<Vertex> sources = vertex.getInputVertices().keySet();
    return Iterables.transform(sources, new Function<Vertex, String>() {
      @Override
      public String apply(Vertex input) {
        return input.getName();
      }
    });
  }

  @Override
  public int getVertexTotalTaskCount(String vertexName) {
    Preconditions.checkArgument(vertexName != null, "VertexName must be specified");
    return getDag().getVertex(vertexName).getTotalTasks();
  }

  @Override
  public int getVertexCompletedTaskCount(String vertexName) {
    Preconditions.checkArgument(vertexName != null, "VertexName must be specified");
    return getDag().getVertex(vertexName).getCompletedTasks();
  }

  @Override
  public int getVertexRunningTaskCount(String vertexName) {
    Preconditions.checkArgument(vertexName != null, "VertexName must be specified");
    return getDag().getVertex(vertexName).getRunningTasks();
  }

  @Override
  public long getFirstAttemptStartTime(String vertexName, int taskIndex) {
    Preconditions.checkArgument(vertexName != null, "VertexName must be specified");
    Preconditions.checkArgument(taskIndex >=0, "TaskIndex must be > 0");
    return getDag().getVertex(vertexName).getTask(
        taskIndex).getFirstAttemptStartTime();
  }

  @Override
  public long getDagStartTime() {
    return getDag().getStartTime();
  }

  @Override
  public void onStateUpdated(VertexStateUpdate event) {
    taskCommunicatorManager.vertexStateUpdateNotificationReceived(event, taskCommunicatorIndex);
  }

  private DAG getDag() {
    dagChangedReadLock.lock();
    try {
      if (dag != null) {
        return dag;
      } else {
        return context.getCurrentDAG();
      }
    } finally {
      dagChangedReadLock.unlock();
    }
  }

  @InterfaceAudience.Private
  public void dagCompleteStart(DAG dag) {
    dagChangedWriteLock.lock();
    try {
      this.dag = dag;
    } finally {
      dagChangedWriteLock.unlock();
    }
  }

  public void dagCompleteEnd() {
    dagChangedWriteLock.lock();
    try {
      this.dag = null;
    } finally {
      dagChangedWriteLock.unlock();
    }
  }
}
