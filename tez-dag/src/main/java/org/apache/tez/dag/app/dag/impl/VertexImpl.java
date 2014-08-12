/* Licensed to the Apache Software Foundation (ASF) under one
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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatus.State;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.RootInputInitializerManager;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TaskTerminationCause;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGEventVertexReRunning;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventAddTezEvent;
import org.apache.tez.dag.app.dag.event.TaskEventRecoverTask;
import org.apache.tez.dag.app.dag.event.TaskEventTermination;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventNullEdgeInitialized;
import org.apache.tez.dag.app.dag.event.VertexEventOneToOneSourceSplit;
import org.apache.tez.dag.app.dag.event.VertexEventRecoverVertex;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputInitialized;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventSourceTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventSourceVertexRecovered;
import org.apache.tez.dag.app.dag.event.VertexEventSourceVertexStarted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventTermination;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl.VertexGroupInfo;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexDataMovementEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexParallelismUpdatedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TaskSpecificLaunchCmdOption;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

/** Implementation of Vertex interface. Maintains the state machines of Vertex.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class VertexImpl implements org.apache.tez.dag.app.dag.Vertex,
  EventHandler<VertexEvent> {

  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  private static final Log LOG = LogFactory.getLog(VertexImpl.class);

  //final fields
  private final Clock clock;


  private final Lock readLock;
  private final Lock writeLock;
  private final TaskAttemptListener taskAttemptListener;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private final Object tasksSyncHandle = new Object();

  private final EventHandler eventHandler;
  // TODO Metrics
  //private final MRAppMetrics metrics;
  private final AppContext appContext;

  private boolean lazyTasksCopyNeeded = false;
  // must be a linked map for ordering
  volatile LinkedHashMap<TezTaskID, Task> tasks = new LinkedHashMap<TezTaskID, Task>();
  private Object fullCountersLock = new Object();
  private TezCounters fullCounters = null;
  private Resource taskResource;

  private Configuration conf;

  //fields initialized in init

  private int numStartedSourceVertices = 0;
  private int numInitedSourceVertices = 0;
  private int numRecoveredSourceVertices = 0;

  private int distanceFromRoot = 0;

  private final List<String> diagnostics = new ArrayList<String>();
  
  //task/attempt related datastructures
  @VisibleForTesting
  int numSuccessSourceAttemptCompletions = 0;

  List<InputSpec> inputSpecList;
  List<OutputSpec> outputSpecList;
  List<GroupInputSpec> groupInputSpecList;
  Set<String> sharedOutputs = Sets.newHashSet();

  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final RouteEventTransition
      ROUTE_EVENT_TRANSITION = new RouteEventTransition();
  private static final TaskAttemptCompletedEventTransition
      TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new TaskAttemptCompletedEventTransition();
  private static final SourceTaskAttemptCompletedEventTransition
      SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new SourceTaskAttemptCompletedEventTransition();

  private VertexState recoveredState = VertexState.NEW;
  private List<TezEvent> recoveredEvents = new ArrayList<TezEvent>();
  private boolean vertexAlreadyInitialized = false;

  protected static final
    StateMachineFactory<VertexImpl, VertexState, VertexEventType, VertexEvent>
       stateMachineFactory
     = new StateMachineFactory<VertexImpl, VertexState, VertexEventType, VertexEvent>
              (VertexState.NEW)

          // Transitions from NEW state
          .addTransition
              (VertexState.NEW,
                  EnumSet.of(VertexState.NEW, VertexState.INITED,
                      VertexState.INITIALIZING, VertexState.FAILED),
                  VertexEventType.V_INIT,
                  new InitTransition())
          .addTransition(VertexState.NEW, 
                EnumSet.of(VertexState.NEW),
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  new NullEdgeInitializedTransition())
          .addTransition
              (VertexState.NEW,
                  EnumSet.of(VertexState.NEW, VertexState.INITED,
                      VertexState.INITIALIZING, VertexState.RUNNING,
                      VertexState.SUCCEEDED, VertexState.FAILED,
                      VertexState.KILLED, VertexState.ERROR,
                      VertexState.RECOVERING),
                  VertexEventType.V_RECOVER,
                  new StartRecoverTransition())
          .addTransition
              (VertexState.NEW,
                  EnumSet.of(VertexState.INITED,
                      VertexState.INITIALIZING, VertexState.RUNNING,
                      VertexState.SUCCEEDED, VertexState.FAILED,
                      VertexState.KILLED, VertexState.ERROR,
                      VertexState.RECOVERING),
                  VertexEventType.V_SOURCE_VERTEX_RECOVERED,
                  new RecoverTransition())
          .addTransition(VertexState.NEW, VertexState.NEW,
              VertexEventType.V_SOURCE_VERTEX_STARTED,
              new SourceVertexStartedTransition())
          .addTransition(VertexState.NEW, VertexState.KILLED,
              VertexEventType.V_TERMINATE,
              new TerminateNewVertexTransition())
          .addTransition(VertexState.NEW, VertexState.ERROR,
              VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition
              (VertexState.RECOVERING,
                  EnumSet.of(VertexState.NEW, VertexState.INITED,
                      VertexState.INITIALIZING, VertexState.RUNNING,
                      VertexState.SUCCEEDED, VertexState.FAILED,
                      VertexState.KILLED, VertexState.ERROR,
                      VertexState.RECOVERING),
                  VertexEventType.V_SOURCE_VERTEX_RECOVERED,
                  new RecoverTransition())
          .addTransition
              (VertexState.RECOVERING, VertexState.RECOVERING,
                  EnumSet.of(VertexEventType.V_INIT,
                      VertexEventType.V_ROUTE_EVENT,
                      VertexEventType.V_SOURCE_VERTEX_STARTED,
                      VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED),
                  new BufferDataRecoverTransition())
          .addTransition
              (VertexState.RECOVERING, VertexState.RECOVERING,
                  VertexEventType.V_TERMINATE,
                  new TerminateDuringRecoverTransition())

          // Transitions from INITIALIZING state
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITIALIZING, VertexState.INITED, 
                  VertexState.FAILED),
              VertexEventType.V_ROOT_INPUT_INITIALIZED,
              new RootInputInitializedTransition())
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITIALIZING),
              VertexEventType.V_ONE_TO_ONE_SOURCE_SPLIT,
              new OneToOneSourceSplitTransition())
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITED, VertexState.FAILED),
              VertexEventType.V_READY_TO_INIT,
              new VertexInitializedTransition())
          .addTransition(VertexState.INITIALIZING, VertexState.FAILED,
              VertexEventType.V_ROOT_INPUT_FAILED,
              new RootInputInitFailedTransition())
          .addTransition(VertexState.INITIALIZING, VertexState.INITIALIZING,
              VertexEventType.V_START,
              new StartWhileInitializingTransition())
          .addTransition(VertexState.INITIALIZING, VertexState.INITIALIZING,
              VertexEventType.V_SOURCE_VERTEX_STARTED,
              new SourceVertexStartedTransition())
          .addTransition(VertexState.INITIALIZING,  VertexState.INITIALIZING,
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.INITIALIZING, VertexState.INITIALIZING,
              VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)
          .addTransition(VertexState.INITIALIZING, VertexState.KILLED,
              VertexEventType.V_TERMINATE,
              new TerminateInitingVertexTransition())
          .addTransition(VertexState.INITIALIZING, VertexState.ERROR,
              VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(VertexState.INITIALIZING, 
              EnumSet.of(VertexState.INITIALIZING, VertexState.INITED,
                  VertexState.FAILED),
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  new NullEdgeInitializedTransition())

          // Transitions from INITED state
          // SOURCE_VERTEX_STARTED - for sources which determine parallelism, 
          // they must complete before this vertex can start.
          .addTransition
              (VertexState.INITED,
                  EnumSet.of(VertexState.INITED, VertexState.ERROR),
                  VertexEventType.V_INIT,
                  new IgnoreInitInInitedTransition())
          .addTransition(VertexState.INITED, VertexState.INITED,
              VertexEventType.V_SOURCE_VERTEX_STARTED,
              new SourceVertexStartedTransition())
          .addTransition(VertexState.INITED, 
              EnumSet.of(VertexState.INITED),
              VertexEventType.V_ONE_TO_ONE_SOURCE_SPLIT,
              new OneToOneSourceSplitTransition())
          .addTransition(VertexState.INITED,  VertexState.INITED,
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.INITED, 
              EnumSet.of(VertexState.RUNNING, VertexState.INITED),
              VertexEventType.V_START,
              new StartTransition())
          .addTransition(VertexState.INITED,
              VertexState.INITED, VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)
          .addTransition(VertexState.INITED, VertexState.KILLED,
              VertexEventType.V_TERMINATE,
              new TerminateInitedVertexTransition())
          .addTransition(VertexState.INITED, VertexState.ERROR,
              VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition
              (VertexState.RUNNING,
              EnumSet.of(VertexState.RUNNING,
                  VertexState.SUCCEEDED, VertexState.TERMINATING, VertexState.FAILED,
                  VertexState.ERROR),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition(VertexState.RUNNING, 
              EnumSet.of(VertexState.RUNNING),
              VertexEventType.V_ONE_TO_ONE_SOURCE_SPLIT,
              new OneToOneSourceSplitTransition())
          .addTransition(VertexState.RUNNING, VertexState.TERMINATING,
              VertexEventType.V_TERMINATE,
              new VertexKilledTransition())
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_TASK_RESCHEDULED,
              new TaskRescheduledTransition())
          .addTransition(VertexState.RUNNING,
              EnumSet.of(VertexState.RUNNING, VertexState.SUCCEEDED,
                  VertexState.FAILED),
              VertexEventType.V_COMPLETED,
              new VertexNoTasksCompletedTransition())
          .addTransition(
              VertexState.RUNNING,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(
              VertexState.RUNNING,
              VertexState.RUNNING, VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)

          // Transitions from TERMINATING state.
          .addTransition
              (VertexState.TERMINATING,
              EnumSet.of(VertexState.TERMINATING, VertexState.KILLED, VertexState.FAILED),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition(
              VertexState.TERMINATING,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.TERMINATING, VertexState.TERMINATING,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_RESCHEDULED))

          // Transitions from SUCCEEDED state
          .addTransition(
              VertexState.SUCCEEDED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(VertexState.SUCCEEDED,
              EnumSet.of(VertexState.RUNNING, VertexState.FAILED),
              VertexEventType.V_TASK_RESCHEDULED,
              new TaskRescheduledAfterVertexSuccessTransition())

          // Ignore-able events
          .addTransition(
              VertexState.SUCCEEDED, VertexState.SUCCEEDED,
              // accumulate these in case we get restarted
              VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)
          .addTransition(
              VertexState.SUCCEEDED, 
              EnumSet.of(VertexState.FAILED, VertexState.ERROR),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedAfterVertexSuccessTransition())
          .addTransition(VertexState.SUCCEEDED, VertexState.SUCCEEDED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  // after we are done reruns of source tasks should not affect
                  // us. These reruns may be triggered by other consumer vertices.
                  // We should have been in RUNNING state if we had triggered the
                  // reruns.
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED))
          .addTransition(VertexState.SUCCEEDED, VertexState.SUCCEEDED,
              VertexEventType.V_TASK_ATTEMPT_COMPLETED,
              new TaskAttemptCompletedEventTransition())

          // Transitions from FAILED state
          .addTransition(
              VertexState.FAILED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.FAILED, VertexState.FAILED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_START,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_ONE_TO_ONE_SOURCE_SPLIT,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_ROOT_INPUT_FAILED,
                  VertexEventType.V_SOURCE_VERTEX_RECOVERED))

          // Transitions from KILLED state
          .addTransition(
              VertexState.KILLED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.KILLED, VertexState.KILLED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_INIT,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_START,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_ONE_TO_ONE_SOURCE_SPLIT,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_ROOT_INPUT_FAILED,
                  VertexEventType.V_SOURCE_VERTEX_RECOVERED))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              VertexState.ERROR,
              VertexState.ERROR,
              EnumSet.of(VertexEventType.V_INIT,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_START,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_TERMINATE,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_ONE_TO_ONE_SOURCE_SPLIT,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_INTERNAL_ERROR,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_ROOT_INPUT_FAILED,
                  VertexEventType.V_SOURCE_VERTEX_RECOVERED))
          // create the topology tables
          .installTopology();

  private final StateMachine<VertexState, VertexEventType, VertexEvent>
      stateMachine;

  //changing fields while the vertex is running
  private int numTasks;
  private int completedTaskCount = 0;
  private int succeededTaskCount = 0;
  private int failedTaskCount = 0;
  private int killedTaskCount = 0;

  private long initTimeRequested; // Time at which INIT request was received.
  private long initedTime; // Time when entering state INITED
  private long startTimeRequested; // Time at which START request was received.
  private long startedTime; // Time when entering state STARTED
  private long finishTime;
  private float progress;

  private final TezVertexID vertexId;  //runtime assigned id.
  private final VertexPlan vertexPlan;
  private boolean initWaitsForRootInitializers = false;

  private final String vertexName;
  private final ProcessorDescriptor processorDescriptor;

  @VisibleForTesting
  Map<Vertex, Edge> sourceVertices;
  private Map<Vertex, Edge> targetVertices;
  Set<Edge> uninitializedEdges = Sets.newHashSet();

  private Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
    rootInputDescriptors;
  private Map<String, RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> 
    additionalOutputs;
  private Map<String, OutputCommitter> outputCommitters;
  private Map<String, InputSpecUpdate> rootInputSpecs = new HashMap<String, InputSpecUpdate>();
  private static final InputSpecUpdate DEFAULT_ROOT_INPUT_SPECS = InputSpecUpdate
      .getDefaultSinglePhysicalInputSpecUpdate(); 
  private final List<OutputSpec> additionalOutputSpecs = new ArrayList<OutputSpec>();
  private Set<String> inputsWithInitializers;
  private int numInitializedInputs;
  private boolean startSignalPending = false;
  private boolean tasksNotYetScheduled = true;
  // We may always store task events in the vertex for scalability
  List<TezEvent> pendingTaskEvents = Lists.newLinkedList();
  List<TezEvent> pendingRouteEvents = new LinkedList<TezEvent>();
  List<TezTaskAttemptID> pendingReportedSrcCompletions = Lists.newLinkedList();


  private RootInputInitializerManager rootInputInitializerManager;

  VertexManager vertexManager;
  
  private final UserGroupInformation dagUgi;

  private boolean parallelismSet = false;
  private TezVertexID originalOneToOneSplitSource = null;

  private AtomicBoolean committed = new AtomicBoolean(false);
  private AtomicBoolean aborted = new AtomicBoolean(false);
  private boolean commitVertexOutputs = false;
  
  private Map<String, VertexGroupInfo> dagVertexGroups;
  
  private TaskLocationHint taskLocationHints[];
  private Map<String, LocalResource> localResources;
  private Map<String, String> environment;
  private final String javaOpts;
  private final ContainerContext containerContext;
  private VertexTerminationCause terminationCause;
  
  private String logIdentifier;
  private boolean recoveryCommitInProgress = false;
  private boolean summaryCompleteSeen = false;
  private boolean hasCommitter = false;
  private boolean vertexCompleteSeen = false;
  private Map<String,EdgeManagerPluginDescriptor> recoveredSourceEdgeManagers = null;
  private Map<String, InputSpecUpdate> recoveredRootInputSpecUpdates = null;

  // Recovery related flags
  boolean recoveryInitEventSeen = false;
  boolean recoveryStartEventSeen = false;
  private VertexStats vertexStats = null;

  private final TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOpts;

  public VertexImpl(TezVertexID vertexId, VertexPlan vertexPlan,
      String vertexName, Configuration conf, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener, Clock clock,
      TaskHeartbeatHandler thh, boolean commitVertexOutputs,
      AppContext appContext, VertexLocationHint vertexLocationHint,
      Map<String, VertexGroupInfo> dagVertexGroups, TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOption) {
    this.vertexId = vertexId;
    this.vertexPlan = vertexPlan;
    this.vertexName = StringInterner.weakIntern(vertexName);
    this.conf = conf;
    this.clock = clock;
    this.appContext = appContext;
    this.commitVertexOutputs = commitVertexOutputs;

    this.taskAttemptListener = taskAttemptListener;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    if (LOG.isDebugEnabled()) {
      logLocationHints(this.vertexName, vertexLocationHint);
    }
    setTaskLocationHints(vertexLocationHint);
    
    this.dagUgi = appContext.getCurrentDAG().getDagUGI();

    this.taskResource = DagTypeConverters
        .createResourceRequestFromTaskConfig(vertexPlan.getTaskConfig());
    this.processorDescriptor = DagTypeConverters
        .convertProcessorDescriptorFromDAGPlan(vertexPlan
            .getProcessorDescriptor());
    this.localResources = DagTypeConverters
        .createLocalResourceMapFromDAGPlan(vertexPlan.getTaskConfig()
            .getLocalResourceList());
    this.localResources.putAll(appContext.getSessionResources());
    this.environment = DagTypeConverters
        .createEnvironmentMapFromDAGPlan(vertexPlan.getTaskConfig()
            .getEnvironmentSettingList());
    this.javaOpts = vertexPlan.getTaskConfig().hasJavaOpts() ? vertexPlan
        .getTaskConfig().getJavaOpts() : null;
    this.taskSpecificLaunchCmdOpts = taskSpecificLaunchCmdOption;
    this.containerContext = new ContainerContext(this.localResources,
        appContext.getCurrentDAG().getCredentials(), this.environment, this.javaOpts, this);

    if (vertexPlan.getInputsCount() > 0) {
      setAdditionalInputs(vertexPlan.getInputsList());
    }
    if (vertexPlan.getOutputsCount() > 0) {
      setAdditionalOutputs(vertexPlan.getOutputsList());
    }

    // Setup the initial parallelism early. This may be changed after
    // initialization or on a setParallelism call.
    this.numTasks = vertexPlan.getTaskConfig().getNumTasks();

    this.dagVertexGroups = dagVertexGroups;

    logIdentifier =  this.getVertexId() + " [" + this.getName() + "]";
    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  protected StateMachine<VertexState, VertexEventType, VertexEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public TezVertexID getVertexId() {
    return vertexId;
  }

  @Override
  public VertexPlan getVertexPlan() {
    return vertexPlan;
  }

  @Override
  public int getDistanceFromRoot() {
    return distanceFromRoot;
  }

  @Override
  public String getName() {
    return vertexName;
  }

  EventHandler getEventHandler() {
    return this.eventHandler;
  }

  @Override
  public Task getTask(TezTaskID taskID) {
    readLock.lock();
    try {
      return tasks.get(taskID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Task getTask(int taskIndex) {
    readLock.lock();
    try {
      // does it matter to create a duplicate list for efficiency
      // instead of traversing the map
      // local assign to LinkedHashMap to ensure that sequential traversal
      // assumption is satisfied
      LinkedHashMap<TezTaskID, Task> taskList = tasks;
      int i=0;
      for(Map.Entry<TezTaskID, Task> entry : taskList.entrySet()) {
        if(taskIndex == i) {
          return entry.getValue();
        }
        ++i;
      }
      return null;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getTotalTasks() {
    return numTasks;
  }

  @Override
  public int getCompletedTasks() {
    readLock.lock();
    try {
      return succeededTaskCount + failedTaskCount + killedTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getSucceededTasks() {
    readLock.lock();
    try {
      return succeededTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getRunningTasks() {
    readLock.lock();
    try {
      int num=0;
      for (Task task : tasks.values()) {
        if(task.getState() == TaskState.RUNNING)
          num++;
      }
      return num;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezCounters getAllCounters() {

    readLock.lock();

    try {
      VertexState state = getInternalState();
      if (state == VertexState.ERROR || state == VertexState.FAILED
          || state == VertexState.KILLED || state == VertexState.SUCCEEDED) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      TezCounters counters = new TezCounters();
      return incrTaskCounters(counters, tasks.values());

    } finally {
      readLock.unlock();
    }
  }

  public VertexStats getVertexStats() {

    readLock.lock();
    try {
      VertexState state = getInternalState();
      if (state == VertexState.ERROR || state == VertexState.FAILED
          || state == VertexState.KILLED || state == VertexState.SUCCEEDED) {
        this.mayBeConstructFinalFullCounters();
        return this.vertexStats;
      }

      VertexStats stats = new VertexStats();
      return updateVertexStats(stats, tasks.values());

    } finally {
      readLock.unlock();
    }
  }


  public static TezCounters incrTaskCounters(
      TezCounters counters, Collection<Task> tasks) {
    for (Task task : tasks) {
      counters.incrAllCounters(task.getCounters());
    }
    return counters;
  }

  public static VertexStats updateVertexStats(
      VertexStats stats, Collection<Task> tasks) {
    for (Task task : tasks) {
      stats.updateStats(task.getReport());
    }
    return stats;
  }


  @Override
  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();
    try {
      computeProgress();
      return progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ProgressBuilder getVertexProgress() {
    this.readLock.lock();
    try {
      ProgressBuilder progress = new ProgressBuilder();
      progress.setTotalTaskCount(numTasks);
      progress.setSucceededTaskCount(succeededTaskCount);
      progress.setRunningTaskCount(getRunningTasks());
      progress.setFailedTaskCount(failedTaskCount);
      progress.setKilledTaskCount(killedTaskCount);
      return progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public VertexStatusBuilder getVertexStatus(
      Set<StatusGetOpts> statusOptions) {
    this.readLock.lock();
    try {
      VertexStatusBuilder status = new VertexStatusBuilder();
      status.setState(getInternalState());
      status.setDiagnostics(diagnostics);
      status.setProgress(getVertexProgress());
      if (statusOptions.contains(StatusGetOpts.GET_COUNTERS)) {
        status.setVertexCounters(getAllCounters());
      }
      return status;
    } finally {
      this.readLock.unlock();
    }
  }
  
  @Override
  public TaskLocationHint getTaskLocationHint(TezTaskID taskId) {
    this.readLock.lock();
    try {
      if (taskLocationHints == null || 
          taskLocationHints.length <= taskId.getId()) {
        return null;
      }
      return taskLocationHints[taskId.getId()];
    } finally {
      this.readLock.unlock();
    }
  }

  private void computeProgress() {
    this.readLock.lock();
    try {
      float progress = 0f;
      for (Task task : this.tasks.values()) {
        progress += (task.isFinished() ? 1f : task.getProgress());
      }
      if (this.numTasks != 0) {
        progress /= this.numTasks;
      }
      this.progress = progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<TezTaskID, Task> getTasks() {
    synchronized (tasksSyncHandle) {
      lazyTasksCopyNeeded = true;
      return Collections.unmodifiableMap(tasks);
    }
  }

  @Override
  public VertexState getState() {
    readLock.lock();
    try {
      return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Set the terminationCause if it had not yet been set.
   *
   * @param trigger The trigger
   * @return true if setting the value succeeded.
   */
  boolean trySetTerminationCause(VertexTerminationCause trigger) {
    if(terminationCause == null){
      terminationCause = trigger;
      return true;
    }
    return false;
  }

  @Override
  public VertexTerminationCause getTerminationCause(){
    readLock.lock();
    try {
      return terminationCause;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public AppContext getAppContext() {
    return this.appContext;
  }

  private void handleParallelismUpdate(int newParallelism,
      Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
      Map<String, InputSpecUpdate> rootInputSpecUpdates) {
    LinkedHashMap<TezTaskID, Task> currentTasks = this.tasks;
    Iterator<Map.Entry<TezTaskID, Task>> iter = currentTasks.entrySet()
        .iterator();
    int i = 0;
    while (iter.hasNext()) {
      i++;
      iter.next();
      if (i <= newParallelism) {
        continue;
      }
      iter.remove();
    }
    this.recoveredSourceEdgeManagers = sourceEdgeManagers;
    this.recoveredRootInputSpecUpdates = rootInputSpecUpdates;
  }

  @Override
  public VertexState restoreFromEvent(HistoryEvent historyEvent) {
    switch (historyEvent.getEventType()) {
      case VERTEX_INITIALIZED:
        recoveryInitEventSeen = true;
        recoveredState = setupVertex((VertexInitializedEvent) historyEvent);
        createTasks();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Recovered state for vertex after Init event"
              + ", vertex=" + logIdentifier
              + ", recoveredState=" + recoveredState);
        }
        return recoveredState;
      case VERTEX_STARTED:
        if (!recoveryInitEventSeen) {
          throw new RuntimeException("Started Event seen but"
              + " no Init Event was encountered earlier");
        }
        recoveryStartEventSeen = true;
        VertexStartedEvent startedEvent = (VertexStartedEvent) historyEvent;
        startTimeRequested = startedEvent.getStartRequestedTime();
        startedTime = startedEvent.getStartTime();
        recoveredState = VertexState.RUNNING;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Recovered state for vertex after Started event"
              + ", vertex=" + logIdentifier
              + ", recoveredState=" + recoveredState);
        }
        return recoveredState;
      case VERTEX_PARALLELISM_UPDATED:
        VertexParallelismUpdatedEvent updatedEvent =
            (VertexParallelismUpdatedEvent) historyEvent;
        if (updatedEvent.getVertexLocationHint() != null) {
          setTaskLocationHints(updatedEvent.getVertexLocationHint());
        }
        numTasks = updatedEvent.getNumTasks();
        handleParallelismUpdate(numTasks, updatedEvent.getSourceEdgeManagers(),
          updatedEvent.getRootInputSpecUpdates());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Recovered state for vertex after parallelism updated event"
              + ", vertex=" + logIdentifier
              + ", recoveredState=" + recoveredState);
        }
        return recoveredState;
      case VERTEX_COMMIT_STARTED:
        recoveryCommitInProgress = true;
        hasCommitter = true;
        return recoveredState;
      case VERTEX_FINISHED:
        VertexFinishedEvent finishedEvent = (VertexFinishedEvent) historyEvent;
        if (finishedEvent.isFromSummary()) {
          summaryCompleteSeen  = true;
        } else {
          vertexCompleteSeen = true;
        }
        recoveryCommitInProgress = false;
        recoveredState = finishedEvent.getState();
        diagnostics.add(finishedEvent.getDiagnostics());
        finishTime = finishedEvent.getFinishTime();
        // TODO counters ??
        if (LOG.isDebugEnabled()) {
          LOG.debug("Recovered state for vertex after finished event"
              + ", vertex=" + logIdentifier
              + ", recoveredState=" + recoveredState);
        }
        return recoveredState;
      case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
        VertexDataMovementEventsGeneratedEvent vEvent =
            (VertexDataMovementEventsGeneratedEvent) historyEvent;
        this.recoveredEvents.addAll(vEvent.getTezEvents());
        return recoveredState;
      default:
        throw new RuntimeException("Unexpected event received for restoring"
            + " state, eventType=" + historyEvent.getEventType());

    }
  }

  @Override
  public String getLogIdentifier() {
    return this.logIdentifier;
  }

  private void setTaskLocationHints(VertexLocationHint vertexLocationHint) {
    if (vertexLocationHint != null && 
        vertexLocationHint.getTaskLocationHints() != null && 
        !vertexLocationHint.getTaskLocationHints().isEmpty()) {
      List<TaskLocationHint> locHints = vertexLocationHint.getTaskLocationHints();
      taskLocationHints = locHints.toArray(new TaskLocationHint[locHints.size()]);
    }
  }

  @Override
  public void scheduleTasks(List<TaskWithLocationHint> tasksToSchedule) {
    writeLock.lock();
    try {
      tasksNotYetScheduled = false;
      if (!pendingTaskEvents.isEmpty()) {
        LOG.info("Routing pending task events for vertex: " + logIdentifier);
        VertexImpl.ROUTE_EVENT_TRANSITION.transition(this,
            new VertexEventRouteEvent(getVertexId(), pendingTaskEvents));
        pendingTaskEvents.clear();
      }
      for (TaskWithLocationHint task : tasksToSchedule) {
        if (numTasks <= task.getTaskIndex().intValue()) {
          throw new TezUncheckedException(
              "Invalid taskId: " + task.getTaskIndex() + " for vertex: " + logIdentifier);
        }
        TaskLocationHint locationHint = task.getTaskLocationHint();
        if (locationHint != null) {
          if (taskLocationHints == null) {
            taskLocationHints = new TaskLocationHint[numTasks];
          }
          taskLocationHints[task.getTaskIndex().intValue()] = locationHint;
        }
        eventHandler.handle(new TaskEvent(
            TezTaskID.getInstance(vertexId, task.getTaskIndex().intValue()),
            TaskEventType.T_SCHEDULE));
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean setParallelism(int parallelism, VertexLocationHint vertexLocationHint,
      Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
      Map<String, InputSpecUpdate> rootInputSpecUpdates) {
    return setParallelism(parallelism, vertexLocationHint, sourceEdgeManagers, rootInputSpecUpdates,
        false);
  }

  private boolean setParallelism(int parallelism, VertexLocationHint vertexLocationHint,
      Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
      Map<String, InputSpecUpdate> rootInputSpecUpdates,
      boolean recovering) {
    if (recovering) {
      writeLock.lock();
      try {
        if (sourceEdgeManagers != null) {
          for(Map.Entry<String, EdgeManagerPluginDescriptor> entry :
              sourceEdgeManagers.entrySet()) {
            LOG.info("Recovering edge manager for source:"
                + entry.getKey() + " destination: " + getVertexId());
            Vertex sourceVertex = appContext.getCurrentDAG().getVertex(entry.getKey());
            Edge edge = sourceVertices.get(sourceVertex);
            try {
              edge.setCustomEdgeManager(entry.getValue());
            } catch (Exception e) {
              LOG.warn("Failed to initialize edge manager for edge"
                  + ", sourceVertexName=" + sourceVertex.getName()
                  + ", destinationVertexName=" + edge.getDestinationVertexName(),
                  e);
              return false;
            }
          }
        }
        
        // Restore any rootInputSpecUpdates which may have been registered during a parallelism
        // update.
        if (rootInputSpecUpdates != null) {
          LOG.info("Got updated RootInputsSpecs during recovery: " + rootInputSpecUpdates.toString());
          this.rootInputSpecs.putAll(rootInputSpecUpdates);
        }
        return true;
      } finally {
        writeLock.unlock();
      }
    }
    Preconditions.checkArgument(parallelism >= 0, "Parallelism must be >=0. Value: " 
    + parallelism + " for vertex: " + logIdentifier);
    setVertexLocationHint(vertexLocationHint);
    writeLock.lock();
    try {
      if (parallelismSet == true) {
        LOG.info("Parallelism can only be set dynamically once per vertex: " + logIdentifier);
        return false;
      }
      
      parallelismSet = true;

      // Input initializer/Vertex Manager/1-1 split expected to set parallelism.
      if (numTasks == -1) {
        if (getState() != VertexState.INITIALIZING) {
          throw new TezUncheckedException(
              "Vertex state is not Initializing. Value: " + getState()
                  + " for vertex: " + logIdentifier);
        }
        
        if(sourceEdgeManagers != null) {
          for(Map.Entry<String, EdgeManagerPluginDescriptor> entry : sourceEdgeManagers.entrySet()) {
            LOG.info("Replacing edge manager for source:"
                + entry.getKey() + " destination: " + getVertexId());
            Vertex sourceVertex = appContext.getCurrentDAG().getVertex(entry.getKey());
            Edge edge = sourceVertices.get(sourceVertex);
            try {
              edge.setCustomEdgeManager(entry.getValue());
            } catch (Exception e) {
              LOG.warn("Failed to initialize edge manager for edge"
                  + ", sourceVertexName=" + sourceVertex.getName()
                  + ", destinationVertexName=" + edge.getDestinationVertexName(),
                  e);
              return false;
            }
          }
        }
        if (rootInputSpecUpdates != null) {
          LOG.info("Got updated RootInputsSpecs: " + rootInputSpecUpdates.toString());
          // Sanity check for correct number of updates.
          for (Entry<String, InputSpecUpdate> rootInputSpecUpdateEntry : rootInputSpecUpdates
              .entrySet()) {
            Preconditions
                .checkState(
                    rootInputSpecUpdateEntry.getValue().isForAllWorkUnits()
                        || (rootInputSpecUpdateEntry.getValue().getAllNumPhysicalInputs() != null && rootInputSpecUpdateEntry
                            .getValue().getAllNumPhysicalInputs().size() == parallelism),
                    "Not enough input spec updates for root input named "
                        + rootInputSpecUpdateEntry.getKey());
          }
          this.rootInputSpecs.putAll(rootInputSpecUpdates);
        }
        this.numTasks = parallelism;
        this.createTasks();
        LOG.info("Vertex " + getVertexId() + 
            " parallelism set to " + parallelism);
        if (canInitVertex()) {
          getEventHandler().handle(new VertexEvent(getVertexId(), VertexEventType.V_READY_TO_INIT));
        }
      } else {
        // This is an artificial restriction since there's no way of knowing whether a VertexManager
        // will attempt to update root input specs. When parallelism has not been initialized, the
        // Vertex will not be in started state so it's safe to update the specifications.
        // TODO TEZ-937 - add e mechanism to query vertex managers, or for VMs to indicate readines
        // for a vertex to start.
        Preconditions.checkState(rootInputSpecUpdates == null,
            "Root Input specs can only be updated when the vertex is configured with -1 tasks");
        if (parallelism >= numTasks) {
          // not that hard to support perhaps. but checking right now since there
          // is no use case for it and checking may catch other bugs.
          LOG.warn("Increasing parallelism is not supported, vertexId="
              + logIdentifier);
          return false;
        }
        if (parallelism == numTasks) {
          LOG.info("setParallelism same as current value: " + parallelism + 
              " for vertex: " + logIdentifier);
          Preconditions.checkArgument(sourceEdgeManagers != null,
              "Source edge managers or RootInputSpecs must be set when not changing parallelism");
        } else {
          LOG.info(
              "Resetting vertex location hints due to change in parallelism for vertex: " + logIdentifier);
          vertexLocationHint = null;
        }

        // start buffering incoming events so that we can re-route existing events
        for (Edge edge : sourceVertices.values()) {
          edge.startEventBuffering();
        }
  
        // assign to local variable of LinkedHashMap to make sure that changing
        // type of task causes compile error. We depend on LinkedHashMap for order
        LinkedHashMap<TezTaskID, Task> currentTasks = this.tasks;
        Iterator<Map.Entry<TezTaskID, Task>> iter = currentTasks.entrySet()
            .iterator();
        int i = 0;
        while (iter.hasNext()) {
          i++;
          Map.Entry<TezTaskID, Task> entry = iter.next();
          Task task = entry.getValue();
          if (task.getState() != TaskState.NEW) {
            LOG.warn(
                "All tasks must be in initial state when changing parallelism"
                    + " for vertex: " + getVertexId() + " name: " + getName());
            return false;
          }
          if (i <= parallelism) {
            continue;
          }
          LOG.info("Removing task: " + entry.getKey());
          iter.remove();
        }
        LOG.info("Vertex " + logIdentifier + 
            " parallelism set to " + parallelism + " from " + numTasks);
        this.numTasks = parallelism;
        assert tasks.size() == numTasks;
  
        // set new edge managers
        if(sourceEdgeManagers != null) {
          for(Map.Entry<String, EdgeManagerPluginDescriptor> entry : sourceEdgeManagers.entrySet()) {
            LOG.info("Replacing edge manager for source:"
                + entry.getKey() + " destination: " + getVertexId());
            Vertex sourceVertex = appContext.getCurrentDAG().getVertex(entry.getKey());
            Edge edge = sourceVertices.get(sourceVertex);
            try {
              edge.setCustomEdgeManager(entry.getValue());
            } catch (Exception e) {
              LOG.warn("Failed to initialize edge manager for edge"
                  + ", sourceVertexName=" + sourceVertex.getName()
                  + ", destinationVertexName=" + edge.getDestinationVertexName(),
                  e);
              return false;
            }
          }
        }

        VertexParallelismUpdatedEvent parallelismUpdatedEvent =
            new VertexParallelismUpdatedEvent(vertexId, numTasks,
                vertexLocationHint,
                sourceEdgeManagers, rootInputSpecUpdates);
        appContext.getHistoryHandler().handle(new DAGHistoryEvent(getDAGId(),
            parallelismUpdatedEvent));

        // stop buffering events
        for (Edge edge : sourceVertices.values()) {
          edge.stopEventBuffering();
        }
      }
      
      for (Map.Entry<Vertex, Edge> entry : targetVertices.entrySet()) {
        Edge edge = entry.getValue();
        if (edge.getEdgeProperty().getDataMovementType() 
            == DataMovementType.ONE_TO_ONE) {
          // inform these target vertices that we have changed parallelism
          VertexEventOneToOneSourceSplit event = 
              new VertexEventOneToOneSourceSplit(entry.getKey().getVertexId(),
                  getVertexId(),
                  ((originalOneToOneSplitSource!=null) ? 
                      originalOneToOneSplitSource : getVertexId()), 
                  numTasks);
          getEventHandler().handle(event);
        }
      }

    } finally {
      writeLock.unlock();
    }
    
    return true;
  }

  public void setVertexLocationHint(VertexLocationHint vertexLocationHint) {
    writeLock.lock();
    try {
      if (LOG.isDebugEnabled()) {
        logLocationHints(this.vertexName, vertexLocationHint);
      }
      setTaskLocationHints(vertexLocationHint);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  /**
   * The only entry point to change the Vertex.
   */
  public void handle(VertexEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing VertexEvent " + event.getVertexId()
          + " of type " + event.getType() + " while in state "
          + getInternalState() + ". Event: " + event);
    }
    try {
      writeLock.lock();
      VertexState oldState = getInternalState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        String message = "Invalid event " + event.getType() +
            " on vertex " + this.vertexName +
            " with vertexId " + this.vertexId +
            " at current state " + oldState;
        LOG.error("Can't handle " + message, e);
        addDiagnostic(message);
        eventHandler.handle(new VertexEvent(this.vertexId,
            VertexEventType.V_INTERNAL_ERROR));
      }

      if (oldState != getInternalState()) {
        LOG.info(logIdentifier + " transitioned from " + oldState + " to "
            + getInternalState() + " due to event "
            + event.getType());
      }
    }

    finally {
      writeLock.unlock();
    }
  }

  private VertexState getInternalState() {
    readLock.lock();
    try {
     return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  //helpful in testing
  protected void addTask(Task task) {
    synchronized (tasksSyncHandle) {
      if (lazyTasksCopyNeeded) {
        LinkedHashMap<TezTaskID, Task> newTasks = new LinkedHashMap<TezTaskID, Task>();
        newTasks.putAll(tasks);
        tasks = newTasks;
        lazyTasksCopyNeeded = false;
      }
    }
    tasks.put(task.getTaskId(), task);
    // TODO Metrics
    //metrics.waitingTask(task);
  }

  void setFinishTime() {
    finishTime = clock.getTime();
  }


  void logJobHistoryVertexInitializedEvent() {
    VertexInitializedEvent initEvt = new VertexInitializedEvent(vertexId, vertexName,
        initTimeRequested, initedTime, numTasks,
        getProcessorName(), getAdditionalInputs());
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGId(), initEvt));
  }

  void logJobHistoryVertexStartedEvent() {
    VertexStartedEvent startEvt = new VertexStartedEvent(vertexId,
        startTimeRequested, startedTime);
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGId(), startEvt));
  }

  void logJobHistoryVertexFinishedEvent() throws IOException {
    this.setFinishTime();
    VertexFinishedEvent finishEvt = new VertexFinishedEvent(vertexId,
        vertexName, initTimeRequested, initedTime, startTimeRequested,
        startedTime, finishTime, VertexState.SUCCEEDED, "",
        getAllCounters(), getVertexStats());
    this.appContext.getHistoryHandler().handleCriticalEvent(
        new DAGHistoryEvent(getDAGId(), finishEvt));
  }

  void logJobHistoryVertexFailedEvent(VertexState state) throws IOException {
    VertexFinishedEvent finishEvt = new VertexFinishedEvent(vertexId,
        vertexName, initTimeRequested, initedTime, startTimeRequested,
        startedTime, clock.getTime(), state, StringUtils.join(LINE_SEPARATOR,
            getDiagnostics()), getAllCounters(), getVertexStats());
    this.appContext.getHistoryHandler().handleCriticalEvent(
        new DAGHistoryEvent(getDAGId(), finishEvt));
  }

  static VertexState checkVertexForCompletion(final VertexImpl vertex) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking for vertex completion for "
          + vertex.logIdentifier
          + ", numTasks=" + vertex.numTasks
          + ", failedTaskCount=" + vertex.failedTaskCount
          + ", killedTaskCount=" + vertex.killedTaskCount
          + ", successfulTaskCount=" + vertex.succeededTaskCount
          + ", completedTaskCount=" + vertex.completedTaskCount
          + ", terminationCause=" + vertex.terminationCause);
    }

    //check for vertex failure first
    if (vertex.completedTaskCount > vertex.tasks.size()) {
      LOG.error("task completion accounting issue: completedTaskCount > nTasks:"
          + " for vertex " + vertex.logIdentifier
          + ", numTasks=" + vertex.numTasks
          + ", failedTaskCount=" + vertex.failedTaskCount
          + ", killedTaskCount=" + vertex.killedTaskCount
          + ", successfulTaskCount=" + vertex.succeededTaskCount
          + ", completedTaskCount=" + vertex.completedTaskCount
          + ", terminationCause=" + vertex.terminationCause);
    }

    if (vertex.completedTaskCount == vertex.tasks.size()) {
      //Only succeed if tasks complete successfully and no terminationCause is registered.
      if(vertex.succeededTaskCount == vertex.tasks.size() && vertex.terminationCause == null) {
        LOG.info("Vertex succeeded: " + vertex.logIdentifier);
        try {
          if (vertex.commitVertexOutputs && !vertex.committed.getAndSet(true)) {
            // commit only once. Dont commit shared outputs
            LOG.info("Invoking committer commit for vertex, vertexId="
                + vertex.logIdentifier);
            if (vertex.outputCommitters != null
                && !vertex.outputCommitters.isEmpty()) {
              boolean firstCommit = true;
              for (Entry<String, OutputCommitter> entry : vertex.outputCommitters.entrySet()) {
                final OutputCommitter committer = entry.getValue();
                final String outputName = entry.getKey();
                if (vertex.sharedOutputs.contains(outputName)) {
                  // dont commit shared committers. Will be committed by the DAG
                  continue;
                }
                if (firstCommit) {
                  // Log commit start event on first actual commit
                  try {
                    vertex.appContext.getHistoryHandler().handleCriticalEvent(
                        new DAGHistoryEvent(vertex.getDAGId(),
                            new VertexCommitStartedEvent(vertex.vertexId,
                                vertex.clock.getTime())));
                  } catch (IOException e) {
                    LOG.error("Failed to persist commit start event to recovery, vertexId="
                        + vertex.logIdentifier, e);
                    vertex.trySetTerminationCause(VertexTerminationCause.INTERNAL_ERROR);
                    return vertex.finished(VertexState.FAILED);
                  }
                } else {
                  firstCommit = false;
                }
                vertex.dagUgi.doAs(new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
                      LOG.info("Invoking committer commit for output=" + outputName
                          + ", vertexId=" + vertex.logIdentifier);
                      committer.commitOutput();
                    return null;
                  }
                });
              }
            }
          }
        } catch (Exception e) {
          LOG.error("Failed to do commit on vertex, vertexId="
              + vertex.logIdentifier, e);
          vertex.trySetTerminationCause(VertexTerminationCause.COMMIT_FAILURE);
          return vertex.finished(VertexState.FAILED);
        }
        return vertex.finished(VertexState.SUCCEEDED);
      }
      else if(vertex.terminationCause == VertexTerminationCause.DAG_KILL ){
        vertex.setFinishTime();
        String diagnosticMsg = "Vertex killed due to user-initiated job kill. "
            + "failedTasks:"
            + vertex.failedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.addDiagnostic(diagnosticMsg);
        vertex.abortVertex(VertexStatus.State.KILLED);
        return vertex.finished(VertexState.KILLED);
      }
      else if(vertex.terminationCause == VertexTerminationCause.OTHER_VERTEX_FAILURE ){
        vertex.setFinishTime();
        String diagnosticMsg = "Vertex killed as other vertex failed. "
            + "failedTasks:"
            + vertex.failedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.addDiagnostic(diagnosticMsg);
        vertex.abortVertex(VertexStatus.State.KILLED);
        return vertex.finished(VertexState.KILLED);
      }
      else if(vertex.terminationCause == VertexTerminationCause.OWN_TASK_FAILURE ){
        if(vertex.failedTaskCount == 0){
          LOG.error("task failure accounting error.  terminationCause=TASK_FAILURE but vertex.failedTaskCount == 0");
        }
        vertex.setFinishTime();
        String diagnosticMsg = "Vertex failed as one or more tasks failed. "
            + "failedTasks:"
            + vertex.failedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.addDiagnostic(diagnosticMsg);
        vertex.abortVertex(VertexStatus.State.FAILED);
        return vertex.finished(VertexState.FAILED);
      }
      else if (vertex.terminationCause == VertexTerminationCause.INTERNAL_ERROR) {
        vertex.setFinishTime();
        String diagnosticMsg = "Vertex failed/killed due to internal error. "
            + "failedTasks:"
            + vertex.failedTaskCount
            + " killedTasks:"
            + vertex.killedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.abortVertex(State.FAILED);
        return vertex.finished(VertexState.FAILED);
      }
      else {
        //should never occur
        throw new TezUncheckedException("All tasks complete, but cannot determine final state of vertex"
            + ", failedTaskCount=" + vertex.failedTaskCount
            + ", killedTaskCount=" + vertex.killedTaskCount
            + ", successfulTaskCount=" + vertex.succeededTaskCount
            + ", completedTaskCount=" + vertex.completedTaskCount
            + ", terminationCause=" + vertex.terminationCause);
      }
    }

    //return the current state, Vertex not finished yet
    return vertex.getInternalState();
  }

  /**
   * Set the terminationCause and send a kill-message to all tasks.
   * The task-kill messages are only sent once.
   */
  void tryEnactKill(VertexTerminationCause trigger,
      TaskTerminationCause taskterminationCause) {
    if(trySetTerminationCause(trigger)){
      LOG.info("Killing tasks in vertex: " + logIdentifier + " due to trigger: "
          + trigger);
      for (Task task : tasks.values()) {
        eventHandler.handle(
            new TaskEventTermination(task.getTaskId(), taskterminationCause));
      }
    }
  }

  VertexState finished(VertexState finalState,
      VertexTerminationCause terminationCause) {
    if (finishTime == 0) setFinishTime();

    switch (finalState) {
      case ERROR:
        eventHandler.handle(new DAGEvent(getDAGId(),
            DAGEventType.INTERNAL_ERROR));
        try {
          logJobHistoryVertexFailedEvent(finalState);
        } catch (IOException e) {
          LOG.error("Failed to send vertex finished event to recovery", e);
        }
        break;
      case KILLED:
      case FAILED:
        eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
            finalState, terminationCause));
        try {
          logJobHistoryVertexFailedEvent(finalState);
        } catch (IOException e) {
          LOG.error("Failed to send vertex finished event to recovery", e);
        }
        break;
      case SUCCEEDED:
        try {
          logJobHistoryVertexFinishedEvent();
          eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
              finalState));
        } catch (IOException e) {
          LOG.error("Failed to send vertex finished event to recovery", e);
          finalState = VertexState.FAILED;
          this.terminationCause = VertexTerminationCause.INTERNAL_ERROR;
          eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
              finalState));
        }
        break;
      default:
        throw new TezUncheckedException("Unexpected VertexState: " + finalState);
    }
    return finalState;
  }

  VertexState finished(VertexState finalState) {
    return finished(finalState, null);
  }


  private void initializeCommitters() throws Exception {
    if (!this.additionalOutputSpecs.isEmpty()) {
      LOG.info("Invoking committer inits for vertex, vertexId=" + logIdentifier);
      for (Entry<String, RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> entry:
          additionalOutputs.entrySet())  {
        final String outputName = entry.getKey();
        final RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> od = entry.getValue();
        if (od.getControllerDescriptor() == null
            || od.getControllerDescriptor().getClassName() == null) {
          LOG.info("Ignoring committer as none specified for output="
              + outputName
              + ", vertexId=" + logIdentifier);
          continue;
        }
        LOG.info("Instantiating committer for output=" + outputName
            + ", vertexId=" + logIdentifier
            + ", committerClass=" + od.getControllerDescriptor().getClassName());

        dagUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            OutputCommitterContext outputCommitterContext =
                new OutputCommitterContextImpl(appContext.getApplicationID(),
                    appContext.getApplicationAttemptId().getAttemptId(),
                    appContext.getCurrentDAG().getName(),
                    vertexName,
                    od,
                    vertexId.getId());
            OutputCommitter outputCommitter = ReflectionUtils
                .createClazzInstance(od.getControllerDescriptor().getClassName(),
                    new Class[]{OutputCommitterContext.class},
                    new Object[]{outputCommitterContext});
            LOG.info("Invoking committer init for output=" + outputName
                + ", vertexId=" + logIdentifier);
            outputCommitter.initialize();
            outputCommitters.put(outputName, outputCommitter);
            LOG.info("Invoking committer setup for output=" + outputName
                + ", vertexId=" + logIdentifier);
            outputCommitter.setupOutput();
            return null;
          }
        });
      }
    }
  }

  private boolean initializeVertex() {
    try {
      initializeCommitters();
    } catch (Exception e) {
      LOG.warn("Vertex Committer init failed, vertexId=" + logIdentifier, e);
      addDiagnostic("Vertex init failed : "
          + StringUtils.stringifyException(e));
      trySetTerminationCause(VertexTerminationCause.INIT_FAILURE);
      abortVertex(VertexStatus.State.FAILED);
      finished(VertexState.FAILED);
      return false;
    }

    // TODO: Metrics
    initedTime = clock.getTime();

    logJobHistoryVertexInitializedEvent();
    return true;
  }

  /**
   * If the number of tasks are greater than the configured value
   * throw an exception that will fail job initialization
   */
  private void checkTaskLimits() {
    // no code, for now
  }

  private ContainerContext getContainerContext(int taskIdx) {
    if (taskSpecificLaunchCmdOpts.addTaskSpecificLaunchCmdOption(vertexName, taskIdx)) {
      String jvmOpts = taskSpecificLaunchCmdOpts.getTaskSpecificOption(javaOpts, vertexName, taskIdx);
      ContainerContext context = new ContainerContext(this.localResources,
          appContext.getCurrentDAG().getCredentials(), this.environment, jvmOpts);
      return context;
    } else {
      return this.containerContext;
    }
  }

  private void createTasks() {
    for (int i=0; i < this.numTasks; ++i) {
      ContainerContext conContext = getContainerContext(i);
      TaskImpl task =
          new TaskImpl(this.getVertexId(), i,
              this.eventHandler,
              conf,
              this.taskAttemptListener,
              this.clock,
              this.taskHeartbeatHandler,
              this.appContext,
              (this.targetVertices != null ?
                this.targetVertices.isEmpty() : true),
              this.taskResource,
              conContext);
      this.addTask(task);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Created task for vertex " + logIdentifier + ": " +
            task.getTaskId());
      }
    }

  }

  private VertexState setupVertex() {
    return setupVertex(null);
  }

  private VertexState setupVertex(VertexInitializedEvent event) {

    if (event == null) {
      initTimeRequested = clock.getTime();
    } else {
      initTimeRequested = event.getInitRequestedTime();
      initedTime = event.getInitedTime();
    }

    // VertexManager needs to be setup before attempting to Initialize any
    // Inputs - since events generated by them will be routed to the
    // VertexManager for handling.

    if (dagVertexGroups != null && !dagVertexGroups.isEmpty()) {
      List<GroupInputSpec> groupSpecList = Lists.newLinkedList();
      for (VertexGroupInfo groupInfo : dagVertexGroups.values()) {
        if (groupInfo.edgeMergedInputs.containsKey(getName())) {
          InputDescriptor mergedInput = groupInfo.edgeMergedInputs.get(getName());
          groupSpecList.add(new GroupInputSpec(groupInfo.groupName,
              Lists.newLinkedList(groupInfo.groupMembers), mergedInput));
        }
      }
      if (!groupSpecList.isEmpty()) {
        groupInputSpecList = groupSpecList;
      }
    }

    // Check if any inputs need initializers
    if (event != null) {
      this.rootInputDescriptors = event.getAdditionalInputs();
    } else {
      if (rootInputDescriptors != null) {
        LOG.info("Root Inputs exist for Vertex: " + getName() + " : "
            + rootInputDescriptors);
        for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input 
            : rootInputDescriptors.values()) {
          if (input.getControllerDescriptor() != null && 
              input.getControllerDescriptor().getClassName() != null) {
            if (inputsWithInitializers == null) {
              inputsWithInitializers = Sets.newHashSet();
            }
            inputsWithInitializers.add(input.getName());
            LOG.info("Starting root input initializer for input: "
                + input.getName() + ", with class: ["
                + input.getControllerDescriptor().getClassName() + "]");
          }
        }
      }
    }

    boolean hasBipartite = false;
    if (sourceVertices != null) {
      for (Edge edge : sourceVertices.values()) {
        if (edge.getEdgeProperty().getDataMovementType() == DataMovementType.SCATTER_GATHER) {
          hasBipartite = true;
          break;
        }
      }
    }

    if (hasBipartite && inputsWithInitializers != null) {
      LOG.fatal("A vertex with an Initial Input and a Shuffle Input are not supported at the moment");
      if (event != null) {
        return VertexState.FAILED;
      } else {
        return finished(VertexState.FAILED);
      }
    }

    assignVertexManager();

    vertexManager.initialize();

    // Setup tasks early if possible. If the VertexManager is not being used
    // to set parallelism, sending events to Tasks is safe (and less confusing
    // then relying on tasks to be created after TaskEvents are generated).
    // For VertexManagers setting parallelism, the setParallelism call needs
    // to be inline.
    if (event != null) {
      numTasks = event.getNumTasks();
    } else {
      numTasks = getVertexPlan().getTaskConfig().getNumTasks();
    }

    if (!(numTasks == -1 || numTasks >= 0)) {
      addDiagnostic("Invalid task count for vertex"
          + ", numTasks=" + numTasks);
      trySetTerminationCause(VertexTerminationCause.INVALID_NUM_OF_TASKS);
      if (event != null) {
        abortVertex(VertexStatus.State.FAILED);
        return finished(VertexState.FAILED);
      } else {
        return VertexState.FAILED;
      }
    }

    checkTaskLimits();
    return VertexState.INITED;
  }
  
  private void assignVertexManager() {
    boolean hasBipartite = false;
    boolean hasOneToOne = false;
    boolean hasCustom = false;
    if (sourceVertices != null) {
      for (Edge edge : sourceVertices.values()) {
        switch(edge.getEdgeProperty().getDataMovementType()) {
        case SCATTER_GATHER:
          hasBipartite = true;
          break;
        case ONE_TO_ONE:
          hasOneToOne = true;
          break;
        case BROADCAST:
          break;
        case CUSTOM:
          hasCustom = true;
          break;
        default:
          throw new TezUncheckedException("Unknown data movement type: " + 
              edge.getEdgeProperty().getDataMovementType());
        }
      }
    }
    
    boolean hasUserVertexManager = vertexPlan.hasVertexManagerPlugin();

    if (hasUserVertexManager) {
      VertexManagerPluginDescriptor pluginDesc = DagTypeConverters
          .convertVertexManagerPluginDescriptorFromDAGPlan(vertexPlan
              .getVertexManagerPlugin());
      LOG.info("Setting user vertex manager plugin: "
          + pluginDesc.getClassName() + " on vertex: " + getName());
      vertexManager = new VertexManager(pluginDesc, this, appContext);
    } else {
      // Intended order of picking a vertex manager
      // If there is an InputInitializer then we use the RootInputVertexManager. May be fixed by TEZ-703
      // If there is a custom edge we fall back to default ImmediateStartVertexManager
      // If there is a one to one edge then we use the InputReadyVertexManager
      // If there is a scatter-gather edge then we use the ShuffleVertexManager
      // Else we use the default ImmediateStartVertexManager
      if (inputsWithInitializers != null) {
        LOG.info("Setting vertexManager to RootInputVertexManager for "
            + logIdentifier);
        vertexManager = new VertexManager(
            new VertexManagerPluginDescriptor(RootInputVertexManager.class.getName()),
            this, appContext);
      } else if (hasOneToOne && !hasCustom) {
        LOG.info("Setting vertexManager to InputReadyVertexManager for "
            + logIdentifier);
        vertexManager = new VertexManager(
            new VertexManagerPluginDescriptor(InputReadyVertexManager.class.getName()),
            this, appContext);
      } else if (hasBipartite && !hasCustom) {
        LOG.info("Setting vertexManager to ShuffleVertexManager for "
            + logIdentifier);
        vertexManager = new VertexManager(
            new VertexManagerPluginDescriptor(ShuffleVertexManager.class.getName()),
            this, appContext);
      } else {
        // schedule all tasks upon vertex start. Default behavior.
        LOG.info("Setting vertexManager to ImmediateStartVertexManager for "
            + logIdentifier);
        vertexManager = new VertexManager(
            new VertexManagerPluginDescriptor(ImmediateStartVertexManager.class.getName()),
            this, appContext);
      }
    }
  }

  public static class StartRecoverTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent vertexEvent) {
      VertexEventRecoverVertex recoverEvent = (VertexEventRecoverVertex) vertexEvent;
      VertexState desiredState = recoverEvent.getDesiredState();

      switch (desiredState) {
        case RUNNING:
          break;
        case SUCCEEDED:
        case KILLED:
        case FAILED:
        case ERROR:
          switch (desiredState) {
            case SUCCEEDED:
              vertex.succeededTaskCount = vertex.numTasks;
              vertex.completedTaskCount = vertex.numTasks;
              break;
            case KILLED:
              vertex.killedTaskCount = vertex.numTasks;
              break;
            case FAILED:
            case ERROR:
              vertex.failedTaskCount = vertex.numTasks;
              break;
          }
          if (vertex.tasks != null) {
            TaskState taskState = TaskState.KILLED;
            switch (desiredState) {
              case SUCCEEDED:
                taskState = TaskState.SUCCEEDED;
                break;
              case KILLED:
                taskState = TaskState.KILLED;
                break;
              case FAILED:
              case ERROR:
                taskState = TaskState.FAILED;
                break;
            }
            for (Task task : vertex.tasks.values()) {
              vertex.eventHandler.handle(
                  new TaskEventRecoverTask(task.getTaskId(),
                      taskState, false));
            }
          }
          LOG.info("DAG informed Vertex of its final completed state"
              + ", vertex=" + vertex.logIdentifier
              + ", state=" + desiredState);
          return desiredState;
        default:
          LOG.info("Unhandled desired state provided by DAG"
              + ", vertex=" + vertex.logIdentifier
              + ", state=" + desiredState);
          vertex.finished(VertexState.ERROR);
      }

      VertexState endState;
      switch (vertex.recoveredState) {
        case NEW:
          // Trigger init and start as desired state is RUNNING
          // Drop all root events
          Iterator<TezEvent> iterator = vertex.recoveredEvents.iterator();
          while (iterator.hasNext()) {
            if (iterator.next().getEventType().equals(
                EventType.ROOT_INPUT_DATA_INFORMATION_EVENT)) {
              iterator.remove();
            }
          }
          vertex.eventHandler.handle(new VertexEvent(vertex.vertexId,
              VertexEventType.V_INIT));
          vertex.eventHandler.handle(new VertexEvent(vertex.vertexId,
              VertexEventType.V_START));
          endState = VertexState.NEW;
          break;
        case INITED:
          try {
            vertex.initializeCommitters();
          } catch (Exception e) {
            LOG.info("Failed to initialize committers"
                + ", vertex=" + vertex.logIdentifier, e);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.INIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }

          // Recover tasks
          if (vertex.tasks != null) {
            for (Task task : vertex.tasks.values()) {
              vertex.eventHandler.handle(
                  new TaskEventRecoverTask(task.getTaskId()));
            }
          }
          // Update tasks with their input payloads as needed

          vertex.eventHandler.handle(new VertexEvent(vertex.vertexId,
              VertexEventType.V_START));
          if (vertex.getInputVertices().isEmpty()) {
            endState = VertexState.INITED;
          } else {
            endState = VertexState.RECOVERING;
          }
          break;
        case RUNNING:
          vertex.tasksNotYetScheduled = false;
          try {
            vertex.initializeCommitters();
          } catch (Exception e) {
            LOG.info("Failed to initialize committers", e);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.INIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }

          // if commit in progress and desired state is not a succeeded one,
          // move to failed
          if (vertex.recoveryCommitInProgress) {
            LOG.info("Recovered vertex was in the middle of a commit"
                + ", failing Vertex=" + vertex.logIdentifier);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.COMMIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }
          assert vertex.tasks.size() == vertex.numTasks;
          if (vertex.tasks != null && vertex.numTasks != 0) {
            for (Task task : vertex.tasks.values()) {
              vertex.eventHandler.handle(
                  new TaskEventRecoverTask(task.getTaskId()));
            }
            vertex.vertexManager.onVertexStarted(vertex.pendingReportedSrcCompletions);
            endState = VertexState.RUNNING;
          } else {
            endState = VertexState.SUCCEEDED;
            vertex.finished(endState);
          }
          break;
        case SUCCEEDED:
        case FAILED:
        case KILLED:
          if (vertex.recoveredState == VertexState.SUCCEEDED
              && vertex.hasCommitter
              && vertex.summaryCompleteSeen && !vertex.vertexCompleteSeen) {
            LOG.warn("Cannot recover vertex as all recovery events not"
                + " found, vertexId=" + vertex.logIdentifier
                + ", hasCommitters=" + vertex.hasCommitter
                + ", summaryCompletionSeen=" + vertex.summaryCompleteSeen
                + ", finalCompletionSeen=" + vertex.vertexCompleteSeen);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.COMMIT_FAILURE);
            endState = VertexState.FAILED;
          } else {
            vertex.tasksNotYetScheduled = false;
            // recover tasks
            if (vertex.tasks != null && vertex.numTasks != 0) {
              TaskState taskState = TaskState.KILLED;
              switch (vertex.recoveredState) {
                case SUCCEEDED:
                  taskState = TaskState.SUCCEEDED;
                  break;
                case KILLED:
                  taskState = TaskState.KILLED;
                  break;
                case FAILED:
                  taskState = TaskState.FAILED;
                  break;
              }
              for (Task task : vertex.tasks.values()) {
                vertex.eventHandler.handle(
                    new TaskEventRecoverTask(task.getTaskId(),
                        taskState));
              }
              vertex.vertexManager.onVertexStarted(vertex.pendingReportedSrcCompletions);
              endState = VertexState.RUNNING;
            } else {
              endState = vertex.recoveredState;
              vertex.finished(endState);
            }
          }
          break;
        default:
          LOG.warn("Invalid recoveredState found when trying to recover"
              + " vertex"
              + ", vertex=" + vertex.logIdentifier
              + ", recoveredState=" + vertex.recoveredState);
          vertex.finished(VertexState.ERROR);
          endState = VertexState.ERROR;
          break;
      }
      if (!endState.equals(VertexState.RECOVERING)) {
        LOG.info("Recovered Vertex State"
            + ", vertexId=" + vertex.logIdentifier
            + ", state=" + endState
            + ", numInitedSourceVertices=" + vertex.numInitedSourceVertices
            + ", numStartedSourceVertices=" + vertex.numStartedSourceVertices
            + ", numRecoveredSourceVertices=" + vertex.numRecoveredSourceVertices
            + ", recoveredEvents="
            + ( vertex.recoveredEvents == null ? "null" : vertex.recoveredEvents.size())
            + ", tasksIsNull=" + (vertex.tasks == null)
            + ", numTasks=" + ( vertex.tasks == null ? "null" : vertex.tasks.size()));
        for (Entry<Vertex, Edge> entry : vertex.getOutputVertices().entrySet()) {
          vertex.eventHandler.handle(new VertexEventSourceVertexRecovered(
              entry.getKey().getVertexId(),
              vertex.vertexId, endState, null,
              vertex.getDistanceFromRoot()));
        }
      }
      if (EnumSet.of(VertexState.RUNNING, VertexState.SUCCEEDED, VertexState.INITED)
          .contains(endState)) {
        // Send events downstream
        vertex.routeRecoveredEvents(endState, vertex.recoveredEvents);
        vertex.recoveredEvents.clear();
      } else {
        // Ensure no recovered events
        if (!vertex.recoveredEvents.isEmpty()) {
          throw new RuntimeException("Invalid Vertex state"
              + ", found non-zero recovered events in invalid state"
              + ", vertex=" + vertex.logIdentifier
              + ", recoveredState=" + endState
              + ", recoveredEvents=" + vertex.recoveredEvents.size());
        }
      }
      return endState;
    }

  }

  private void routeRecoveredEvents(VertexState vertexState,
      List<TezEvent> tezEvents) {
    for (TezEvent tezEvent : tezEvents) {
      EventMetaData sourceMeta = tezEvent.getSourceInfo();
      TezTaskAttemptID srcTaId = sourceMeta.getTaskAttemptID();
      if (tezEvent.getEventType() == EventType.DATA_MOVEMENT_EVENT) {
        ((DataMovementEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
      } else if (tezEvent.getEventType() == EventType.COMPOSITE_DATA_MOVEMENT_EVENT) {
        ((CompositeDataMovementEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
      } else if (tezEvent.getEventType() == EventType.INPUT_FAILED_EVENT) {
        ((InputFailedEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
      } else if (tezEvent.getEventType() == EventType.ROOT_INPUT_DATA_INFORMATION_EVENT) {
        if (vertexState == VertexState.RUNNING
            || vertexState == VertexState.INITED) {
          // Only routed if vertex is still running
          eventHandler.handle(new VertexEventRouteEvent(
              this.getVertexId(), Collections.singletonList(tezEvent), true));
        }
        continue;
      }

      Vertex destVertex = getDAG().getVertex(sourceMeta.getEdgeVertexName());
      Edge destEdge = targetVertices.get(destVertex);
      if (destEdge == null) {
        throw new TezUncheckedException("Bad destination vertex: " +
            sourceMeta.getEdgeVertexName() + " for event vertex: " +
            getVertexId());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Routing recovered event"
            + ", vertex=" + logIdentifier
            + ", eventType=" + tezEvent.getEventType()
            + ", sourceInfo=" + sourceMeta
            + ", destinationVertex" + destVertex.getName());
      }
      eventHandler.handle(new VertexEventRouteEvent(destVertex
          .getVertexId(), Collections.singletonList(tezEvent), true));
    }
  }

  public static class TerminateDuringRecoverTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent vertexEvent) {
      LOG.info("Received a terminate during recovering, setting recovered"
          + " state to KILLED");
      vertex.recoveredState = VertexState.KILLED;
    }

  }

  public static class NullEdgeInitializedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent vertexEvent) {
      VertexEventNullEdgeInitialized event = (VertexEventNullEdgeInitialized) vertexEvent;
      Edge edge = event.getEdge();
      Vertex otherVertex = event.getVertex();
      Preconditions.checkState(
          vertex.getState() == VertexState.NEW
              || vertex.getState() == VertexState.INITIALIZING,
          "Unexpected state " + vertex.getState() + " for vertex: "
              + vertex.logIdentifier);
      Preconditions.checkState(
          (vertex.sourceVertices == null || vertex.sourceVertices.containsKey(otherVertex) ||
          vertex.targetVertices == null || vertex.targetVertices.containsKey(otherVertex)),
          "Not connected to vertex " + otherVertex.getName() + " from vertex: " + vertex.logIdentifier);
      LOG.info("Edge initialized for connection to vertex " + otherVertex.getName() + 
          " at vertex : " + vertex.logIdentifier);
      vertex.uninitializedEdges.remove(edge);
      if(vertex.getState() == VertexState.INITIALIZING && vertex.canInitVertex()) {
        // Vertex in Initialing state and can init. Do init.
        return VertexInitializedTransition.doTransition(vertex);
      }
      // Vertex is either New (waiting for sources to init) or its not ready to init (failed)
      return vertex.getState();
    }
  }

  public static class BufferDataRecoverTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent vertexEvent) {
      LOG.info("Received upstream event while still recovering"
          + ", vertexId=" + vertex.logIdentifier
          + ", vertexEventType=" + vertexEvent.getType());
      if (vertexEvent.getType().equals(VertexEventType.V_ROUTE_EVENT)) {
        VertexEventRouteEvent evt = (VertexEventRouteEvent) vertexEvent;
        vertex.pendingRouteEvents.addAll(evt.getEvents());
      } else if (vertexEvent.getType().equals(
          VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED)) {
        VertexEventSourceTaskAttemptCompleted evt =
            (VertexEventSourceTaskAttemptCompleted) vertexEvent;
        vertex.pendingReportedSrcCompletions.add(
            evt.getCompletionEvent().getTaskAttemptId());
      } else if (vertexEvent.getType().equals(
          VertexEventType.V_SOURCE_VERTEX_STARTED)) {
        VertexEventSourceVertexStarted startEvent =
            (VertexEventSourceVertexStarted) vertexEvent;
        int distanceFromRoot = startEvent.getSourceDistanceFromRoot() + 1;
        if(vertex.distanceFromRoot < distanceFromRoot) {
          vertex.distanceFromRoot = distanceFromRoot;
        }
        ++vertex.numStartedSourceVertices;
      } else if (vertexEvent.getType().equals(VertexEventType.V_INIT)) {
        ++vertex.numInitedSourceVertices;
      }
    }
  }


  public static class RecoverTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent vertexEvent) {
      VertexEventSourceVertexRecovered sourceRecoveredEvent =
          (VertexEventSourceVertexRecovered) vertexEvent;
      // Use distance from root from Recovery events as upstream vertices may not
      // send source vertex started event that is used to compute distance
      int distanceFromRoot = sourceRecoveredEvent.getSourceDistanceFromRoot() + 1;
      if(vertex.distanceFromRoot < distanceFromRoot) {
        vertex.distanceFromRoot = distanceFromRoot;
      }

      ++vertex.numRecoveredSourceVertices;

      switch (sourceRecoveredEvent.getSourceVertexState()) {
        case NEW:
          // Nothing to do
          break;
        case INITED:
          ++vertex.numInitedSourceVertices;
          break;
        case RUNNING:
        case SUCCEEDED:
          ++vertex.numInitedSourceVertices;
          ++vertex.numStartedSourceVertices;
          if (sourceRecoveredEvent.getCompletedTaskAttempts() != null) {
            vertex.pendingReportedSrcCompletions.addAll(
                sourceRecoveredEvent.getCompletedTaskAttempts());
          }
          break;
        case FAILED:
        case KILLED:
        case ERROR:
          // Nothing to do
          // Recover as if source vertices have not inited/started
          break;
        default:
          LOG.warn("Received invalid SourceVertexRecovered event"
              + ", vertex=" + vertex.logIdentifier
              + ", sourceVertex=" + sourceRecoveredEvent.getSourceVertexID()
              + ", sourceVertexState=" + sourceRecoveredEvent.getSourceVertexState());
          return vertex.finished(VertexState.ERROR);
      }

      if (vertex.numRecoveredSourceVertices !=
          vertex.getInputVerticesCount()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for source vertices to recover"
              + ", vertex=" + vertex.logIdentifier
              + ", numRecoveredSourceVertices=" + vertex.numRecoveredSourceVertices
              + ", totalSourceVertices=" + vertex.getInputVerticesCount());
        }
        return VertexState.RECOVERING;
      }


      // Complete recovery
      VertexState endState = VertexState.NEW;
      List<TezTaskAttemptID> completedTaskAttempts = Lists.newLinkedList();
      switch (vertex.recoveredState) {
        case NEW:
          // Drop all root events if not inited properly
          Iterator<TezEvent> iterator = vertex.recoveredEvents.iterator();
          while (iterator.hasNext()) {
            if (iterator.next().getEventType().equals(
                EventType.ROOT_INPUT_DATA_INFORMATION_EVENT)) {
              iterator.remove();
            }
          }
          // Trigger init if all sources initialized
          if (vertex.numInitedSourceVertices == vertex.getInputVerticesCount()) {
            vertex.eventHandler.handle(new VertexEvent(vertex.vertexId,
                VertexEventType.V_INIT));
          }
          if (vertex.numStartedSourceVertices == vertex.getInputVerticesCount()) {
            vertex.eventHandler.handle(new VertexEvent(vertex.vertexId,
                VertexEventType.V_START));
          }
          endState = VertexState.NEW;
          break;
        case INITED:
          vertex.vertexAlreadyInitialized = true;
          try {
            vertex.initializeCommitters();
          } catch (Exception e) {
            LOG.info("Failed to initialize committers, vertex="
                + vertex.logIdentifier, e);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.INIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }
          if (!vertex.setParallelism(0,
              null, vertex.recoveredSourceEdgeManagers, vertex.recoveredRootInputSpecUpdates, true)) {
            LOG.info("Failed to recover edge managers, vertex="
                + vertex.logIdentifier);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.INIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }
          // Recover tasks
          if (vertex.tasks != null) {
            for (Task task : vertex.tasks.values()) {
              vertex.eventHandler.handle(
                  new TaskEventRecoverTask(task.getTaskId()));
            }
          }
          if (vertex.numInitedSourceVertices != vertex.getInputVerticesCount()) {
            LOG.info("Vertex already initialized but source vertices have not"
                + " initialized"
                + ", vertexId=" + vertex.logIdentifier
                + ", numInitedSourceVertices=" + vertex.numInitedSourceVertices);
          } else {
            if (vertex.numStartedSourceVertices == vertex.getInputVerticesCount()) {
              vertex.eventHandler.handle(new VertexEvent(vertex.vertexId,
                VertexEventType.V_START));
            }
          }
          endState = VertexState.INITED;
          break;
        case RUNNING:
          vertex.tasksNotYetScheduled = false;
          // if commit in progress and desired state is not a succeeded one,
          // move to failed
          if (vertex.recoveryCommitInProgress) {
            LOG.info("Recovered vertex was in the middle of a commit"
                + ", failing Vertex=" + vertex.logIdentifier);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.COMMIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }
          try {
            vertex.initializeCommitters();
          } catch (Exception e) {
            LOG.info("Failed to initialize committers", e);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.INIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }
          if (!vertex.setParallelism(0, null, vertex.recoveredSourceEdgeManagers,
            vertex.recoveredRootInputSpecUpdates, true)) {
            LOG.info("Failed to recover edge managers");
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.INIT_FAILURE);
            endState = VertexState.FAILED;
            break;
          }
          assert vertex.tasks.size() == vertex.numTasks;
          if (vertex.tasks != null && vertex.numTasks != 0) {
            for (Task task : vertex.tasks.values()) {
              vertex.eventHandler.handle(
                  new TaskEventRecoverTask(task.getTaskId()));
            }
            vertex.vertexManager.onVertexStarted(vertex.pendingReportedSrcCompletions);
            endState = VertexState.RUNNING;
          } else {
            endState = VertexState.SUCCEEDED;
            vertex.finished(endState);
          }
          break;
        case SUCCEEDED:
        case FAILED:
        case KILLED:
          vertex.tasksNotYetScheduled = false;
          // recover tasks
          assert vertex.tasks.size() == vertex.numTasks;
          if (vertex.tasks != null  && vertex.numTasks != 0) {
            TaskState taskState = TaskState.KILLED;
            switch (vertex.recoveredState) {
              case SUCCEEDED:
                taskState = TaskState.SUCCEEDED;
                break;
              case KILLED:
                taskState = TaskState.KILLED;
                break;
              case FAILED:
                taskState = TaskState.FAILED;
                break;
            }
            for (Task task : vertex.tasks.values()) {
              vertex.eventHandler.handle(
                  new TaskEventRecoverTask(task.getTaskId(),
                      taskState));
            }
            // Wait for all tasks to recover and report back
            vertex.vertexManager.onVertexStarted(vertex.pendingReportedSrcCompletions);
            endState = VertexState.RUNNING;
          } else {
            endState = vertex.recoveredState;
            vertex.finished(endState);
          }
          break;
        default:
          LOG.warn("Invalid recoveredState found when trying to recover"
              + " vertex, recoveredState=" + vertex.recoveredState);
          vertex.finished(VertexState.ERROR);
          endState = VertexState.ERROR;
          break;
      }

      LOG.info("Recovered Vertex State"
          + ", vertexId=" + vertex.logIdentifier
          + ", state=" + endState
          + ", numInitedSourceVertices" + vertex.numInitedSourceVertices
          + ", numStartedSourceVertices=" + vertex.numStartedSourceVertices
          + ", numRecoveredSourceVertices=" + vertex.numRecoveredSourceVertices
          + ", tasksIsNull=" + (vertex.tasks == null)
          + ", numTasks=" + ( vertex.tasks == null ? 0 : vertex.tasks.size()));

      for (Entry<Vertex, Edge> entry : vertex.getOutputVertices().entrySet()) {
        vertex.eventHandler.handle(new VertexEventSourceVertexRecovered(
            entry.getKey().getVertexId(),
            vertex.vertexId, endState, completedTaskAttempts,
            vertex.getDistanceFromRoot()));
      }
      if (EnumSet.of(VertexState.RUNNING, VertexState.SUCCEEDED, VertexState.INITED)
          .contains(endState)) {
        // Send events downstream
        vertex.routeRecoveredEvents(endState, vertex.recoveredEvents);
        vertex.recoveredEvents.clear();
        if (!vertex.pendingRouteEvents.isEmpty()) {
          VertexImpl.ROUTE_EVENT_TRANSITION.transition(vertex,
              new VertexEventRouteEvent(vertex.getVertexId(),
                  vertex.pendingRouteEvents));
          vertex.pendingRouteEvents.clear();
        }
      } else {
        // Ensure no recovered events
        if (!vertex.recoveredEvents.isEmpty()) {
          throw new RuntimeException("Invalid Vertex state"
              + ", found non-zero recovered events in invalid state"
              + ", recoveredState=" + endState
              + ", recoveredEvents=" + vertex.recoveredEvents.size());
        }
      }
      return endState;
    }

  }

  public static class IgnoreInitInInitedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      LOG.info("Received event during INITED state"
          + ", vertex=" + vertex.logIdentifier
          + ", eventType=" + event.getType());
      if (!vertex.vertexAlreadyInitialized) {
        LOG.error("Vertex not initialized but in INITED state"
            + ", vertexId=" + vertex.logIdentifier);
        return vertex.finished(VertexState.ERROR);
      } else {
        return VertexState.INITED;
      }
    }
  }



  public static class InitTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexState vertexState = VertexState.NEW;
      vertex.numInitedSourceVertices++;
      // TODO fix this as part of TEZ-1008
      // Should have a different way to infer source vertices INITED
      // as compared to a recovery triggered INIT
      // In normal flow, upstream vertices send a V_INIT downstream to
      // trigger an init of the downstream vertex. In case of recovery,
      // upstream vertices may not send this event if they are already in a
      // RUNNING or completed state. Hence, recovering vertices may send
      // themselves a V_INIT to trigger a transition. Hence, the count may
      // go one over.
      if (vertex.sourceVertices == null || vertex.sourceVertices.isEmpty() ||
          (vertex.numInitedSourceVertices == vertex.sourceVertices.size()
            || vertex.numInitedSourceVertices == (vertex.sourceVertices.size()+1))) {
        vertexState = handleInitEvent(vertex, event);
        if (vertexState != VertexState.FAILED) {
          if (vertex.targetVertices != null && !vertex.targetVertices.isEmpty()) {
            for (Vertex target : vertex.targetVertices.keySet()) {
              vertex.getEventHandler().handle(new VertexEvent(target.getVertexId(),
                VertexEventType.V_INIT));
            }
          }
        }
      }
      return vertexState;
    }

    private VertexState handleInitEvent(VertexImpl vertex, VertexEvent event) {
      VertexState state = vertex.setupVertex();
      if (state.equals(VertexState.FAILED)) {
        return state;
      }
      // TODO move before to handle NEW state 
      if (vertex.targetVertices != null) {
        for (Edge e : vertex.targetVertices.values()) {
          if (e.getEdgeManager() == null) {
            Preconditions
                .checkState(
                    e.getEdgeProperty().getDataMovementType() == DataMovementType.CUSTOM,
                    "Null edge manager allowed only for custom edge. " + vertex.logIdentifier);
            vertex.uninitializedEdges.add(e);
          }
        }
      }
      if (vertex.sourceVertices != null) {
        for (Edge e : vertex.sourceVertices.values()) {
          if (e.getEdgeManager() == null) {
            Preconditions
                .checkState(
                    e.getEdgeProperty().getDataMovementType() == DataMovementType.CUSTOM,
                    "Null edge manager allowed only for custom edge. " + vertex.logIdentifier);
            vertex.uninitializedEdges.add(e);
          }
        }
      }
      
      // Create tasks based on initial configuration, but don't start them yet.
      if (vertex.numTasks == -1) {
        LOG.info("Num tasks is -1. Expecting VertexManager/InputInitializers/1-1 split"
            + " to set #tasks for the vertex " + vertex.getVertexId());

        if (vertex.inputsWithInitializers != null) {
          // Use DAGScheduler to arbitrate resources among vertices later
          vertex.rootInputInitializerManager = vertex.createRootInputInitializerManager(
              vertex.getDAG().getName(), vertex.getName(), vertex.getVertexId(),
              vertex.eventHandler, -1,
              vertex.appContext.getTaskScheduler().getNumClusterNodes(),
              vertex.getTaskResource(),
              vertex.appContext.getTaskScheduler().getTotalResources());
          List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
              inputList = Lists.newArrayListWithCapacity(vertex.inputsWithInitializers.size());
          for (String inputName : vertex.inputsWithInitializers) {
            inputList.add(vertex.rootInputDescriptors.get(inputName));
          }
          LOG.info("Vertex will initialize via inputInitializers "
              + vertex.logIdentifier + ". Starting root input initializers: "
              + vertex.inputsWithInitializers.size());
          vertex.rootInputInitializerManager.runInputInitializers(inputList);
          return VertexState.INITIALIZING;
        } else {
          boolean hasOneToOneUninitedSource = false;
          for (Map.Entry<Vertex, Edge> entry : vertex.sourceVertices.entrySet()) {
            if (entry.getValue().getEdgeProperty().getDataMovementType() == 
                DataMovementType.ONE_TO_ONE) {
              if (entry.getKey().getTotalTasks() == -1) {
                hasOneToOneUninitedSource = true;
                break;
              }
            }
          }
          if (hasOneToOneUninitedSource) {
            LOG.info("Vertex will initialize from 1-1 sources. " + vertex.logIdentifier);
            return VertexState.INITIALIZING;
          }
          if (vertex.vertexPlan.hasVertexManagerPlugin()) {
            LOG.info("Vertex will initialize via custom vertex manager. " + vertex.logIdentifier);
            return VertexState.INITIALIZING;
          }
          throw new TezUncheckedException(vertex.getVertexId() + 
          " has -1 tasks but does not have input initializers, " +
          "1-1 uninited sources or custom vertex manager to set it at runtime");
        }
      } else {
        LOG.info("Creating " + vertex.numTasks + " for vertex: " + vertex.logIdentifier);
        vertex.createTasks();
        if (vertex.inputsWithInitializers != null) {
          vertex.rootInputInitializerManager = vertex.createRootInputInitializerManager(
              vertex.getDAG().getName(), vertex.getName(), vertex.getVertexId(),
              vertex.eventHandler, vertex.getTotalTasks(),
              vertex.appContext.getTaskScheduler().getNumClusterNodes(),
              vertex.getTaskResource(),
              vertex.appContext.getTaskScheduler().getTotalResources());
          List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
          inputList = Lists.newArrayListWithCapacity(vertex.inputsWithInitializers.size());
          for (String inputName : vertex.inputsWithInitializers) {
            inputList.add(vertex.rootInputDescriptors.get(inputName));
          }
          LOG.info("Starting root input initializers: "
              + vertex.inputsWithInitializers.size());
          // special case when numTasks>0 and still we want to stay in initializing 
          // state. This is handled in RootInputInitializedTransition specially.
          vertex.initWaitsForRootInitializers = true;
          vertex.rootInputInitializerManager.runInputInitializers(inputList);
          return VertexState.INITIALIZING;
        }
        if (!vertex.uninitializedEdges.isEmpty()) {
          LOG.info("Vertex has uninitialized edges. " + vertex.logIdentifier);
          return VertexState.INITIALIZING;
        }
        LOG.info("Directly initializing vertex: " + vertex.logIdentifier);
        boolean isInitialized = vertex.initializeVertex();
        if (isInitialized) {
          return VertexState.INITED;
        } else {
          return VertexState.FAILED;
        }
      }
    }
  } // end of InitTransition

  @VisibleForTesting
  protected RootInputInitializerManager createRootInputInitializerManager(
      String dagName, String vertexName, TezVertexID vertexID,
      EventHandler eventHandler, int numTasks, int numNodes,
      Resource vertexTaskResource, Resource totalResource) {
    return new RootInputInitializerManager(this, appContext, this.dagUgi);
  }
  
  private boolean initializeVertexInInitializingState() {
    boolean isInitialized = initializeVertex();
    if (!isInitialized) {
      // Don't bother starting if the vertex state is failed.
      return false;
    }

    return true;
  }
  
  void startIfPossible() {
    if (startSignalPending) {
      // Trigger a start event to ensure route events are seen before
      // a start event.
      LOG.info("Triggering start event for vertex: " + logIdentifier +
          " with distanceFromRoot: " + distanceFromRoot );
      eventHandler.handle(new VertexEvent(vertexId,
          VertexEventType.V_START));
    }
  }

  public static class VertexInitializedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {
    
    static VertexState doTransition(VertexImpl vertex) {
      Preconditions.checkState(vertex.canInitVertex(), "Vertex: " + vertex.logIdentifier);
      boolean isInitialized = vertex.initializeVertexInInitializingState();
      if (!isInitialized) {
        return VertexState.FAILED;
      }

      vertex.startIfPossible();
      return VertexState.INITED;      
    }
    
    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      return doTransition(vertex);
    }
  }
  
  // present in most transitions so that the initializer thread can be shutdown properly
  public static class RootInputInitializedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventRootInputInitialized liInitEvent = (VertexEventRootInputInitialized) event;
      VertexState state = vertex.getState();
      if (state == VertexState.INITIALIZING) {
        vertex.vertexManager.onRootVertexInitialized(
            liInitEvent.getInputName(),
            vertex.getAdditionalInputs().get(liInitEvent.getInputName())
                .getIODescriptor(), liInitEvent.getEvents());
      }

      vertex.numInitializedInputs++;
      if (vertex.numInitializedInputs == vertex.inputsWithInitializers.size()) {
        // All inputs initialized, shutdown the initializer.
        vertex.rootInputInitializerManager.shutdown();
      }
      
      // done. check if we need to do the initialization
      if (vertex.getState() == VertexState.INITIALIZING && 
          vertex.initWaitsForRootInitializers) {
        // set the wait flag to false
        vertex.initWaitsForRootInitializers = false;
        // initialize vertex if possible and needed
        if (vertex.canInitVertex()) {
          Preconditions.checkState(vertex.numTasks >= 0, 
              "Parallelism should have been set by now for vertex: " + vertex.logIdentifier);
          return VertexInitializedTransition.doTransition(vertex);
        }
      }
      return vertex.getState();
    }
  }

  public static class OneToOneSourceSplitTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventOneToOneSourceSplit splitEvent = 
          (VertexEventOneToOneSourceSplit)event;
      TezVertexID originalSplitSource = splitEvent.getOriginalSplitSource();
      
      if (vertex.originalOneToOneSplitSource != null) {
        VertexState state = vertex.getState();
        Preconditions
            .checkState(
                (state == VertexState.INITIALIZING
                    || state == VertexState.INITED || state == VertexState.RUNNING),
                " Unexpected 1-1 split for vertex " + vertex.getVertexId()
                    + " in state " + vertex.getState() + " . Split in vertex "
                    + originalSplitSource + " sent by vertex "
                    + splitEvent.getSenderVertex() + " numTasks "
                    + splitEvent.getNumTasks());
        if (vertex.originalOneToOneSplitSource.equals(originalSplitSource)) {
          // ignore another split event that may have come from a different
          // path in the DAG. We have already split because of that source
          LOG.info("Ignoring split of vertex " + vertex.getVertexId() + 
              " because of split in vertex " + originalSplitSource + 
              " sent by vertex " + splitEvent.getSenderVertex() +
              " numTasks " + splitEvent.getNumTasks());
          return state;
        }
        // cannot split from multiple sources
        throw new TezUncheckedException("Vertex: " + vertex.getVertexId() + 
            " asked to split by: " + originalSplitSource + 
            " but was already split by:" + vertex.originalOneToOneSplitSource);
      }
      
      LOG.info("Splitting vertex " + vertex.getVertexId() + 
          " because of split in vertex " + originalSplitSource + 
          " sent by vertex " + splitEvent.getSenderVertex() +
          " numTasks " + splitEvent.getNumTasks());
      vertex.originalOneToOneSplitSource = originalSplitSource;
      vertex.setParallelism(splitEvent.getNumTasks(), null, null, null);
      if (vertex.getState() == VertexState.RUNNING || 
          vertex.getState() == VertexState.INITED) {
        return vertex.getState();
      } else {
        Preconditions.checkState(vertex.getState() == VertexState.INITIALIZING,
            " Unexpected 1-1 split for vertex " + vertex.getVertexId() +
                " in state " + vertex.getState() +
                " . Split in vertex " + originalSplitSource +
                " sent by vertex " + splitEvent.getSenderVertex() +
                " numTasks " + splitEvent.getNumTasks());
        return vertex.getState();
      }
    }
  }

  // Temporary to maintain topological order while starting vertices. Not useful
  // since there's not much difference between the INIT and RUNNING states.
  public static class SourceVertexStartedTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventSourceVertexStarted startEvent =
                                      (VertexEventSourceVertexStarted) event;
      int distanceFromRoot = startEvent.getSourceDistanceFromRoot() + 1;
      if(vertex.distanceFromRoot < distanceFromRoot) {
        vertex.distanceFromRoot = distanceFromRoot;
      }
      vertex.numStartedSourceVertices++;
      LOG.info("Source vertex started: " + startEvent.getSourceVertexId() +
          " for vertex: " + vertex.getVertexId() + " numStartedSources: " + 
          vertex.numStartedSourceVertices + " numSources: " + vertex.sourceVertices.size());
      
      if (vertex.numStartedSourceVertices < vertex.sourceVertices.size()) {
        LOG.info("Cannot start vertex: " + vertex.logIdentifier + " numStartedSources: "
            + vertex.numStartedSourceVertices + " numSources: " + vertex.sourceVertices.size());
        return;
      }
      
      // vertex meets external start dependency conditions. Save this signal in 
      // case we are not ready to start now and need to start later
      vertex.startSignalPending = true;
      
      if (vertex.getState() != VertexState.INITED) {
        // vertex itself is not ready to start. External dependencies have already
        // notified us.
        LOG.info("Cannot start vertex. Not in inited state. "
            + vertex.logIdentifier + " . VertesState: " + vertex.getState()
            + " numTasks: " + vertex.numTasks + " Num uninitialized edges: "
            + vertex.uninitializedEdges.size());
        return;
      }
      
      // vertex is inited and all dependencies are ready. Inited vertex means
      // parallelism must be set already and edges defined
      Preconditions.checkState(
          (vertex.numTasks >= 0 && vertex.uninitializedEdges.isEmpty()),
          "Cannot start vertex that is not completely defined. Vertex: "
              + vertex.logIdentifier + " numTasks: " + vertex.numTasks);
      
      vertex.startIfPossible();
    }
  }

  boolean canInitVertex() {
    if (numTasks >= 0 && uninitializedEdges.isEmpty() && !initWaitsForRootInitializers) {
      // vertex fully defined
      return true;
    }
    LOG.info("Cannot init vertex: " + logIdentifier + " numTasks: " + numTasks
        + " numUnitializedEdges: " + uninitializedEdges.size()
        + " numInitializedInputs: " + numInitializedInputs
        + " initWaitsForRootInitializers: " + initWaitsForRootInitializers);
    return false;
  }
  
  public static class StartWhileInitializingTransition implements 
    SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      // vertex state machine does not start itself in the initializing state
      // this start event can only come directly from the DAG. That means this 
      // is a top level vertex of the dag
      Preconditions.checkState(
          (vertex.sourceVertices == null || vertex.sourceVertices.isEmpty()),
          "Vertex: " + vertex.logIdentifier + " got invalid start event");
      vertex.startTimeRequested = vertex.clock.getTime();
      vertex.startSignalPending = true;
    }

  }

  public static class StartTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {
  
  @Override
  public VertexState transition(VertexImpl vertex, VertexEvent event) {
      Preconditions.checkState(vertex.getState() == VertexState.INITED, 
          "Unexpected state " + vertex.getState() + " for " + vertex.logIdentifier);
      vertex.startTimeRequested = vertex.clock.getTime();
      return vertex.startVertex();
    }
  }

  private VertexState startVertex() {
    Preconditions.checkState(getState() == VertexState.INITED, 
        "Vertex must be inited " + logIdentifier);

    startedTime = clock.getTime();
    vertexManager.onVertexStarted(pendingReportedSrcCompletions);
    pendingReportedSrcCompletions.clear();
    logJobHistoryVertexStartedEvent();

    // TODO: Metrics
    //job.metrics.runningJob(job);

    // default behavior is to start immediately. so send information about us
    // starting to downstream vertices. If the connections/structure of this
    // vertex is not fully defined yet then we could send this event later
    // when we are ready
    if (targetVertices != null) {
      for (Vertex targetVertex : targetVertices.keySet()) {
        eventHandler.handle(new VertexEventSourceVertexStarted(targetVertex
            .getVertexId(), getVertexId(), distanceFromRoot));
      }
    }

    // If we have no tasks, just transition to vertex completed
    if (this.numTasks == 0) {
      eventHandler.handle(new VertexEvent(
        this.vertexId, VertexEventType.V_COMPLETED));
    }
    return VertexState.RUNNING;
  }

  private void abortVertex(final VertexStatus.State finalState) {
    if (this.aborted.getAndSet(true)) {
      LOG.info("Ignoring multiple aborts for vertex: " + logIdentifier);
      return;
    }
    LOG.info("Invoking committer abort for vertex, vertexId=" + logIdentifier);
    if (outputCommitters != null) {
      try {
        dagUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() {
            for (Entry<String, OutputCommitter> entry : outputCommitters.entrySet()) {
              try {
                LOG.info("Invoking committer abort for output=" + entry.getKey() + ", vertexId="
                    + logIdentifier);
                entry.getValue().abortOutput(finalState);
              } catch (Exception e) {
                LOG.warn("Could not abort committer for output=" + entry.getKey() + ", vertexId="
                    + logIdentifier, e);
              }
            }
            return null;
          }
        });
      } catch (Exception e) {
        throw new TezUncheckedException("Unknown error while attempting VertexCommitter(s) abort", e);
      }
    }
    if (finishTime == 0) {
      setFinishTime();
    }
  }

  private void mayBeConstructFinalFullCounters() {
    // Calculating full-counters. This should happen only once for the vertex.
    synchronized (this.fullCountersLock) {
      if (this.fullCounters != null) {
        // Already constructed. Just return.
        return;
      }
      this.constructFinalFullcounters();
    }
  }

  @Private
  public void constructFinalFullcounters() {
    this.fullCounters = new TezCounters();
    this.vertexStats = new VertexStats();

    for (Task t : this.tasks.values()) {
      vertexStats.updateStats(t.getReport());
      TezCounters counters = t.getCounters();
      this.fullCounters.incrAllCounters(counters);
    }
  }

  private static class RootInputInitFailedTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventRootInputFailed fe = (VertexEventRootInputFailed) event;
      vertex.trySetTerminationCause(VertexTerminationCause.INIT_FAILURE);
      vertex.addDiagnostic("Vertex Input: " + fe.getInputName()
          + " initializer failed.");
      if (fe.getError() != null) {
        LOG.error("Vertex Input: " + fe.getInputName() + " initializer failed",
            fe.getError());
        if (fe.getError().getMessage() != null) {
          vertex.addDiagnostic(fe.getError().getMessage());
        }
      }
      if (vertex.rootInputInitializerManager != null) {
        vertex.rootInputInitializerManager.shutdown();
      }
      vertex.finished(VertexState.FAILED,
          VertexTerminationCause.ROOT_INPUT_INIT_FAILURE);
    }
  }

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class TerminateNewVertexTransition
  implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTermination vet = (VertexEventTermination) event;
      vertex.trySetTerminationCause(vet.getTerminationCause());
      vertex.setFinishTime();
      vertex.addDiagnostic("Vertex received Kill in NEW state.");
      vertex.finished(VertexState.KILLED);
    }
  }

  private static class TerminateInitedVertexTransition
  implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTermination vet = (VertexEventTermination) event;
      vertex.trySetTerminationCause(vet.getTerminationCause());
      vertex.abortVertex(VertexStatus.State.KILLED);
      vertex.addDiagnostic("Vertex received Kill in INITED state.");
      vertex.finished(VertexState.KILLED);
    }
  }

  private static class TerminateInitingVertexTransition extends TerminateInitedVertexTransition {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      super.transition(vertex, event);
      if (vertex.rootInputInitializerManager != null) {
        vertex.rootInputInitializerManager.shutdown();
      }
    }
  }

  private static class VertexKilledTransition
      implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      vertex.addDiagnostic("Vertex received Kill while in RUNNING state.");
      VertexEventTermination vet = (VertexEventTermination) event;
      VertexTerminationCause trigger = vet.getTerminationCause();
      switch(trigger){
        case DAG_KILL : vertex.tryEnactKill(trigger, TaskTerminationCause.DAG_KILL); break;
        case OWN_TASK_FAILURE: vertex.tryEnactKill(trigger, TaskTerminationCause.OTHER_TASK_FAILURE); break;
        case ROOT_INPUT_INIT_FAILURE:
        case COMMIT_FAILURE:
        case INVALID_NUM_OF_TASKS: 
        case INIT_FAILURE:
        case INTERNAL_ERROR:
        case OTHER_VERTEX_FAILURE: vertex.tryEnactKill(trigger, TaskTerminationCause.OTHER_VERTEX_FAILURE); break;
        default://should not occur
          throw new TezUncheckedException("VertexKilledTransition: event.terminationCause is unexpected: " + trigger);
      }

      // TODO: Metrics
      //job.metrics.endRunningJob(job);
    }
  }

  /**
   * Here, the Vertex is being told that one of it's source task-attempts
   * completed.
   */
  private static class SourceTaskAttemptCompletedEventTransition implements
  SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTaskAttemptCompleted completionEvent =
          ((VertexEventSourceTaskAttemptCompleted) event).getCompletionEvent();
      LOG.info("Source task attempt completed for vertex: " + vertex.getVertexId()
            + " attempt: " + completionEvent.getTaskAttemptId()
            + " with state: " + completionEvent.getTaskAttemptState()
            + " vertexState: " + vertex.getState());

      if (TaskAttemptStateInternal.SUCCEEDED.equals(completionEvent
          .getTaskAttemptState())) {
        vertex.numSuccessSourceAttemptCompletions++;
        if (vertex.getState() == VertexState.RUNNING) {
          vertex.vertexManager.onSourceTaskCompleted(completionEvent
              .getTaskAttemptId().getTaskID());
        } else {
          vertex.pendingReportedSrcCompletions.add(completionEvent.getTaskAttemptId());
        }
      }

    }
  }

  private static class TaskAttemptCompletedEventTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTaskAttemptCompleted completionEvent =
        ((VertexEventTaskAttemptCompleted) event);

      // If different tasks were connected to different destination vertices
      // then this would need to be sent via the edges
      // Notify all target vertices
      if (vertex.targetVertices != null) {
        for (Vertex targetVertex : vertex.targetVertices.keySet()) {
          vertex.eventHandler.handle(
              new VertexEventSourceTaskAttemptCompleted(
                  targetVertex.getVertexId(), completionEvent)
              );
        }
      }
    }
  }

  private static class TaskCompletedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      boolean forceTransitionToKillWait = false;
      vertex.completedTaskCount++;
      LOG.info("Num completed Tasks for " + vertex.logIdentifier + " : "
          + vertex.completedTaskCount);
      VertexEventTaskCompleted taskEvent = (VertexEventTaskCompleted) event;
      Task task = vertex.tasks.get(taskEvent.getTaskID());
      if (taskEvent.getState() == TaskState.SUCCEEDED) {
        taskSucceeded(vertex, task);
      } else if (taskEvent.getState() == TaskState.FAILED) {
        LOG.info("Failing vertex: " + vertex.logIdentifier + 
            " because task failed: " + taskEvent.getTaskID());
        vertex.tryEnactKill(VertexTerminationCause.OWN_TASK_FAILURE, TaskTerminationCause.OTHER_TASK_FAILURE);
        forceTransitionToKillWait = true;
        taskFailed(vertex, task);
      } else if (taskEvent.getState() == TaskState.KILLED) {
        taskKilled(vertex, task);
      }

      VertexState state = VertexImpl.checkVertexForCompletion(vertex);
      if(state == VertexState.RUNNING && forceTransitionToKillWait){
        return VertexState.TERMINATING;
      }

      return state;
    }

    private void taskSucceeded(VertexImpl vertex, Task task) {
      vertex.succeededTaskCount++;
      // TODO Metrics
      // job.metrics.completedTask(task);
    }

    private void taskFailed(VertexImpl vertex, Task task) {
      vertex.failedTaskCount++;
      vertex.addDiagnostic("Task failed"
        + ", taskId=" + task.getTaskId()
        + ", diagnostics=" + task.getDiagnostics());
      // TODO Metrics
      //vertex.metrics.failedTask(task);
    }

    private void taskKilled(VertexImpl vertex, Task task) {
      vertex.killedTaskCount++;
      // TODO Metrics
      //job.metrics.killedTask(task);
    }
  }

  private static class TaskRescheduledTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      //succeeded task is restarted back
      vertex.completedTaskCount--;
      vertex.succeededTaskCount--;
    }
  }
  
  private static class VertexNoTasksCompletedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      return VertexImpl.checkVertexForCompletion(vertex);
    }
  }
  
  private static class TaskCompletedAfterVertexSuccessTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {
    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTaskCompleted vEvent = (VertexEventTaskCompleted) event;
      VertexState finalState;
      VertexStatus.State finalStatus;
      String diagnosticMsg;
      if (vEvent.getState() == TaskState.FAILED) {
        finalState = VertexState.FAILED;
        finalStatus = VertexStatus.State.FAILED;
        diagnosticMsg = "Vertex " + vertex.logIdentifier +" failed as task " + vEvent.getTaskID() + 
          " failed after vertex succeeded.";
      } else {
        finalState = VertexState.ERROR;
        finalStatus = VertexStatus.State.ERROR;
        diagnosticMsg = "Vertex " + vertex.logIdentifier + " error as task " + vEvent.getTaskID() + 
            " completed with state " + vEvent.getState() + " after vertex succeeded.";
      }
      LOG.info(diagnosticMsg);
      vertex.addDiagnostic(diagnosticMsg);
      vertex.abortVertex(finalStatus);
      vertex.finished(finalState, VertexTerminationCause.OWN_TASK_FAILURE);
      return finalState;
    }
  }

  private static class TaskRescheduledAfterVertexSuccessTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      if (vertex.outputCommitters == null // no committer
          || vertex.outputCommitters.isEmpty() // no committer
          || !vertex.commitVertexOutputs) { // committer does not commit on vertex success
        LOG.info(vertex.getVertexId() + " back to running due to rescheduling "
            + ((VertexEventTaskReschedule)event).getTaskID());
        (new TaskRescheduledTransition()).transition(vertex, event);
        // inform the DAG that we are re-running
        vertex.eventHandler.handle(new DAGEventVertexReRunning(vertex.getVertexId()));
        return VertexState.RUNNING;
      }

      // terminate any running tasks
      String diagnosticMsg = vertex.getVertexId() + " failed due to post-commit rescheduling of "
          + ((VertexEventTaskReschedule)event).getTaskID();
      LOG.info(diagnosticMsg);
      vertex.tryEnactKill(VertexTerminationCause.OWN_TASK_FAILURE,
          TaskTerminationCause.OWN_TASK_FAILURE);
      vertex.addDiagnostic(diagnosticMsg);
      vertex.abortVertex(VertexStatus.State.FAILED);
      vertex.finished(VertexState.FAILED, VertexTerminationCause.OWN_TASK_FAILURE);
      return VertexState.FAILED;
    }
  }

  private void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }

  private static boolean isEventFromVertex(Vertex vertex,
      EventMetaData sourceMeta) {
    if (!sourceMeta.getTaskVertexName().equals(vertex.getName())) {
      return false;
    }
    return true;
  }

  private static void checkEventSourceMetadata(Vertex vertex,
      EventMetaData sourceMeta) {
    assert isEventFromVertex(vertex, sourceMeta);
  }

//  private static class RouteEventsWhileInitializingTransition implements
//      SingleArcTransition<VertexImpl, VertexEvent> {
//
//    @Override
//    public void transition(VertexImpl vertex, VertexEvent event) {
//      VertexEventRouteEvent re = (VertexEventRouteEvent) event;
//      // Store the events for post-init routing, since INIT state is when
//      // initial task parallelism will be set
//      vertex.pendingRouteEvents.addAll(re.getEvents());
//    }
//  }

  private static class RouteEventTransition  implements
  SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventRouteEvent rEvent = (VertexEventRouteEvent) event;
      boolean recovered = rEvent.isRecovered();
      List<TezEvent> tezEvents = rEvent.getEvents();

      if (vertex.getAppContext().isRecoveryEnabled()
          && !recovered
          && !tezEvents.isEmpty()) {
        List<TezEvent> dataMovementEvents =
            Lists.newArrayList();
        for (TezEvent tezEvent : tezEvents) {
          if (!isEventFromVertex(vertex, tezEvent.getSourceInfo())) {
            continue;
          }
          if  (tezEvent.getEventType().equals(EventType.COMPOSITE_DATA_MOVEMENT_EVENT)
            || tezEvent.getEventType().equals(EventType.DATA_MOVEMENT_EVENT)
            || tezEvent.getEventType().equals(EventType.ROOT_INPUT_DATA_INFORMATION_EVENT)) {
            dataMovementEvents.add(tezEvent);
          }
        }
        if (!dataMovementEvents.isEmpty()) {
          VertexDataMovementEventsGeneratedEvent historyEvent =
              new VertexDataMovementEventsGeneratedEvent(vertex.vertexId,
                  dataMovementEvents);
          vertex.appContext.getHistoryHandler().handle(
              new DAGHistoryEvent(vertex.getDAGId(), historyEvent));
        }
      }
      for(TezEvent tezEvent : tezEvents) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex: " + vertex.getName() + " routing event: "
              + tezEvent.getEventType()
              + " Recovered:" + recovered);
        }
        EventMetaData sourceMeta = tezEvent.getSourceInfo();
        switch(tezEvent.getEventType()) {
        case INPUT_FAILED_EVENT:
        case DATA_MOVEMENT_EVENT:
        case COMPOSITE_DATA_MOVEMENT_EVENT:
          {
            if (isEventFromVertex(vertex, sourceMeta)) {
              // event from this vertex. send to destination vertex
              TezTaskAttemptID srcTaId = sourceMeta.getTaskAttemptID();
              if (tezEvent.getEventType() == EventType.DATA_MOVEMENT_EVENT) {
                ((DataMovementEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
              } else if (tezEvent.getEventType() == EventType.COMPOSITE_DATA_MOVEMENT_EVENT) {
                ((CompositeDataMovementEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
              } else {
                ((InputFailedEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
              }
              Vertex destVertex = vertex.getDAG().getVertex(sourceMeta.getEdgeVertexName());
              Edge destEdge = vertex.targetVertices.get(destVertex);
              if (destEdge == null) {
                throw new TezUncheckedException("Bad destination vertex: " +
                    sourceMeta.getEdgeVertexName() + " for event vertex: " +
                    vertex.getVertexId());
              }
              vertex.eventHandler.handle(new VertexEventRouteEvent(destVertex
                  .getVertexId(), Collections.singletonList(tezEvent)));
            } else {
              // event not from this vertex. must have come from source vertex.
              // send to tasks
              if (vertex.tasksNotYetScheduled) {
                vertex.pendingTaskEvents.add(tezEvent);
              } else {
                Edge srcEdge = vertex.sourceVertices.get(vertex.getDAG().getVertex(
                    sourceMeta.getTaskVertexName()));
                if (srcEdge == null) {
                  throw new TezUncheckedException("Bad source vertex: " +
                      sourceMeta.getTaskVertexName() + " for destination vertex: " +
                      vertex.getVertexId());
                }
                srcEdge.sendTezEventToDestinationTasks(tezEvent);
              }
            }
          }
          break;
        case ROOT_INPUT_DATA_INFORMATION_EVENT:
          if (vertex.tasksNotYetScheduled) {
            vertex.pendingTaskEvents.add(tezEvent);
          } else {
            checkEventSourceMetadata(vertex, sourceMeta);
            InputDataInformationEvent riEvent = (InputDataInformationEvent) tezEvent
                .getEvent();
            TezTaskID targetTaskID = TezTaskID.getInstance(vertex.getVertexId(),
                riEvent.getTargetIndex());
            vertex.eventHandler.handle(new TaskEventAddTezEvent(targetTaskID, tezEvent));
          }
          break;
        case VERTEX_MANAGER_EVENT:
        {
          VertexManagerEvent vmEvent = (VertexManagerEvent) tezEvent.getEvent();
          Vertex target = vertex.getDAG().getVertex(vmEvent.getTargetVertexName());
          Preconditions.checkArgument(target != null,
              "Event sent to unkown vertex: " + vmEvent.getTargetVertexName());
          if (target == vertex) {
            vertex.vertexManager.onVertexManagerEventReceived(vmEvent);
          } else {
            checkEventSourceMetadata(vertex, sourceMeta);
            vertex.eventHandler.handle(new VertexEventRouteEvent(target
                .getVertexId(), Collections.singletonList(tezEvent)));
          }
        }
          break;
        case ROOT_INPUT_INITIALIZER_EVENT:
        {
          InputInitializerEvent riEvent = (InputInitializerEvent) tezEvent.getEvent();
          Vertex target = vertex.getDAG().getVertex(riEvent.getTargetVertexName());
          Preconditions.checkArgument(target != null,
              "Event sent to unkown vertex: " + riEvent.getTargetVertexName());
          if (target == vertex) {
            vertex.rootInputInitializerManager.handleInitializerEvent(riEvent);
          } else {
            checkEventSourceMetadata(vertex, sourceMeta);
            vertex.eventHandler.handle(new VertexEventRouteEvent(target.getVertexId(),
                Collections.singletonList(tezEvent)));
          }
        }
          break;
        case INPUT_READ_ERROR_EVENT:
          {
            checkEventSourceMetadata(vertex, sourceMeta);
            Edge srcEdge = vertex.sourceVertices.get(vertex.getDAG().getVertex(
                sourceMeta.getEdgeVertexName()));
            srcEdge.sendTezEventToSourceTasks(tezEvent);
          }
          break;
        case TASK_STATUS_UPDATE_EVENT:
          {
            checkEventSourceMetadata(vertex, sourceMeta);
            TaskStatusUpdateEvent sEvent =
                (TaskStatusUpdateEvent) tezEvent.getEvent();
            vertex.getEventHandler().handle(
                new TaskAttemptEventStatusUpdate(sourceMeta.getTaskAttemptID(),
                    sEvent));
          }
          break;
        case TASK_ATTEMPT_COMPLETED_EVENT:
          {
            checkEventSourceMetadata(vertex, sourceMeta);
            vertex.getEventHandler().handle(
                new TaskAttemptEvent(sourceMeta.getTaskAttemptID(),
                    TaskAttemptEventType.TA_DONE));
          }
          break;
        case TASK_ATTEMPT_FAILED_EVENT:
          {
            checkEventSourceMetadata(vertex, sourceMeta);
            TaskAttemptFailedEvent taskFailedEvent =
                (TaskAttemptFailedEvent) tezEvent.getEvent();
            vertex.getEventHandler().handle(
                new TaskAttemptEventAttemptFailed(sourceMeta.getTaskAttemptID(),
                    TaskAttemptEventType.TA_FAILED,
                    "Error: " + taskFailedEvent.getDiagnostics()));
          }
          break;
        default:
          throw new TezUncheckedException("Unhandled tez event type: "
              + tezEvent.getEventType());
        }
      }
    }
  }

  private static class InternalErrorTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      LOG.error("Invalid event " + event.getType() + " on Vertex "
          + vertex.getVertexId());
      vertex.eventHandler.handle(new DAGEventDiagnosticsUpdate(
          vertex.getDAGId(), "Invalid event " + event.getType()
          + " on Vertex " + vertex.getVertexId()));
      vertex.setFinishTime();
      vertex.finished(VertexState.ERROR);
    }
  }

  @Override
  public void setInputVertices(Map<Vertex, Edge> inVertices) {
    this.sourceVertices = inVertices;
  }

  @Override
  public void setOutputVertices(Map<Vertex, Edge> outVertices) {
    this.targetVertices = outVertices;
  }

  @Override
  public void setAdditionalInputs(List<RootInputLeafOutputProto> inputs) {
    this.rootInputDescriptors = Maps.newHashMapWithExpectedSize(inputs.size());
    for (RootInputLeafOutputProto input : inputs) {

      InputDescriptor id = DagTypeConverters
          .convertInputDescriptorFromDAGPlan(input.getIODescriptor());

      this.rootInputDescriptors
          .put(
              input.getName(),
              new RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>(
                  input.getName(), id,
                  input.hasControllerDescriptor() ? DagTypeConverters
                      .convertInputInitializerDescriptorFromDAGPlan(input
                          .getControllerDescriptor()) : null));
      this.rootInputSpecs.put(input.getName(), DEFAULT_ROOT_INPUT_SPECS);
    }
  }

  @Nullable
  @Override
  public Map<String, OutputCommitter> getOutputCommitters() {
    return outputCommitters;
  }

  @Nullable
  @Private
  @VisibleForTesting
  public OutputCommitter getOutputCommitter(String outputName) {
    if (this.outputCommitters != null) {
      return outputCommitters.get(outputName);
    }
    return null;
  }

  @Override
  public void setAdditionalOutputs(List<RootInputLeafOutputProto> outputs) {
    LOG.info("setting additional outputs for vertex " + this.vertexName);
    this.additionalOutputs = Maps.newHashMapWithExpectedSize(outputs.size());
    this.outputCommitters = Maps.newHashMapWithExpectedSize(outputs.size());
    for (RootInputLeafOutputProto output : outputs) {
      OutputDescriptor od = DagTypeConverters
          .convertOutputDescriptorFromDAGPlan(output.getIODescriptor());

      this.additionalOutputs
          .put(
              output.getName(),
              new RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>(
                  output.getName(), od,
                  output.hasControllerDescriptor() ? DagTypeConverters
                      .convertOutputCommitterDescriptorFromDAGPlan(output
                          .getControllerDescriptor()) : null));
      OutputSpec outputSpec = new OutputSpec(output.getName(), od, 0);
      additionalOutputSpecs.add(outputSpec);
    }
  }

  @Nullable
  @Override
  public Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
    getAdditionalInputs() {
    return this.rootInputDescriptors;
  }

  @Nullable
  @Override
  public Map<String, RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> 
    getAdditionalOutputs() {
    return this.additionalOutputs;
  }

  @Override
  public int compareTo(Vertex other) {
    return this.vertexId.compareTo(other.getVertexId());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Vertex other = (Vertex) obj;
    return this.vertexId.equals(other.getVertexId());
  }

  @Override
  public int hashCode() {
    final int prime = 11239;
    return prime + prime * this.vertexId.hashCode();
  }

  @Override
  public Map<Vertex, Edge> getInputVertices() {
    return Collections.unmodifiableMap(this.sourceVertices);
  }

  @Override
  public Map<Vertex, Edge> getOutputVertices() {
    return Collections.unmodifiableMap(this.targetVertices);
  }

  @Override
  public int getInputVerticesCount() {
    return this.sourceVertices.size();
  }

  @Override
  public int getOutputVerticesCount() {
    return this.targetVertices.size();
  }

  @Override
  public ProcessorDescriptor getProcessorDescriptor() {
    return processorDescriptor;
  }

  @Override
  public DAG getDAG() {
    return appContext.getCurrentDAG();
  }

  private TezDAGID getDAGId() {
    return getDAG().getID();
  }

  public Resource getTaskResource() {
    return taskResource;
  }

  @VisibleForTesting
  String getProcessorName() {
    return this.processorDescriptor.getClassName();
  }

  @VisibleForTesting
  String getJavaOpts() {
    return this.javaOpts;
  }

  @VisibleForTesting
  TaskLocationHint[] getTaskLocationHints() {
    return taskLocationHints;
  }

  // TODO Eventually remove synchronization.
  @Override
  public synchronized List<InputSpec> getInputSpecList(int taskIndex) {
    inputSpecList = new ArrayList<InputSpec>(this.getInputVerticesCount()
        + (rootInputDescriptors == null ? 0 : rootInputDescriptors.size()));
    if (rootInputDescriptors != null) {
      for (Entry<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
           rootInputDescriptorEntry : rootInputDescriptors.entrySet()) {
        inputSpecList.add(new InputSpec(rootInputDescriptorEntry.getKey(),
            rootInputDescriptorEntry.getValue().getIODescriptor(), rootInputSpecs.get(
                rootInputDescriptorEntry.getKey()).getNumPhysicalInputsForWorkUnit(taskIndex)));
      }
    }
    for (Entry<Vertex, Edge> entry : this.getInputVertices().entrySet()) {
      InputSpec inputSpec = entry.getValue().getDestinationSpec(taskIndex);
      if (LOG.isDebugEnabled()) {
        LOG.debug("For vertex : " + this.getName()
            + ", Using InputSpec : " + inputSpec);
      }
      // TODO DAGAM This should be based on the edge type.
      inputSpecList.add(inputSpec);
    }
    return inputSpecList;
  }

  // TODO Eventually remove synchronization.
  @Override
  public synchronized List<OutputSpec> getOutputSpecList(int taskIndex) {
    if (this.outputSpecList == null) {
      outputSpecList = new ArrayList<OutputSpec>(this.getOutputVerticesCount()
          + this.additionalOutputSpecs.size());
      outputSpecList.addAll(additionalOutputSpecs);
      for (Entry<Vertex, Edge> entry : this.getOutputVertices().entrySet()) {
        OutputSpec outputSpec = entry.getValue().getSourceSpec(taskIndex);
        outputSpecList.add(outputSpec);
      }
    }
    return outputSpecList;
  }
  
  //TODO Eventually remove synchronization.
  @Override
  public synchronized List<GroupInputSpec> getGroupInputSpecList(int taskIndex) {
    return groupInputSpecList;
  }
  
  @Override
  public synchronized void addSharedOutputs(Set<String> outputs) {
    this.sharedOutputs.addAll(outputs);
  }
  
  @Override
  public synchronized Set<String> getSharedOutputs() {
    return this.sharedOutputs;
  }

  @VisibleForTesting
  VertexManager getVertexManager() {
    return this.vertexManager;
  }

  private static void logLocationHints(String vertexName,
      VertexLocationHint locationHint) {
    if (locationHint == null) {
      LOG.debug("No Vertex LocationHint specified for vertex=" + vertexName);
      return;
    }
    Multiset<String> hosts = HashMultiset.create();
    Multiset<String> racks = HashMultiset.create();
    int counter = 0;
    for (TaskLocationHint taskLocationHint : locationHint
        .getTaskLocationHints()) {
      StringBuilder sb = new StringBuilder();
      if (taskLocationHint.getDataLocalHosts() == null) {
        sb.append("No Hosts");
      } else {
        sb.append("Hosts: ");
        for (String host : taskLocationHint.getDataLocalHosts()) {
          hosts.add(host);
          sb.append(host).append(", ");
        }
      }
      if (taskLocationHint.getRacks() == null) {
        sb.append("No Racks");
      } else {
        sb.append("Racks: ");
        for (String rack : taskLocationHint.getRacks()) {
          racks.add(rack);
          sb.append(rack).append(", ");
        }
      }
      LOG.debug("Vertex: " + vertexName + ", Location: "
          + counter + " : " + sb.toString());
      counter++;
    }

    LOG.debug("Vertex: " + vertexName + ", Host Counts");
    for (Multiset.Entry<String> host : hosts.entrySet()) {
      LOG.debug("Vertex: " + vertexName + ", host: " + host.toString());
    }

    LOG.debug("Vertex: " + vertexName + ", Rack Counts");
    for (Multiset.Entry<String> rack : racks.entrySet()) {
      LOG.debug("Vertex: " + vertexName + ", rack: " + rack.toString());
    }
  }
}
