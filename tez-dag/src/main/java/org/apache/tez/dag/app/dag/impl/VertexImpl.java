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
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.BitSet;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.LimitExceededException;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.io.NonSyncByteArrayInputStream;
import org.apache.tez.common.io.NonSyncByteArrayOutputStream;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.Scope;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.api.event.VertexStateUpdateParallelismUpdated;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.RecoveryParser.VertexRecoveryData;
import org.apache.tez.dag.app.TaskAttemptEventInfo;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.RootInputInitializerManager;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TaskTerminationCause;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.CallableEvent;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGEventVertexReRunning;
import org.apache.tez.dag.app.dag.event.SpeculatorEvent;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventScheduleTask;
import org.apache.tez.dag.app.dag.event.TaskEventTermination;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventCommitCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventInputDataInformation;
import org.apache.tez.dag.app.dag.event.VertexEventManagerUserCodeError;
import org.apache.tez.dag.app.dag.event.VertexEventNullEdgeInitialized;
import org.apache.tez.dag.app.dag.event.VertexEventRecoverVertex;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputInitialized;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventSourceTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventSourceVertexStarted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventTermination;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl.VertexGroupInfo;
import org.apache.tez.dag.app.dag.impl.Edge.PendingEventRouteMetadata;
import org.apache.tez.dag.app.dag.speculation.legacy.LegacySpeculator;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TaskAttemptIdentifierImpl;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TaskSpecificLaunchCmdOption;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.InputStatistics;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.OutputStatistics;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.VertexStatistics;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.CustomProcessorEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/** Implementation of Vertex interface. Maintains the state machines of Vertex.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class VertexImpl implements org.apache.tez.dag.app.dag.Vertex, EventHandler<VertexEvent> {

  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  private static final Logger LOG = LoggerFactory.getLogger(VertexImpl.class);

  //final fields
  private final Clock clock;

  private final Lock readLock;
  private final Lock writeLock;
  private final TaskCommunicatorManagerInterface taskCommunicatorManagerInterface;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private final Object tasksSyncHandle = new Object();

  private final EventHandler eventHandler;
  // TODO Metrics
  //private final MRAppMetrics metrics;
  private final AppContext appContext;
  private final DAG dag;
  private final VertexRecoveryData recoveryData;
  private List<TezEvent> initGeneratedEvents = new ArrayList<TezEvent>();
  // set it to be true when setParallelism is called(used for recovery) 
  private boolean setParallelismCalledFlag = false;

  private boolean lazyTasksCopyNeeded = false;
  // must be a linked map for ordering
  volatile LinkedHashMap<TezTaskID, Task> tasks = new LinkedHashMap<TezTaskID, Task>();
  private Object fullCountersLock = new Object();
  private TezCounters counters = new TezCounters();
  private TezCounters fullCounters = null;
  private TezCounters cachedCounters = null;
  private long cachedCountersTimestamp = 0;
  private Resource taskResource;

  // Merged/combined vertex level config
  private Configuration vertexConf;
  // Vertex specific configs only ( include the dag specific configs too )
  // Useful when trying to serialize only the diff from global configs
  @VisibleForTesting
  Configuration vertexOnlyConf;
  
  private final boolean isSpeculationEnabled;

  @VisibleForTesting
  final int taskSchedulerIdentifier;
  @VisibleForTesting
  final int containerLauncherIdentifier;
  @VisibleForTesting
  final int taskCommunicatorIdentifier;

  final ServicePluginInfo servicePluginInfo;


  private final float maxFailuresPercent;
  private boolean logSuccessDiagnostics = false;

  private final VertexConfigImpl vertexContextConfig;
  //fields initialized in init

  @VisibleForTesting
  int numStartedSourceVertices = 0;
  @VisibleForTesting
  int numInitedSourceVertices = 0;
  @VisibleForTesting
  int numRecoveredSourceVertices = 0;

  private int distanceFromRoot = 0;

  private final List<String> diagnostics = new ArrayList<String>();

  protected final StateChangeNotifier stateChangeNotifier;
  
  //task/attempt related datastructures
  @VisibleForTesting
  int numSuccessSourceAttemptCompletions = 0;

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
  private static final CommitCompletedTransition
      COMMIT_COMPLETED_TRANSITION =
          new CommitCompletedTransition();
  private static final VertexStateChangedCallback STATE_CHANGED_CALLBACK =
      new VertexStateChangedCallback();

  @VisibleForTesting
  final List<TezEvent> pendingInitializerEvents = new LinkedList<TezEvent>();

  @VisibleForTesting
  final List<VertexManagerEvent> pendingVmEvents = new LinkedList<>();
  
  LegacySpeculator speculator;

  @VisibleForTesting
  Map<String, ListenableFuture<Void>> commitFutures = new ConcurrentHashMap<String, ListenableFuture<Void>>();

  protected static final
    StateMachineFactory<VertexImpl, VertexState, VertexEventType, VertexEvent>
       stateMachineFactory
     = new StateMachineFactory<VertexImpl, VertexState, VertexEventType, VertexEvent>
              (VertexState.NEW)

          // Transitions from NEW state
          .addTransition
              (VertexState.NEW,
                  EnumSet.of(VertexState.NEW, VertexState.INITED,
                      VertexState.INITIALIZING, VertexState.FAILED, VertexState.KILLED),
                  VertexEventType.V_INIT,
                  new InitTransition())
          .addTransition(VertexState.NEW,
                EnumSet.of(VertexState.NEW),
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  new NullEdgeInitializedTransition())
          .addTransition(VertexState.NEW,
                EnumSet.of(VertexState.NEW),
                VertexEventType.V_ROUTE_EVENT,
                ROUTE_EVENT_TRANSITION)
          .addTransition(VertexState.NEW,
                EnumSet.of(VertexState.NEW),
                VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition
              (VertexState.NEW,
                  EnumSet.of(VertexState.NEW,
                      VertexState.SUCCEEDED, VertexState.FAILED,
                      VertexState.KILLED, VertexState.ERROR),
                  VertexEventType.V_RECOVER,
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

          // Transitions from INITIALIZING state
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITIALIZING, VertexState.INITED,
                  VertexState.FAILED),
              VertexEventType.V_ROOT_INPUT_INITIALIZED,
              new RootInputInitializedTransition())
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITIALIZING, VertexState.INITED,
                  VertexState.FAILED),
              VertexEventType.V_INPUT_DATA_INFORMATION,
              new InputDataInformationTransition())
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITED, VertexState.FAILED),
              VertexEventType.V_READY_TO_INIT,
              new VertexInitializedTransition())
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.FAILED),
              VertexEventType.V_ROOT_INPUT_FAILED,
              new RootInputInitFailedTransition())
          .addTransition(VertexState.INITIALIZING, VertexState.INITIALIZING,
              VertexEventType.V_START,
              new StartWhileInitializingTransition())
          .addTransition(VertexState.INITIALIZING, VertexState.INITIALIZING,
              VertexEventType.V_SOURCE_VERTEX_STARTED,
              new SourceVertexStartedTransition())
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITIALIZING),
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.INITIALIZING,
              EnumSet.of(VertexState.INITIALIZING, VertexState.FAILED),
              VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)
          .addTransition(VertexState.INITIALIZING, EnumSet.of(VertexState.FAILED),
              VertexEventType.V_MANAGER_USER_CODE_ERROR,
              new VertexManagerUserCodeErrorTransition())
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
          .addTransition(VertexState.INITED,
              EnumSet.of(VertexState.FAILED),
              VertexEventType.V_ROOT_INPUT_FAILED,
              new RootInputInitFailedTransition())
          .addTransition(VertexState.INITED, VertexState.INITED,
              VertexEventType.V_SOURCE_VERTEX_STARTED,
              new SourceVertexStartedTransition())
          .addTransition(VertexState.INITED,
              EnumSet.of(VertexState.INITED),
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.INITED,
              EnumSet.of(VertexState.RUNNING, VertexState.INITED, VertexState.TERMINATING),
              VertexEventType.V_START,
              new StartTransition())
          .addTransition(VertexState.INITED,
              EnumSet.of(VertexState.INITED, VertexState.FAILED),
              VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)
          .addTransition(VertexState.INITED, VertexState.KILLED,
              VertexEventType.V_TERMINATE,
              new TerminateInitedVertexTransition())
          .addTransition(VertexState.INITED, EnumSet.of(VertexState.FAILED),
              VertexEventType.V_MANAGER_USER_CODE_ERROR,
              new VertexManagerUserCodeErrorTransition())
          .addTransition(VertexState.INITED, VertexState.ERROR,
              VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(VertexState.RUNNING,
              EnumSet.of(VertexState.TERMINATING),
              VertexEventType.V_ROOT_INPUT_FAILED,
              new RootInputInitFailedTransition())
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.RUNNING,
              EnumSet.of(VertexState.RUNNING, VertexState.TERMINATING),
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition
              (VertexState.RUNNING,
              EnumSet.of(VertexState.RUNNING,
                  VertexState.COMMITTING,
                  VertexState.SUCCEEDED, VertexState.TERMINATING, VertexState.FAILED,
                  VertexState.ERROR),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition(VertexState.RUNNING, VertexState.TERMINATING,
              VertexEventType.V_TERMINATE,
              new VertexKilledTransition())
          .addTransition(VertexState.RUNNING, EnumSet.of(VertexState.TERMINATING),
              VertexEventType.V_MANAGER_USER_CODE_ERROR,
              new VertexManagerUserCodeErrorTransition())
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
              EnumSet.of(VertexState.RUNNING, VertexState.TERMINATING),
              VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)

          // Transitions from COMMITTING state.
          .addTransition(
              VertexState.COMMITTING,
              EnumSet.of(VertexState.COMMITTING, VertexState.TERMINATING,
                  VertexState.SUCCEEDED, VertexState.FAILED),
              VertexEventType.V_COMMIT_COMPLETED,
              COMMIT_COMPLETED_TRANSITION)
          .addTransition(
              VertexState.COMMITTING,
              VertexState.TERMINATING,
              VertexEventType.V_TERMINATE,
              new VertexKilledWhileCommittingTransition()) 
          .addTransition(
              VertexState.COMMITTING,
              VertexState.ERROR,
              VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(
              VertexState.COMMITTING,
              EnumSet.of(VertexState.COMMITTING, VertexState.TERMINATING),
              VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)
          .addTransition(
              VertexState.COMMITTING,
              VertexState.TERMINATING,
              VertexEventType.V_TASK_RESCHEDULED,
              new TaskRescheduledWhileCommittingTransition())
          .addTransition(VertexState.COMMITTING,
              EnumSet.of(VertexState.TERMINATING),
              VertexEventType.V_MANAGER_USER_CODE_ERROR,
              new VertexManagerUserCodeErrorTransition())
   
          // Transitions from TERMINATING state.
          .addTransition
              (VertexState.TERMINATING,
              EnumSet.of(VertexState.TERMINATING, VertexState.KILLED, VertexState.FAILED, VertexState.ERROR),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition(
              VertexState.TERMINATING,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(
              VertexState.TERMINATING,
              EnumSet.of(VertexState.TERMINATING, VertexState.FAILED, VertexState.KILLED, VertexState.ERROR),
              VertexEventType.V_COMMIT_COMPLETED,
              COMMIT_COMPLETED_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.TERMINATING, VertexState.TERMINATING,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_MANAGER_USER_CODE_ERROR,
                  VertexEventType.V_ROOT_INPUT_FAILED,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_COMPLETED))

          // Transitions from SUCCEEDED state
          .addTransition(
              VertexState.SUCCEEDED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(VertexState.SUCCEEDED,
              EnumSet.of(VertexState.RUNNING, VertexState.FAILED),
              VertexEventType.V_TASK_RESCHEDULED,
              new TaskRescheduledAfterVertexSuccessTransition())

          .addTransition(
              VertexState.SUCCEEDED,
              EnumSet.of(VertexState.SUCCEEDED, VertexState.FAILED),
              // accumulate these in case we get restarted
              VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)
          .addTransition(
              VertexState.SUCCEEDED,
              EnumSet.of(VertexState.FAILED, VertexState.ERROR),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedAfterVertexSuccessTransition())
          // Ignore-able events
          .addTransition(VertexState.SUCCEEDED, VertexState.SUCCEEDED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_ROOT_INPUT_FAILED,
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
                  VertexEventType.V_MANAGER_USER_CODE_ERROR,
                  VertexEventType.V_ROOT_INPUT_FAILED,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_START,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_INPUT_DATA_INFORMATION))

          // Transitions from KILLED state
          .addTransition(
              VertexState.KILLED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.KILLED, VertexState.KILLED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_MANAGER_USER_CODE_ERROR,
                  VertexEventType.V_ROOT_INPUT_FAILED,
                  VertexEventType.V_INIT,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_START,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_INPUT_DATA_INFORMATION))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              VertexState.ERROR,
              VertexState.ERROR,
              EnumSet.of(VertexEventType.V_INIT,
                  VertexEventType.V_ROOT_INPUT_FAILED,
                  VertexEventType.V_SOURCE_VERTEX_STARTED,
                  VertexEventType.V_START,
                  VertexEventType.V_ROUTE_EVENT,
                  VertexEventType.V_TERMINATE,
                  VertexEventType.V_MANAGER_USER_CODE_ERROR,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_INTERNAL_ERROR,
                  VertexEventType.V_ROOT_INPUT_INITIALIZED,
                  VertexEventType.V_NULL_EDGE_INITIALIZED,
                  VertexEventType.V_INPUT_DATA_INFORMATION))
          // create the topology tables
          .installTopology();

  private void augmentStateMachine() {
    stateMachine
        .registerStateEnteredCallback(VertexState.SUCCEEDED,
            STATE_CHANGED_CALLBACK)
        .registerStateEnteredCallback(VertexState.FAILED,
            STATE_CHANGED_CALLBACK)
        .registerStateEnteredCallback(VertexState.KILLED,
            STATE_CHANGED_CALLBACK)
        .registerStateEnteredCallback(VertexState.RUNNING,
            STATE_CHANGED_CALLBACK)
        .registerStateEnteredCallback(VertexState.INITIALIZING,
            STATE_CHANGED_CALLBACK);;
  }

  private final StateMachineTez<VertexState, VertexEventType, VertexEvent, VertexImpl> stateMachine;

  //changing fields while the vertex is running
  @VisibleForTesting
  int numTasks;
  @VisibleForTesting
  int completedTaskCount = 0;
  @VisibleForTesting
  int succeededTaskCount = 0;
  @VisibleForTesting
  int failedTaskCount = 0;
  @VisibleForTesting
  int killedTaskCount = 0;

  // Both failed and killed task attempt counts are incremented via direct calls
  // and not via state machine changes as they always increase. In no situation, does
  // the counter need to be reset or changed back as failed attempts never go back to succeeded.
  // Likewise for killed attempts.
  // The same cannot apply to succeeded task attempts if they are tracked as they might be
  // subsequently declared as failed.
  @VisibleForTesting
  AtomicInteger failedTaskAttemptCount = new AtomicInteger(0);
  @VisibleForTesting
  AtomicInteger killedTaskAttemptCount = new AtomicInteger(0);

  @VisibleForTesting
  long initTimeRequested; // Time at which INIT request was received.
  @VisibleForTesting
  long initedTime; // Time when entering state INITED
  @VisibleForTesting
  long startTimeRequested; // Time at which START request was received.
  @VisibleForTesting
  long startedTime; // Time when entering state STARTED
  @VisibleForTesting
  long finishTime;
  long firstTaskStartTime = -1L;
  Object firstTaskStartTimeLock = new Object();
  private float progress;

  private final TezVertexID vertexId;  //runtime assigned id.
  private final VertexPlan vertexPlan;
  private boolean initWaitsForRootInitializers = false;

  private final String vertexName;
  private final ProcessorDescriptor processorDescriptor;
  
  private boolean vertexToBeReconfiguredByManager = false;
  final AtomicBoolean vmIsInitialized = new AtomicBoolean(false);
  final AtomicBoolean completelyConfiguredSent = new AtomicBoolean(false);

  private final AtomicBoolean internalErrorTriggered = new AtomicBoolean(false);

  @VisibleForTesting
  Map<Vertex, Edge> sourceVertices;
  private Map<Vertex, Edge> targetVertices;
  Set<Edge> uninitializedEdges = Sets.newHashSet();
  // using a linked hash map to conveniently map edge names to a contiguous index
  LinkedHashMap<String, Integer> ioIndices = Maps.newLinkedHashMap();

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
  @VisibleForTesting
  int numInitializerCompletionsHandled = 0;
  private boolean startSignalPending = false;
  // We may always store task events in the vertex for scalability
  List<TezEvent> pendingTaskEvents = Lists.newLinkedList();
  private boolean tasksNotYetScheduled = true;
  // must be a random access structure
  
  private final List<EventInfo> onDemandRouteEvents = Lists.newArrayListWithCapacity(1000);
  // Do not send any events if attempt is failed due to INPUT_FAILED_EVENTS.
  private final Set<TezTaskAttemptID> failedTaskAttemptIDs = Sets.newHashSet();
  private final ReadWriteLock onDemandRouteEventsReadWriteLock = new ReentrantReadWriteLock();
  private final Lock onDemandRouteEventsReadLock = onDemandRouteEventsReadWriteLock.readLock();
  private final Lock onDemandRouteEventsWriteLock = onDemandRouteEventsReadWriteLock.writeLock();
  
  List<TezEvent> pendingRouteEvents = new LinkedList<TezEvent>();
  List<TezTaskAttemptID> pendingReportedSrcCompletions = Lists.newLinkedList();


  private RootInputInitializerManager rootInputInitializerManager;

  VertexManager vertexManager;

  private final UserGroupInformation dagUgi;

  private AtomicBoolean committed = new AtomicBoolean(false);
  private AtomicBoolean aborted = new AtomicBoolean(false);
  private AtomicBoolean commitCanceled = new AtomicBoolean(false);
  private boolean commitVertexOutputs = false;

  private Map<String, VertexGroupInfo> dagVertexGroups;

  private TaskLocationHint taskLocationHints[];
  private Map<String, LocalResource> localResources;
  private final Map<String, String> environment;
  private final Map<String, String> environmentTaskSpecific;
  private final String javaOptsTaskSpecific;
  private final String javaOpts;
  private final ContainerContext containerContext;
  private VertexTerminationCause terminationCause;

  private String logIdentifier;

  private VertexStats vertexStats = null;

  private final TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOpts;

  @VisibleForTesting
  VertexStatisticsImpl completedTasksStatsCache;

  static class EventInfo {
    final TezEvent tezEvent;
    final Edge eventEdge;
    final int eventTaskIndex;
    boolean isObsolete = false;
    EventInfo(TezEvent tezEvent, Edge eventEdge, int eventTaskIndex) {
      this.tezEvent = tezEvent;
      this.eventEdge = eventEdge;
      this.eventTaskIndex = eventTaskIndex;
    }
  }

  private VertexStatisticsImpl finalStatistics;

  
  static class IOStatisticsImpl extends org.apache.tez.runtime.api.impl.IOStatistics 
    implements InputStatistics, OutputStatistics {
    
    @Override
    public long getDataSize() {
      return super.getDataSize();
    }
    
    @Override
    public long getItemsProcessed() {
      return super.getItemsProcessed();
    }
  }

  class VertexStatisticsImpl implements VertexStatistics {
    final Map<String, IOStatisticsImpl> ioStats;
    final BitSet taskSet;

    public VertexStatisticsImpl() {
      ioStats = Maps.newHashMapWithExpectedSize(ioIndices.size());
      taskSet = new BitSet();
      for (String name : getIOIndices().keySet()) {
        ioStats.put(name, new IOStatisticsImpl());
      }
    }

    public IOStatisticsImpl getIOStatistics(String ioName) {
      return ioStats.get(ioName);
    }

    void mergeFrom(TaskStatistics taskStats) {
      if (taskStats == null) {
        return;
      }

      for (Map.Entry<String, org.apache.tez.runtime.api.impl.IOStatistics> entry : taskStats
          .getIOStatistics().entrySet()) {
        String ioName = entry.getKey();
        IOStatisticsImpl myIOStat = ioStats.get(ioName);
        Preconditions.checkState(myIOStat != null, "Unexpected IO name: " + ioName
            + " for vertex:" + getLogIdentifier());
        myIOStat.mergeFrom(entry.getValue());
      }
    }

    @Override
    public InputStatistics getInputStatistics(String inputName) {
      return getIOStatistics(inputName);
    }

    @Override
    public OutputStatistics getOutputStatistics(String outputName) {
      return getIOStatistics(outputName);
    }

    void addTask(TezTaskID taskID) {
      taskSet.set(taskID.getId());
    }

    boolean containsTask(TezTaskID taskID) {
      return taskSet.get(taskID.getId());
    }
  }

  void resetCompletedTaskStatsCache(boolean recompute) {
    completedTasksStatsCache = new VertexStatisticsImpl();
    if (recompute) {
      for (Task t : getTasks().values()) {
        if (t.getState() == TaskState.SUCCEEDED) {
          completedTasksStatsCache.mergeFrom(((TaskImpl) t).getStatistics());
        }
      }
    }
  }

  public VertexImpl(TezVertexID vertexId, VertexPlan vertexPlan,
      String vertexName, Configuration dagConf, EventHandler eventHandler,
      TaskCommunicatorManagerInterface taskCommunicatorManagerInterface, Clock clock,
      TaskHeartbeatHandler thh, boolean commitVertexOutputs,
      AppContext appContext, VertexLocationHint vertexLocationHint,
      Map<String, VertexGroupInfo> dagVertexGroups, TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOption,
      StateChangeNotifier entityStatusTracker, Configuration dagOnlyConf) {
    this.vertexId = vertexId;
    this.vertexPlan = vertexPlan;
    this.vertexName = StringInterner.weakIntern(vertexName);
    this.vertexConf = new Configuration(dagConf);
    this.vertexOnlyConf = new Configuration(dagOnlyConf);
    if (vertexPlan.hasVertexConf()) {
      ConfigurationProto confProto = vertexPlan.getVertexConf();
      for (PlanKeyValuePair keyValuePair : confProto.getConfKeyValuesList()) {
        TezConfiguration.validateProperty(keyValuePair.getKey(), Scope.VERTEX);
        vertexConf.set(keyValuePair.getKey(), keyValuePair.getValue());
        vertexOnlyConf.set(keyValuePair.getKey(), keyValuePair.getValue());
      }
    }
    this.vertexContextConfig = new VertexConfigImpl(vertexConf);

    this.clock = clock;
    this.appContext = appContext;
    this.commitVertexOutputs = commitVertexOutputs;
    this.logIdentifier =  this.getVertexId() + " [" + this.getName() + "]";

    this.taskCommunicatorManagerInterface = taskCommunicatorManagerInterface;
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
    this.dag = appContext.getCurrentDAG();

    this.taskResource = DagTypeConverters
        .createResourceRequestFromTaskConfig(vertexPlan.getTaskConfig());
    this.processorDescriptor = DagTypeConverters
        .convertProcessorDescriptorFromDAGPlan(vertexPlan
            .getProcessorDescriptor());
    this.localResources = DagTypeConverters
        .createLocalResourceMapFromDAGPlan(vertexPlan.getTaskConfig()
            .getLocalResourceList());
    this.localResources.putAll(dag.getLocalResources());
    this.environment = DagTypeConverters
        .createEnvironmentMapFromDAGPlan(vertexPlan.getTaskConfig()
            .getEnvironmentSettingList());
    this.taskSpecificLaunchCmdOpts = taskSpecificLaunchCmdOption;
    this.recoveryData = appContext.getDAGRecoveryData() == null ?
        null : appContext.getDAGRecoveryData().getVertexRecoveryData(vertexId);
    // Set up log properties, including task specific log properties.
    String javaOptsWithoutLoggerMods =
        vertexPlan.getTaskConfig().hasJavaOpts() ? vertexPlan.getTaskConfig().getJavaOpts() : null;
    String logString = vertexConf.get(TezConfiguration.TEZ_TASK_LOG_LEVEL, TezConfiguration.TEZ_TASK_LOG_LEVEL_DEFAULT);
    String [] taskLogParams = TezClientUtils.parseLogParams(logString);
    this.javaOpts = TezClientUtils.maybeAddDefaultLoggingJavaOpts(taskLogParams[0], javaOptsWithoutLoggerMods);

    if (taskSpecificLaunchCmdOpts.hasModifiedLogProperties()) {
      String [] taskLogParamsTaskSpecific = taskSpecificLaunchCmdOption.getTaskSpecificLogParams();
      this.javaOptsTaskSpecific = TezClientUtils
          .maybeAddDefaultLoggingJavaOpts(taskLogParamsTaskSpecific[0], javaOptsWithoutLoggerMods);

      environmentTaskSpecific = new HashMap<String, String>(this.environment.size());
      environmentTaskSpecific.putAll(environment);
      if (taskLogParamsTaskSpecific.length == 2 && !Strings.isNullOrEmpty(taskLogParamsTaskSpecific[1])) {
        TezClientUtils.addLogParamsToEnv(environmentTaskSpecific, taskLogParamsTaskSpecific);
      }
    } else {
      this.javaOptsTaskSpecific = null;
      this.environmentTaskSpecific = null;
    }

    // env for tasks which don't have task-specific configuration. Has to be set up later to
    // optionally allow copying this for specific tasks
    TezClientUtils.addLogParamsToEnv(this.environment, taskLogParams);


    this.containerContext = new ContainerContext(this.localResources,
        appContext.getCurrentDAG().getCredentials(), this.environment, this.javaOpts, this);
    LOG.info("Default container context for " + logIdentifier + "=" + containerContext + ", Default Resources=" + this.taskResource);

    if (vertexPlan.getInputsCount() > 0) {
      setAdditionalInputs(vertexPlan.getInputsList());
    }
    if (vertexPlan.getOutputsCount() > 0) {
      setAdditionalOutputs(vertexPlan.getOutputsList());
    }
    this.stateChangeNotifier = entityStatusTracker;

    // Setup the initial parallelism early. This may be changed after
    // initialization or on a setParallelism call.
    this.numTasks = vertexPlan.getTaskConfig().getNumTasks();
    // Not sending the notifier a parallelism update since this is the initial parallelism

    this.dagVertexGroups = dagVertexGroups;
    
    isSpeculationEnabled = vertexConf.getBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED,
        TezConfiguration.TEZ_AM_SPECULATION_ENABLED_DEFAULT);
    if (isSpeculationEnabled()) {
      speculator = new LegacySpeculator(vertexConf, getAppContext(), this);
    }

    maxFailuresPercent = vertexConf.getFloat(TezConfiguration.TEZ_VERTEX_FAILURES_MAXPERCENT,
            TezConfiguration.TEZ_VERTEX_FAILURES_MAXPERCENT_DEFAULT);

    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.

    boolean isLocal = vertexConf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE,
        TezConfiguration.TEZ_LOCAL_MODE_DEFAULT);

    String tezDefaultComponentName =
        isLocal ? TezConstants.getTezUberServicePluginName() :
            TezConstants.getTezYarnServicePluginName();

    org.apache.tez.dag.api.Vertex.VertexExecutionContext execContext = dag.getDefaultExecutionContext();
    if (vertexPlan.hasExecutionContext()) {
      execContext = DagTypeConverters.convertFromProto(vertexPlan.getExecutionContext());
      LOG.info("Using ExecutionContext from Vertex for Vertex {}", vertexName);
    } else if (execContext != null) {
      LOG.info("Using ExecutionContext from DAG for Vertex {}", vertexName);
    }
    if (execContext != null) {
      if (execContext.shouldExecuteInAm()) {
        tezDefaultComponentName = TezConstants.getTezUberServicePluginName();
      }
    }

    String taskSchedulerName = tezDefaultComponentName;
    String containerLauncherName = tezDefaultComponentName;
    String taskCommName = tezDefaultComponentName;

    if (execContext != null) {
      if (execContext.getTaskSchedulerName() != null) {
        taskSchedulerName = execContext.getTaskSchedulerName();
      }
      if (execContext.getContainerLauncherName() != null) {
        containerLauncherName = execContext.getContainerLauncherName();
      }
      if (execContext.getTaskCommName() != null) {
        taskCommName = execContext.getTaskCommName();
      }
    }

    try {
      taskSchedulerIdentifier = appContext.getTaskScheduerIdentifier(taskSchedulerName);
    } catch (Exception e) {
      LOG.error("Failed to get index for taskScheduler: " + taskSchedulerName);
      throw e;
    }
    try {
      taskCommunicatorIdentifier = appContext.getTaskCommunicatorIdentifier(taskCommName);
    } catch (Exception e) {
      LOG.error("Failed to get index for taskCommunicator: " + taskCommName);
      throw e;
    }
    try {
      containerLauncherIdentifier =
          appContext.getContainerLauncherIdentifier(containerLauncherName);
    } catch (Exception e) {
      LOG.error("Failed to get index for containerLauncher: " + containerLauncherName);
      throw e;
    }
    this.servicePluginInfo = new ServicePluginInfo()
        .setContainerLauncherName(
            appContext.getContainerLauncherName(this.containerLauncherIdentifier))
        .setTaskSchedulerName(appContext.getTaskSchedulerName(this.taskSchedulerIdentifier))
        .setTaskCommunicatorName(appContext.getTaskCommunicatorName(this.taskCommunicatorIdentifier))
        .setContainerLauncherClassName(
            appContext.getContainerLauncherClassName(this.containerLauncherIdentifier))
        .setTaskSchedulerClassName(
            appContext.getTaskSchedulerClassName(this.taskSchedulerIdentifier))
        .setTaskCommunicatorClassName(
            appContext.getTaskCommunicatorClassName(this.taskCommunicatorIdentifier));

    StringBuilder sb = new StringBuilder();
    sb.append("Running vertex: ").append(logIdentifier).append(" : ")
        .append("TaskScheduler=").append(taskSchedulerIdentifier).append(":").append(taskSchedulerName)
        .append(", ContainerLauncher=").append(containerLauncherIdentifier).append(":").append(containerLauncherName)
        .append(", TaskCommunicator=").append(taskCommunicatorIdentifier).append(":").append(taskCommName);
    LOG.info(sb.toString());

    stateMachine = new StateMachineTez<VertexState, VertexEventType, VertexEvent, VertexImpl>(
        stateMachineFactory.make(this), this);
    augmentStateMachine();
  }

  @Override
  public Configuration getConf() {
    return vertexConf;
  }

  @Override
  public int getTaskSchedulerIdentifier() {
    return this.taskSchedulerIdentifier;
  }

  @Override
  public int getContainerLauncherIdentifier() {
    return this.containerLauncherIdentifier;
  }

  @Override
  public int getTaskCommunicatorIdentifier() {
    return this.taskCommunicatorIdentifier;
  }

  @Override
  public ServicePluginInfo getServicePluginInfo() {
    return servicePluginInfo;
  }

  @Override
  public boolean isSpeculationEnabled() {
    return isSpeculationEnabled;
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
  public LinkedHashMap<String, Integer> getIOIndices() {
    return ioIndices;
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
    return getTask(TezTaskID.getInstance(this.vertexId, taskIndex));
  }

  @Override
  public int getTotalTasks() {
    readLock.lock();
    try {
      return numTasks;
    } finally {
      readLock.unlock();
    }
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
      if (inTerminalState()) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      TezCounters counters = new TezCounters();
      counters.incrAllCounters(this.counters);
      return incrTaskCounters(counters, tasks.values());

    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezCounters getCachedCounters() {
    readLock.lock();

    try {
      // FIXME a better lightweight approach for counters is needed
      if (fullCounters == null && cachedCounters != null
          && ((cachedCountersTimestamp+10000) > System.currentTimeMillis())) {
        LOG.info("Asked for counters"
            + ", cachedCountersTimestamp=" + cachedCountersTimestamp
            + ", currentTime=" + System.currentTimeMillis());
        return cachedCounters;
      }

      cachedCountersTimestamp = System.currentTimeMillis();
      if (inTerminalState()) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      TezCounters counters = new TezCounters();
      counters.incrAllCounters(this.counters);
      cachedCounters = incrTaskCounters(counters, tasks.values());
      return cachedCounters;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void addCounters(final TezCounters tezCounters) {
    counters.incrAllCounters(tezCounters);
  }

  @Override
  public int getMaxTaskConcurrency() {
    return vertexConf.getInt(TezConfiguration.TEZ_AM_VERTEX_MAX_TASK_CONCURRENCY, 
        TezConfiguration.TEZ_AM_VERTEX_MAX_TASK_CONCURRENCY_DEFAULT);
  }

  public VertexStats getVertexStats() {

    readLock.lock();
    try {
      if (inTerminalState()) {
        this.mayBeConstructFinalFullCounters();
        return this.vertexStats;
      }

      VertexStats stats = new VertexStats();
      return updateVertexStats(stats, tasks.values());

    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getInitTime() {
    readLock.lock();
    try {
      return initedTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getStartTime() {
    readLock.lock();
    try {
      return startedTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    readLock.lock();
    try {
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void reportTaskStartTime(long taskStartTime) {
    synchronized (firstTaskStartTimeLock) {
      if (firstTaskStartTime < 0 || taskStartTime < firstTaskStartTime) {
        firstTaskStartTime = taskStartTime;
      }
    }
  }

  @Override
  public long getFirstTaskStartTime() {
    return firstTaskStartTime;
  }

  @Override
  public long getLastTaskFinishTime() {
    readLock.lock();
    try {
      if (inTerminalState()) {
        mayBeConstructFinalFullCounters();
        return vertexStats.getLastTaskFinishTime();
      } else {
        return -1;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public VertexConfig getVertexConfig() {
    return vertexContextConfig;
  }

  boolean inTerminalState() {
    VertexState state = getInternalState();
    if (state == VertexState.ERROR || state == VertexState.FAILED
        || state == VertexState.KILLED || state == VertexState.SUCCEEDED) {
      return true;
    }
    return false;
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
      final VertexState state = this.getState();
      switch (state) {
      case NEW:
      case INITED:
      case INITIALIZING:
        progress = 0.0f;
        break;
      case RUNNING:
        computeProgress();
        break;
      case KILLED:
      case ERROR:
      case FAILED:
      case TERMINATING:
        progress = 0.0f;
        break;
      case COMMITTING:
      case SUCCEEDED:
        progress = 1.0f;
        break;
      default:
        // unknown, do not change progress
        break;
      }
      return progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public float getCompletedTaskProgress() {
    this.readLock.lock();
    try {
      int totalTasks = getTotalTasks();
      if (totalTasks < 0) {
        return 0.0f;
      }
      if (totalTasks == 0) {
        VertexState state = getStateMachine().getCurrentState();
        if (state == VertexState.ERROR || state == VertexState.FAILED
            || state == VertexState.KILLED || state == VertexState.SUCCEEDED) {
          return 1.0f;
        } else {
          return 0.0f;
        }
      }
      return ((float)this.succeededTaskCount/totalTasks);
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
      if (inTerminalState()) {
        progress.setRunningTaskCount(0);
      } else {
        progress.setRunningTaskCount(getRunningTasks());
      }
      progress.setFailedTaskCount(failedTaskCount);
      progress.setKilledTaskCount(killedTaskCount);
      progress.setFailedTaskAttemptCount(failedTaskAttemptCount.get());
      progress.setKilledTaskAttemptCount(killedTaskAttemptCount.get());
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

  @VisibleForTesting
  List<EventInfo> getOnDemandRouteEvents() {
    return onDemandRouteEvents;
  }
  
  private void computeProgress() {
    this.readLock.lock();
    try {
      float progress = 0f;
      for (Task task : this.tasks.values()) {
        progress += (task.getProgress());
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

  @Override
  public String getLogIdentifier() {
    return this.logIdentifier;
  }

  @Override
  public void incrementFailedTaskAttemptCount() {
    this.failedTaskAttemptCount.incrementAndGet();
  }

  @Override
  public void incrementKilledTaskAttemptCount() {
    this.killedTaskAttemptCount.incrementAndGet();
  }

  @Override
  public int getFailedTaskAttemptCount() {
    return this.failedTaskAttemptCount.get();
  }

  @Override
  public int getKilledTaskAttemptCount() {
    return this.killedTaskAttemptCount.get();
  }

  private void setTaskLocationHints(VertexLocationHint vertexLocationHint) {
    if (vertexLocationHint != null &&
        vertexLocationHint.getTaskLocationHints() != null &&
        !vertexLocationHint.getTaskLocationHints().isEmpty()) {
      List<TaskLocationHint> locHints = vertexLocationHint.getTaskLocationHints();
      // TODO TEZ-2246 hints size must match num tasks
      taskLocationHints = locHints.toArray(new TaskLocationHint[locHints.size()]);
    }
  }

  @Override
  public void scheduleSpeculativeTask(TezTaskID taskId) {
    readLock.lock();
    try {
      Preconditions.checkState(taskId.getId() < numTasks);
      eventHandler.handle(new TaskEvent(taskId, TaskEventType.T_ADD_SPEC_ATTEMPT));
    } finally {
      readLock.unlock();
    }
  }
  
  void setupEdgeRouting() throws AMUserCodeException {
    for (Edge e : sourceVertices.values()) {
      e.routingToBegin();
    }
  }
  
  private void unsetTasksNotYetScheduled() throws AMUserCodeException {
    if (tasksNotYetScheduled) {
      setupEdgeRouting();
      // change state under lock
      writeLock.lock();
      try {
        tasksNotYetScheduled = false;
        // only now can we be sure of the edge manager type. so until now
        // we will accumulate pending tasks in case legacy routing gets used.
        // this is only needed to support mixed mode routing. Else for
        // on demand routing events can be directly added to taskEvents when 
        // they arrive in handleRoutedEvents instead of first caching them in 
        // pendingTaskEvents. When legacy routing is removed then pendingTaskEvents
        // can be removed.
        if (!pendingTaskEvents.isEmpty()) {
          LOG.info("Routing pending task events for vertex: " + logIdentifier);
          try {
            handleRoutedTezEvents(pendingTaskEvents, true);
          } catch (AMUserCodeException e) {
            String msg = "Exception in " + e.getSource() + ", vertex=" + logIdentifier;
            LOG.error(msg, e);
            addDiagnostic(msg + ", " + e.getMessage() + ", "
                + ExceptionUtils.getStackTrace(e.getCause()));
            eventHandler.handle(new VertexEventTermination(vertexId,
                VertexTerminationCause.AM_USERCODE_FAILURE));
            return;
          }
          pendingTaskEvents.clear();
        }
      } finally {
        writeLock.unlock();
      }
    }
  }
  
  TaskSpec createRemoteTaskSpec(int taskIndex) throws AMUserCodeException {
    return TaskSpec.createBaseTaskSpec(getDAG().getName(),
        getName(), getTotalTasks(), getProcessorDescriptor(),
        getInputSpecList(taskIndex), getOutputSpecList(taskIndex), 
        getGroupInputSpecList(), vertexOnlyConf);
  }

  @Override
  public void scheduleTasks(List<ScheduleTaskRequest> tasksToSchedule) {
    try {
      unsetTasksNotYetScheduled();
      // update state under write lock
      writeLock.lock();
      try {
        for (ScheduleTaskRequest task : tasksToSchedule) {
          if (numTasks <= task.getTaskIndex()) {
            throw new TezUncheckedException(
                "Invalid taskId: " + task.getTaskIndex() + " for vertex: " + logIdentifier);
          }
          TaskLocationHint locationHint = task.getTaskLocationHint();
          if (locationHint != null) {
            if (taskLocationHints == null) {
              taskLocationHints = new TaskLocationHint[numTasks];
            }
            taskLocationHints[task.getTaskIndex()] = locationHint;
          }
        }
      } finally {
        writeLock.unlock();
      }

      /**
       * read lock is not needed here. For e.g after starting task
       * scheduling on the vertex, it would not change numTasks. Rest of
       * the methods creating remote task specs have their
       * own locking mechanisms. Ref: TEZ-3297
       */
      for (ScheduleTaskRequest task : tasksToSchedule) {
        TezTaskID taskId = TezTaskID.getInstance(vertexId, task.getTaskIndex());
        TaskSpec baseTaskSpec = createRemoteTaskSpec(taskId.getId());
        boolean fromRecovery = recoveryData == null ? false : recoveryData.getTaskRecoveryData(taskId) != null;
        eventHandler.handle(new TaskEventScheduleTask(taskId, baseTaskSpec,
            getTaskLocationHint(taskId), fromRecovery));
      }
    } catch (AMUserCodeException e) {
      String msg = "Exception in " + e.getSource() + ", vertex=" + getLogIdentifier();
      LOG.error(msg, e);
      // send event to fail the vertex
      eventHandler.handle(new VertexEventManagerUserCodeError(getVertexId(), e));
      // throw an unchecked exception to stop the vertex manager that invoked this.
      throw new TezUncheckedException(e);
    }
  }
  
  @Override
  public void reconfigureVertex(int parallelism,
      @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, EdgeProperty> sourceEdgeProperties) throws AMUserCodeException {
    setParallelismWrapper(parallelism, locationHint, sourceEdgeProperties, null, true);
  }
  
  @Override
  public void reconfigureVertex(@Nullable Map<String, InputSpecUpdate> rootInputSpecUpdate,
      int parallelism,
      @Nullable VertexLocationHint locationHint) throws AMUserCodeException {
    setParallelism(parallelism, locationHint, null, rootInputSpecUpdate, true);
  }

  @Override
  public void reconfigureVertex(int parallelism,
      @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, EdgeProperty> sourceEdgeProperties,
      @Nullable Map<String, InputSpecUpdate> rootInputSpecUpdate) throws AMUserCodeException {
    setParallelismWrapper(parallelism, locationHint, sourceEdgeProperties, rootInputSpecUpdate, true);
  }
  
  @Override
  public void setParallelism(int parallelism, VertexLocationHint vertexLocationHint,
      Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
      Map<String, InputSpecUpdate> rootInputSpecUpdates, boolean fromVertexManager) 
          throws AMUserCodeException {
    // temporarily support conversion of edge manager to edge property
    Map<String, EdgeProperty> sourceEdgeProperties = Maps.newHashMap();
    readLock.lock();
    try {
      if (sourceEdgeManagers != null && !sourceEdgeManagers.isEmpty()) {
        for (Edge e : sourceVertices.values()) {
          EdgeManagerPluginDescriptor newEdge = sourceEdgeManagers.get(e.getSourceVertexName());
          EdgeProperty oldEdge = e.getEdgeProperty();
          if (newEdge != null) {
            sourceEdgeProperties.put(
                e.getSourceVertexName(),
                EdgeProperty.create(newEdge, oldEdge.getDataSourceType(),
                    oldEdge.getSchedulingType(), oldEdge.getEdgeSource(),
                    oldEdge.getEdgeDestination()));
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    setParallelismWrapper(parallelism, vertexLocationHint, sourceEdgeProperties, rootInputSpecUpdates,
        fromVertexManager);
  }

  private void setParallelismWrapper(int parallelism, VertexLocationHint vertexLocationHint,
      Map<String, EdgeProperty> sourceEdgeProperties,
      Map<String, InputSpecUpdate> rootInputSpecUpdates,
      boolean fromVertexManager) throws AMUserCodeException {
    Preconditions.checkArgument(parallelism >= 0, "Parallelism must be >=0. Value: " + parallelism
        + " for vertex: " + logIdentifier);
    writeLock.lock();
    this.setParallelismCalledFlag = true;
    try {
      // disallow changing things after a vertex has started
      if (!tasksNotYetScheduled) {
        String msg = "setParallelism cannot be called after scheduling tasks. Vertex: "
            + getLogIdentifier();
        LOG.info(msg);
        throw new TezUncheckedException(msg);
      }

      if (fromVertexManager && canInitVertex()) {
        // vertex is fully defined. setParallelism has been called. VertexManager should have 
        // informed us about this. Otherwise we would have notified listeners that we are fully 
        // defined before we are actually fully defined
        Preconditions
            .checkState(
                vertexToBeReconfiguredByManager,
                "Vertex is fully configured but still"
                    + " the reconfiguration API has been called. VertexManager must notify the framework using "
                    + " context.vertexReconfigurationPlanned() before re-configuring the vertex."
                    + " vertexId=" + logIdentifier);
      }
      
      // Input initializer/Vertex Manager/1-1 split expected to set parallelism.
      if (numTasks == -1) {
        if (getState() != VertexState.INITIALIZING) {
          throw new TezUncheckedException(
              "Vertex state is not Initializing. Value: " + getState()
                  + " for vertex: " + logIdentifier);
        }

        if(sourceEdgeProperties != null) {
          for(Map.Entry<String, EdgeProperty> entry : sourceEdgeProperties.entrySet()) {
            LOG.info("Replacing edge manager for source:"
                + entry.getKey() + " destination: " + getLogIdentifier());
            Vertex sourceVertex = appContext.getCurrentDAG().getVertex(entry.getKey());
            Edge edge = sourceVertices.get(sourceVertex);
            try {
              edge.setEdgeProperty(entry.getValue());
            } catch (Exception e) {
              throw new TezUncheckedException("Fail to update EdgeProperty for Edge,"
                  + "sourceVertex:" + edge.getSourceVertexName()
                  + "destinationVertex:" + edge.getDestinationVertexName(), e);
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
        int oldNumTasks = numTasks;
        this.numTasks = parallelism;
        stateChangeNotifier.stateChanged(vertexId,
            new VertexStateUpdateParallelismUpdated(vertexName, numTasks, oldNumTasks));
        this.createTasks();
        setVertexLocationHint(vertexLocationHint);
        LOG.info("Vertex " + getLogIdentifier() +
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
 
        int oldNumTasks = numTasks;
        
        // start buffering incoming events so that we can re-route existing events
        for (Edge edge : sourceVertices.values()) {
          edge.startEventBuffering();
        }

        if (parallelism == numTasks) {
          LOG.info("setParallelism same as current value: " + parallelism +
              " for vertex: " + logIdentifier);
          Preconditions.checkArgument(sourceEdgeProperties != null,
              "Source edge managers or RootInputSpecs must be set when not changing parallelism");
        } else {
          LOG.info("Resetting vertex location hints due to change in parallelism for vertex: "
              + logIdentifier);
          vertexLocationHint = null;

          if (parallelism > numTasks) {
            addTasks((parallelism));
          } else if (parallelism < numTasks) {
            removeTasks(parallelism);
          }
        }
        Preconditions.checkState(this.numTasks == parallelism, getLogIdentifier());
        
        // set new vertex location hints
        setVertexLocationHint(vertexLocationHint);
        LOG.info("Vertex " + getLogIdentifier() + " parallelism set to " + parallelism + " from "
            + oldNumTasks);
        
        // notify listeners
        stateChangeNotifier.stateChanged(vertexId,
            new VertexStateUpdateParallelismUpdated(vertexName, numTasks, oldNumTasks));
        assert tasks.size() == numTasks;

        // set new edge managers
        if(sourceEdgeProperties != null) {
          for(Map.Entry<String, EdgeProperty> entry : sourceEdgeProperties.entrySet()) {
            LOG.info("Replacing edge manager for source:"
                + entry.getKey() + " destination: " + getLogIdentifier());
            Vertex sourceVertex = appContext.getCurrentDAG().getVertex(entry.getKey());
            Edge edge = sourceVertices.get(sourceVertex);
            try {
              edge.setEdgeProperty(entry.getValue());
            } catch (Exception e) {
              throw new TezUncheckedException(e);
            }
          }
        }

        // stop buffering events
        for (Edge edge : sourceVertices.values()) {
          edge.stopEventBuffering();
        }
      }

    } finally {
      writeLock.unlock();
    }
  }

  @Override
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
  public void vertexReconfigurationPlanned() {
    writeLock.lock();
    try {
      Preconditions.checkState(!vmIsInitialized.get(),
          "context.vertexReconfigurationPlanned() cannot be called after initialize()");
      Preconditions.checkState(!completelyConfiguredSent.get(), "vertexReconfigurationPlanned() "
          + " cannot be invoked after the vertex has been configured.");
      this.vertexToBeReconfiguredByManager = true;
    } finally {
      writeLock.unlock();
    }      
  }

  @Override
  public void doneReconfiguringVertex() {
    writeLock.lock();
    try {
      Preconditions.checkState(vertexToBeReconfiguredByManager, "doneReconfiguringVertex() can be "
          + "invoked only after vertexReconfigurationPlanned() is invoked");
      this.vertexToBeReconfiguredByManager = false;
      if (canInitVertex()) {
        maybeSendConfiguredEvent();
      } else {
        Preconditions.checkState(getInternalState() == VertexState.INITIALIZING, "Vertex: "
            + getLogIdentifier());
      }
      
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
      } catch (RuntimeException e) {
        String message = "Uncaught Exception when handling event " + event.getType() +
            " on vertex " + this.vertexName +
            " with vertexId " + this.vertexId +
            " at current state " + oldState;
        LOG.error(message, e);
        addDiagnostic(message);
        if (!internalErrorTriggered.getAndSet(true)) {
          eventHandler.handle(new VertexEvent(this.vertexId,
              VertexEventType.V_INTERNAL_ERROR));
        }
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
    if (recoveryData == null || !recoveryData.shouldSkipInit()) {
      VertexInitializedEvent initEvt = new VertexInitializedEvent(vertexId, vertexName,
          initTimeRequested, initedTime, numTasks,
          getProcessorName(), getAdditionalInputs(), initGeneratedEvents,
          servicePluginInfo);
      this.appContext.getHistoryHandler().handle(
              new DAGHistoryEvent(getDAGId(), initEvt));
    }
  }

  void logJobHistoryVertexStartedEvent() {
    if (recoveryData == null
        || !recoveryData.isVertexStarted()) {
      VertexStartedEvent startEvt = new VertexStartedEvent(vertexId,
          startTimeRequested, startedTime);
      this.appContext.getHistoryHandler().handle(
          new DAGHistoryEvent(getDAGId(), startEvt));
    }
  }

  void logVertexConfigurationDoneEvent() {
    if (recoveryData == null || !recoveryData.shouldSkipInit()) {
      Map<String, EdgeProperty> sourceEdgeProperties = new HashMap<String, EdgeProperty>();
      for (Map.Entry<Vertex, Edge> entry : this.sourceVertices.entrySet()) {
        sourceEdgeProperties.put(entry.getKey().getName(), entry.getValue().getEdgeProperty());
      }
      VertexConfigurationDoneEvent reconfigureDoneEvent =
          new VertexConfigurationDoneEvent(vertexId, clock.getTime(),
              numTasks, taskLocationHints == null ? null : VertexLocationHint.create(Lists.newArrayList(taskLocationHints)),
                  sourceEdgeProperties, rootInputSpecs, setParallelismCalledFlag);
      this.appContext.getHistoryHandler().handle(
          new DAGHistoryEvent(getDAGId(), reconfigureDoneEvent));
    }
  }

  void logJobHistoryVertexFinishedEvent() throws IOException {
    if (recoveryData == null
        || !recoveryData.isVertexSucceeded()) {
      logJobHistoryVertexCompletedHelper(VertexState.SUCCEEDED, finishTime,
          logSuccessDiagnostics ? StringUtils.join(getDiagnostics(), LINE_SEPARATOR) : "",
          getAllCounters());
    }
  }

  void logJobHistoryVertexFailedEvent(VertexState state) throws IOException {
    if (recoveryData == null
        || !recoveryData.isVertexFinished()) {
      TezCounters counters = null;
      try {
        counters = getAllCounters();
      } catch (LimitExceededException e) {
        // Ignore as failed vertex
        addDiagnostic("Counters limit exceeded: " + e.getMessage());
      }

      logJobHistoryVertexCompletedHelper(state, clock.getTime(),
          StringUtils.join(getDiagnostics(), LINE_SEPARATOR), counters);
    }
  }

  private void logJobHistoryVertexCompletedHelper(VertexState finalState, long finishTime,
                                                  String diagnostics, TezCounters counters) throws IOException {
    Map<String, Integer> taskStats = new HashMap<String, Integer>();
    taskStats.put(ATSConstants.NUM_COMPLETED_TASKS, completedTaskCount);
    taskStats.put(ATSConstants.NUM_SUCCEEDED_TASKS, succeededTaskCount);
    taskStats.put(ATSConstants.NUM_FAILED_TASKS, failedTaskCount);
    taskStats.put(ATSConstants.NUM_KILLED_TASKS, killedTaskCount);
    taskStats.put(ATSConstants.NUM_FAILED_TASKS_ATTEMPTS, failedTaskAttemptCount.get());
    taskStats.put(ATSConstants.NUM_KILLED_TASKS_ATTEMPTS, killedTaskAttemptCount.get());

    VertexFinishedEvent finishEvt = new VertexFinishedEvent(vertexId, vertexName, numTasks,
        initTimeRequested, initedTime, startTimeRequested, startedTime, finishTime, finalState,
        diagnostics, counters, getVertexStats(), taskStats, servicePluginInfo);
    this.appContext.getHistoryHandler().handleCriticalEvent(
        new DAGHistoryEvent(getDAGId(), finishEvt));
  }

  private static VertexState commitOrFinish(final VertexImpl vertex) {
    // commit only once. Dont commit shared outputs
    if (vertex.outputCommitters != null
        && !vertex.outputCommitters.isEmpty()) {
      if (vertex.recoveryData != null
          && vertex.recoveryData.isVertexCommitted()) {
        LOG.info("Vertex was already committed as per recovery"
            + " data, vertex=" + vertex.logIdentifier);
        return vertex.finished(VertexState.SUCCEEDED);
      }
      boolean firstCommit = true;
      for (Entry<String, OutputCommitter> entry : vertex.outputCommitters.entrySet()) {
        final OutputCommitter committer = entry.getValue();
        final String outputName = entry.getKey();
        if (vertex.sharedOutputs.contains(outputName)) {
          // dont commit shared committers. Will be committed by the DAG
          continue;
        }
        if (firstCommit) {
          LOG.info("Invoking committer commit for vertex, vertexId="
              + vertex.logIdentifier);
          // Log commit start event on first actual commit
          try {
            vertex.appContext.getHistoryHandler().handleCriticalEvent(
                new DAGHistoryEvent(vertex.getDAGId(),
                    new VertexCommitStartedEvent(vertex.vertexId,
                        vertex.clock.getTime())));
          } catch (IOException e) {
            LOG.error("Failed to persist commit start event to recovery, vertex="
                + vertex.logIdentifier, e);
            vertex.trySetTerminationCause(VertexTerminationCause.RECOVERY_ERROR);
            return vertex.finished(VertexState.FAILED);
          }
          firstCommit = false;
        }
        VertexCommitCallback commitCallback = new VertexCommitCallback(vertex, outputName);
        CallableEvent commitCallableEvent = new CallableEvent(commitCallback) {
          @Override
          public Void call() throws Exception {
            try {
              TezUtilsInternal.setHadoopCallerContext(vertex.appContext.getHadoopShim(),
                  vertex.vertexId);
              vertex.dagUgi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                  LOG.info("Invoking committer commit for output=" + outputName
                      + ", vertexId=" + vertex.logIdentifier);
                  committer.commitOutput();
                  return null;
                }
              });
            } finally {
              vertex.appContext.getHadoopShim().clearHadoopCallerContext();
            }
            return null;
          }
        };
        ListenableFuture<Void> commitFuture = 
            vertex.getAppContext().getExecService().submit(commitCallableEvent);
        Futures.addCallback(commitFuture, commitCallableEvent.getCallback());
        vertex.commitFutures.put(outputName, commitFuture);
      }
    }
    if (vertex.commitFutures.isEmpty()) {
      return vertex.finished(VertexState.SUCCEEDED);
    } else {
      return VertexState.COMMITTING;
    }
  }

  private static String constructCheckTasksForCompletionLog(VertexImpl vertex) {
    String logLine = vertex.logIdentifier
        + ", tasks=" + vertex.numTasks
        + ", failed=" + vertex.failedTaskCount
        + ", killed=" + vertex.killedTaskCount
        + ", success=" + vertex.succeededTaskCount
        + ", completed=" + vertex.completedTaskCount
        + ", commits=" + vertex.commitFutures.size()
        + ", err=" + vertex.terminationCause;
    return logLine;
  }

  // triggered by task_complete
  static VertexState checkTasksForCompletion(final VertexImpl vertex) {
    // this log helps quickly count the completion count for a vertex.
    // grepping and counting for attempts and handling re-tries is time consuming
    LOG.info("Task Completion: " + constructCheckTasksForCompletionLog(vertex));
    //check for vertex failure first
    if (vertex.completedTaskCount > vertex.tasks.size()) {
      LOG.error("task completion accounting issue: completedTaskCount > nTasks:"
          + constructCheckTasksForCompletionLog(vertex));
    }

    if (vertex.completedTaskCount == vertex.tasks.size()) {
      // finished - gather stats
      vertex.finalStatistics = vertex.constructStatistics();

      //Only succeed if tasks complete successfully and no terminationCause is registered or if failures are below configured threshold.
      boolean vertexSucceeded = vertex.succeededTaskCount == vertex.numTasks;
      boolean vertexFailuresBelowThreshold = (vertex.succeededTaskCount + vertex.failedTaskCount == vertex.numTasks)
          && (vertex.failedTaskCount * 100 <= vertex.maxFailuresPercent * vertex.numTasks);

      if((vertexSucceeded || vertexFailuresBelowThreshold) && vertex.terminationCause == null) {
        if(vertexSucceeded) {
          LOG.info("All tasks have succeeded, vertex:" + vertex.logIdentifier);
        } else {
          LOG.info("All tasks in the vertex " + vertex.logIdentifier + " have completed and the percentage of failed tasks (failed/total) (" + vertex.failedTaskCount + "/" + vertex.numTasks + ") is less that the threshold of " + vertex.maxFailuresPercent);
          vertex.addDiagnostic("Vertex succeeded as percentage of failed tasks (failed/total) (" + vertex.failedTaskCount + "/" + vertex.numTasks + ") is less that the threshold of " + vertex.maxFailuresPercent);
          vertex.logSuccessDiagnostics = true;
          for (Task task : vertex.tasks.values()) {
            if (!task.getState().equals(TaskState.FAILED)) {
              continue;
            }
            // Find the last attempt and mark that as successful
            Iterator<TezTaskAttemptID> attempts = task.getAttempts().keySet().iterator();
            TezTaskAttemptID lastAttempt = null;
            while (attempts.hasNext()) {
              TezTaskAttemptID attempt = attempts.next();
              if (lastAttempt == null || attempt.getId() > lastAttempt.getId()) {
                lastAttempt = attempt;
              }
            }
            LOG.info("Succeeding failed task attempt:" + lastAttempt);
            for (Map.Entry<Vertex, Edge> vertexEdge : vertex.targetVertices.entrySet()) {
              Vertex destVertex = vertexEdge.getKey();
              Edge edge = vertexEdge.getValue();
              try {
                List<TezEvent> tezEvents = edge.generateEmptyEventsForAttempt(lastAttempt);

                // Downstream vertices need to receive a SUCCEEDED completion event for each failed task to ensure num bipartite count is correct
                VertexEventTaskAttemptCompleted completionEvent = new VertexEventTaskAttemptCompleted(lastAttempt, TaskAttemptStateInternal.SUCCEEDED);

                // Notify all target vertices
                vertex.eventHandler.handle(new VertexEventSourceTaskAttemptCompleted(destVertex.getVertexId(), completionEvent));
                vertex.eventHandler.handle(new VertexEventRouteEvent(destVertex.getVertexId(), tezEvents));
              } catch (Exception e) {
                throw new TezUncheckedException(e);
              }
            }
          }
        }

        if (vertex.commitVertexOutputs && !vertex.committed.getAndSet(true)) {
          // start commit if there're commits or just finish if no commits
          return commitOrFinish(vertex);
        } else {
          // just finish because no vertex committing needed
          return vertex.finished(VertexState.SUCCEEDED);
        }
      }
      return finishWithTerminationCause(vertex);
    }

    //return the current state, Vertex not finished yet
    return vertex.getInternalState();
  }

  //triggered by commit_complete
  static VertexState checkCommitsForCompletion(final VertexImpl vertex) {
    LOG.info("Commits completion: "
            + constructCheckTasksForCompletionLog(vertex));
    // terminationCause is null mean commit is succeeded, otherwise terminationCause will be set.
    if (vertex.terminationCause == null) {
      Preconditions.checkState(vertex.getState() == VertexState.COMMITTING,
          "Vertex should be in COMMITTING state, but in " + vertex.getState()
          + ", vertex:" + vertex.getLogIdentifier());
      if (vertex.commitFutures.isEmpty()) {
        // move from COMMITTING to SUCCEEDED
        return vertex.finished(VertexState.SUCCEEDED);
      } else {
        return VertexState.COMMITTING;
      }
    } else {
      if (!vertex.commitFutures.isEmpty()) {
        // pending commits are running
        return VertexState.TERMINATING;
      } else {
        // all the commits are completed successfully
        return finishWithTerminationCause(vertex);
      }
    }
  }

  private static VertexState finishWithTerminationCause(VertexImpl vertex) {
    Preconditions.checkArgument(vertex.getTerminationCause() != null, "TerminationCause is not set");
    String diagnosticMsg = "Vertex did not succeed due to " + vertex.getTerminationCause()
        + ", failedTasks:" + vertex.failedTaskCount
        + " killedTasks:" + vertex.killedTaskCount;
    LOG.info(diagnosticMsg);
    vertex.addDiagnostic(diagnosticMsg);
    return vertex.finished(vertex.getTerminationCause().getFinishedState());
  }

  /**
   * Set the terminationCause and send a kill-message to all tasks.
   * The task-kill messages are only sent once.
   */
  void tryEnactKill(VertexTerminationCause trigger,
      TaskTerminationCause taskterminationCause) {
    // In most cases the dag is shutting down due to some error
    TaskAttemptTerminationCause errCause = TaskAttemptTerminationCause.TERMINATED_AT_SHUTDOWN;
    if (taskterminationCause == TaskTerminationCause.DAG_KILL) {
      errCause = TaskAttemptTerminationCause.TERMINATED_BY_CLIENT;
    }
    if(trySetTerminationCause(trigger)){
      String msg = "Killing tasks in vertex: " + logIdentifier + " due to trigger: " + trigger; 
      LOG.info(msg);
      for (Task task : tasks.values()) {
        eventHandler.handle( // attempt was terminated because the vertex is shutting down
            new TaskEventTermination(task.getTaskId(), errCause, msg));
      }
    }
  }

  VertexState finished(VertexState finalState,
      VertexTerminationCause termCause, String diag) {
    if (finishTime == 0) setFinishTime();
    if (termCause != null) {
      trySetTerminationCause(termCause);
    }
    if (rootInputInitializerManager != null) {
      rootInputInitializerManager.shutdown();
      rootInputInitializerManager = null;
    }

    switch (finalState) {
      case ERROR:
        addDiagnostic("Vertex: " + logIdentifier + " error due to:" + terminationCause);
        if (!StringUtils.isEmpty(diag)) {
          addDiagnostic(diag);
        }
        abortVertex(VertexStatus.State.valueOf(finalState.name()));
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
        addDiagnostic("Vertex " + logIdentifier + " killed/failed due to:" + terminationCause);
        if (!StringUtils.isEmpty(diag)) {
          addDiagnostic(diag);
        }
        abortVertex(VertexStatus.State.valueOf(finalState.name()));
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
          try {
            logJobHistoryVertexFinishedEvent();
            eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
                finalState));
          } catch (LimitExceededException e) {
            LOG.error("Counter limits exceeded for vertex: " + getLogIdentifier(), e);
            finalState = VertexState.FAILED;
            addDiagnostic("Counters limit exceeded: " + e.getMessage());
            trySetTerminationCause(VertexTerminationCause.COUNTER_LIMITS_EXCEEDED);
            logJobHistoryVertexFailedEvent(finalState);
            eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
                finalState));
          }
        } catch (IOException e) {
          LOG.error("Failed to send vertex finished event to recovery", e);
          finalState = VertexState.FAILED;
          trySetTerminationCause(VertexTerminationCause.INTERNAL_ERROR);
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
    return finished(finalState, null, null);
  }


  private void initializeCommitters() throws Exception {
    if (!this.additionalOutputSpecs.isEmpty()) {
      LOG.info("Setting up committers for vertex " + logIdentifier + ", numAdditionalOutputs=" +
          additionalOutputs.size());
      for (Entry<String, RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> entry:
          additionalOutputs.entrySet())  {
        final String outputName = entry.getKey();
        final RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> od = entry.getValue();
        if (od.getControllerDescriptor() == null
            || od.getControllerDescriptor().getClassName() == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Ignoring committer as none specified for output="
                + outputName
                + ", vertexId=" + logIdentifier);
          }
          continue;
        }
        LOG.info("Instantiating committer for output=" + outputName
            + ", vertex=" + logIdentifier
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
            if (LOG.isDebugEnabled()) {
              LOG.debug("Invoking committer init for output=" + outputName
                  + ", vertex=" + logIdentifier);
            }

            try {
              TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(), vertexId);
              outputCommitter.initialize();
              outputCommitters.put(outputName, outputCommitter);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Invoking committer setup for output=" + outputName
                    + ", vertex=" + logIdentifier);
              }
              outputCommitter.setupOutput();
            } finally {
              appContext.getHadoopShim().clearHadoopCallerContext();
            }

            return null;
          }
        });
      }
    }
  }

  private boolean initializeVertex() {
    // Don't need to initialize committer if vertex is fully completed
    if (recoveryData != null && recoveryData.shouldSkipInit()) {
      // Do other necessary recovery here
      initedTime = recoveryData.getVertexInitedEvent().getInitedTime();
      List<TezEvent> initGeneratedEvents = recoveryData.getVertexInitedEvent().getInitGeneratedEvents();
      if (initGeneratedEvents != null && !initGeneratedEvents.isEmpty()) {
        eventHandler.handle(new VertexEventRouteEvent(getVertexId(), initGeneratedEvents));
      }
      // reset rootInputDescriptor because it may be changed during input initialization.
      this.rootInputDescriptors = recoveryData.getVertexInitedEvent().getAdditionalInputs();
    } else {
      initedTime = clock.getTime();
    }
    // Only initialize committer when it is in non-recovery mode or vertex is not recovered to completed 
    // state in recovery mode
    if (recoveryData == null || recoveryData.getVertexFinishedEvent() == null) {
      try {
        initializeCommitters();
      } catch (Exception e) {
        LOG.warn("Vertex Committer init failed, vertex=" + logIdentifier, e);
        addDiagnostic("Vertex init failed : "
            + ExceptionUtils.getStackTrace(e));
        trySetTerminationCause(VertexTerminationCause.INIT_FAILURE);
        finished(VertexState.FAILED);
        return false;
      }
    }

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

  @VisibleForTesting
  ContainerContext getContainerContext(int taskIdx) {
    if (taskSpecificLaunchCmdOpts.addTaskSpecificLaunchCmdOption(vertexName, taskIdx)) {

      String jvmOpts = javaOptsTaskSpecific != null ? javaOptsTaskSpecific : javaOpts;

      if (taskSpecificLaunchCmdOpts.hasModifiedTaskLaunchOpts()) {
        jvmOpts = taskSpecificLaunchCmdOpts.getTaskSpecificOption(jvmOpts, vertexName, taskIdx);
      }

      ContainerContext context = new ContainerContext(this.localResources,
          appContext.getCurrentDAG().getCredentials(),
          this.environmentTaskSpecific != null ? this.environmentTaskSpecific : this.environment,
          jvmOpts);
      return context;
    } else {
      return this.containerContext;
    }
  }

  private TaskImpl createTask(int taskIndex) {
    ContainerContext conContext = getContainerContext(taskIndex);
    return new TaskImpl(this.getVertexId(), taskIndex,
        this.eventHandler,
        vertexConf,
        this.taskCommunicatorManagerInterface,
        this.clock,
        this.taskHeartbeatHandler,
        this.appContext,
        (this.targetVertices != null ?
          this.targetVertices.isEmpty() : true),
        this.taskResource,
        conContext,
        this.stateChangeNotifier,
        this);
  }
  
  private void createTasks() {
    for (int i=0; i < this.numTasks; ++i) {
      TaskImpl task = createTask(i);
      this.addTask(task);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Created task for vertex " + logIdentifier + ": " +
            task.getTaskId());
      }
    }
  }
  
  private void addTasks(int newNumTasks) {
    Preconditions.checkArgument(newNumTasks > this.numTasks, getLogIdentifier());
    int initialNumTasks = this.numTasks;
    for (int i = initialNumTasks; i < newNumTasks; ++i) {
      TaskImpl task = createTask(i);
      this.addTask(task);
      this.numTasks++;
      if(LOG.isDebugEnabled()) {
        LOG.debug("Created task for vertex " + logIdentifier + ": " +
            task.getTaskId());
      }
    }
  }
  
  private void removeTasks(int newNumTasks) {
    Preconditions.checkArgument(newNumTasks < this.numTasks, getLogIdentifier());
    // assign to local variable of LinkedHashMap to make sure that changing
    // type of task causes compile error. We depend on LinkedHashMap for order
    LinkedHashMap<TezTaskID, Task> currentTasks = this.tasks;
    Iterator<Map.Entry<TezTaskID, Task>> iter = currentTasks.entrySet()
        .iterator();
    // remove tasks from the end to maintain index numbers
    int i = 0;
    while (iter.hasNext()) {
      i++;
      Map.Entry<TezTaskID, Task> entry = iter.next();
      Task task = entry.getValue();
      if (task.getState() != TaskState.NEW) {
        String msg = "All tasks must be in initial state when changing parallelism"
            + " for vertex: " + getLogIdentifier();
        LOG.warn(msg);
        throw new TezUncheckedException(msg);
      }
      if (i <= newNumTasks) {
        continue;
      }
      LOG.debug("Removing task: {}", entry.getKey());
      iter.remove();
      this.numTasks--;
    }
  }


  private VertexState setupVertex() {

    this.initTimeRequested = clock.getTime();

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
      LOG.error("A vertex with an Initial Input and a Shuffle Input are not supported at the moment");
      return finished(VertexState.FAILED);
    }

    numTasks = getVertexPlan().getTaskConfig().getNumTasks();
    if (!(numTasks == -1 || numTasks >= 0)) {
      addDiagnostic("Invalid task count for vertex"
          + ", numTasks=" + numTasks);
      trySetTerminationCause(VertexTerminationCause.INVALID_NUM_OF_TASKS);
      return VertexState.FAILED;
    }

    checkTaskLimits();
    // set VertexManager as the last step. Because in recovery case, we may need to restore 
    // some info from last the AM attempt and skip the initialization step. Otherwise numTasks may be
    // reset to -1 after the restore.
    try {
      assignVertexManager();
    } catch (TezException e1) {
      String msg = "Fail to create VertexManager, " + ExceptionUtils.getStackTrace(e1);
      LOG.error(msg);
      return finished(VertexState.FAILED, VertexTerminationCause.INIT_FAILURE, msg);
    }

    try {
      vertexManager.initialize();
      vmIsInitialized.set(true);
      if (!pendingVmEvents.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Processing: " + pendingVmEvents.size() + " pending VMEvents for Vertex: " +
              logIdentifier);
        }
        for (VertexManagerEvent vmEvent : pendingVmEvents) {
          vertexManager.onVertexManagerEventReceived(vmEvent);
        }
        pendingVmEvents.clear();
      }
    } catch (AMUserCodeException e) {
      String msg = "Exception in " + e.getSource()+ ", vertex:" + logIdentifier;
      LOG.error(msg, e);
      finished(VertexState.FAILED, VertexTerminationCause.AM_USERCODE_FAILURE,
          msg + ", " + e.getMessage() + ", " + ExceptionUtils.getStackTrace(e.getCause()));
      return VertexState.FAILED;
    }
    return VertexState.INITED;
  }

  private void assignVertexManager() throws TezException {
    // condition for skip initializing stage
    //   - VertexInputInitializerEvent is seen
    //   - VertexReconfigureDoneEvent is seen
    //        -  Reason to check whether VertexManager has complete its responsibility
    //           VertexManager actually is involved in the InputInitializer (InputInitializer generate events
    //           and send them to VertexManager which do some processing and send back to Vertex), so that means
    //           Input initializer will affect on the VertexManager and we couldn't skip the initializing step if  
    //           VertexManager has not completed its responsibility.
    //        -  Why using VertexReconfigureDoneEvent
    //           -  VertexReconfigureDoneEvent represent the case that user use API reconfigureVertex
    //              VertexReconfigureDoneEvent will be logged
    if (recoveryData != null
        && recoveryData.shouldSkipInit()) {
      // Replace the original VertexManager with NoOpVertexManager if the reconfiguration is done in the last AM attempt
      VertexConfigurationDoneEvent reconfigureDoneEvent = recoveryData.getVertexConfigurationDoneEvent();
      if (LOG.isInfoEnabled()) {
        LOG.info("VertexManager reconfiguration is done in the last AM Attempt"
            + ", use NoOpVertexManager to replace it, vertexId=" + logIdentifier);
        LOG.info("VertexReconfigureDoneEvent=" + reconfigureDoneEvent);
      }
      NonSyncByteArrayOutputStream out = new NonSyncByteArrayOutputStream();
      try {
        reconfigureDoneEvent.toProtoStream(out);
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to deserilize VertexReconfigureDoneEvent");
      }
      this.vertexManager = new VertexManager(
          VertexManagerPluginDescriptor.create(NoOpVertexManager.class.getName())
            .setUserPayload(UserPayload.create(ByteBuffer.wrap(out.toByteArray()))),
          dagUgi, this, appContext, stateChangeNotifier);
      return;
    }

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
          + pluginDesc.getClassName() + " on vertex: " + getLogIdentifier());
      vertexManager = new VertexManager(pluginDesc, dagUgi, this, appContext, stateChangeNotifier);
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
        vertexManager = new VertexManager(RootInputVertexManager
            .createConfigBuilder(vertexConf).build(),
            dagUgi, this, appContext, stateChangeNotifier);
      } else if (hasOneToOne && !hasCustom) {
        LOG.info("Setting vertexManager to InputReadyVertexManager for "
            + logIdentifier);
        vertexManager = new VertexManager(
            VertexManagerPluginDescriptor.create(InputReadyVertexManager.class.getName()),
            dagUgi, this, appContext, stateChangeNotifier);
      } else if (hasBipartite && !hasCustom) {
        LOG.info("Setting vertexManager to ShuffleVertexManager for "
            + logIdentifier);
        // shuffle vertex manager needs a conf payload
        vertexManager = new VertexManager(ShuffleVertexManager.createConfigBuilder(vertexConf).build(),
            dagUgi, this, appContext, stateChangeNotifier);
      } else {
        // schedule all tasks upon vertex start. Default behavior.
        LOG.info("Setting vertexManager to ImmediateStartVertexManager for "
            + logIdentifier);
        vertexManager = new VertexManager(
            VertexManagerPluginDescriptor.create(ImmediateStartVertexManager.class.getName()),
            dagUgi, this, appContext, stateChangeNotifier);
      }
    }
  }
  
  private static List<TaskAttemptIdentifier> getTaskAttemptIdentifiers(DAG dag, 
      List<TezTaskAttemptID> taIds) {
    List<TaskAttemptIdentifier> attempts = new ArrayList<TaskAttemptIdentifier>(taIds.size());
    String dagName = dag.getName();
    for (TezTaskAttemptID taId : taIds) {
      String vertexName = dag.getVertex(taId.getTaskID().getVertexID()).getName();
      attempts.add(getTaskAttemptIdentifier(dagName, vertexName, taId));
    }
    return attempts;
  }
  
  private static TaskAttemptIdentifier getTaskAttemptIdentifier(String dagName, String vertexName, 
      TezTaskAttemptID taId) {
    return new TaskAttemptIdentifierImpl(dagName, vertexName, taId);
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
          "Not connected to vertex " + otherVertex.getLogIdentifier() + " from vertex: " + vertex.logIdentifier);
      LOG.info("Edge initialized for connection to vertex " + otherVertex.getLogIdentifier() +
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

  public static class RecoverTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent vertexEvent) {
      VertexEventRecoverVertex recoverEvent = (VertexEventRecoverVertex) vertexEvent;
      // with desired state, for the cases that DAG is completed
      VertexState desiredState = recoverEvent.getDesiredState();
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
      default:
        LOG.info("Unhandled desired state provided by DAG"
            + ", vertex=" + vertex.logIdentifier
            + ", state=" + desiredState);
        return vertex.finished(VertexState.ERROR);
      }

      LOG.info("DAG informed vertices of its final completed state"
          + ", vertex=" + vertex.logIdentifier
          + ", desiredState=" + desiredState);
      return vertex.finished(recoverEvent.getDesiredState());
    }
  }
  
  public static class InitTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      // recover from recovery data (NEW->FAILED/KILLED)
      if (vertex.recoveryData != null
          && !vertex.recoveryData.isVertexInited()
          && vertex.recoveryData.isVertexFinished()) {
        VertexFinishedEvent finishedEvent = vertex.recoveryData.getVertexFinishedEvent();
        vertex.diagnostics.add(finishedEvent.getDiagnostics());
        return vertex.finished(finishedEvent.getState());
      }

      VertexState vertexState = VertexState.NEW;
      vertex.numInitedSourceVertices++;
      if (vertex.sourceVertices == null || vertex.sourceVertices.isEmpty() ||
          (vertex.numInitedSourceVertices == vertex.sourceVertices.size())) {
        vertexState = handleInitEvent(vertex);
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

    private VertexState handleInitEvent(VertexImpl vertex) {
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
        // this block must always return VertexState.INITIALIZING
        LOG.info("Num tasks is -1. Expecting VertexManager/InputInitializers/1-1 split"
            + " to set #tasks for the vertex " + vertex.getLogIdentifier());

        if (vertex.inputsWithInitializers != null) {
          if (vertex.recoveryData == null || !vertex.recoveryData.shouldSkipInit()) {
            LOG.info("Vertex will initialize from input initializer. " + vertex.logIdentifier);
            try {
              vertex.setupInputInitializerManager();
            } catch (TezException e) {
              String msg = "Fail to create InputInitializerManager, " + ExceptionUtils.getStackTrace(e);
              LOG.info(msg);
              return vertex.finished(VertexState.FAILED, VertexTerminationCause.INIT_FAILURE, msg);
            }
          }
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
          throw new TezUncheckedException(vertex.getLogIdentifier() +
          " has -1 tasks but does not have input initializers, " +
          "1-1 uninited sources or custom vertex manager to set it at runtime");
        }
      } else {
        LOG.info("Creating " + vertex.numTasks + " tasks for vertex: " + vertex.logIdentifier);
        vertex.createTasks();
        // this block may return VertexState.INITIALIZING
        if (vertex.inputsWithInitializers != null &&
            (vertex.recoveryData == null || !vertex.recoveryData.shouldSkipInit())) {
          LOG.info("Vertex will initialize from input initializer. " + vertex.logIdentifier);
          try {
            vertex.setupInputInitializerManager();
          } catch (TezException e) {
            String msg = "Fail to create InputInitializerManager, " + ExceptionUtils.getStackTrace(e);
            LOG.error(msg);
            return vertex.finished(VertexState.FAILED, VertexTerminationCause.INIT_FAILURE, msg);
          }
          return VertexState.INITIALIZING;
        }
        if (!vertex.uninitializedEdges.isEmpty()) {
          LOG.info("Vertex has uninitialized edges. " + vertex.logIdentifier);
          return VertexState.INITIALIZING;
        }
        LOG.info("Directly initializing vertex: " + vertex.logIdentifier);
        // vertex is completely configured. Send out notification now.
        vertex.maybeSendConfiguredEvent();
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
    return new RootInputInitializerManager(this, appContext, this.dagUgi, this.stateChangeNotifier);
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
        try {
          vertex.vertexManager.onRootVertexInitialized(liInitEvent.getInputName(), vertex
              .getAdditionalInputs().get(liInitEvent.getInputName()).getIODescriptor(),
              liInitEvent.getEvents());
        } catch (AMUserCodeException e) {
            String msg = "Exception in " + e.getSource() + ", vertex:" + vertex.getLogIdentifier();
            LOG.error(msg, e);
            vertex.finished(VertexState.FAILED,
                VertexTerminationCause.AM_USERCODE_FAILURE, msg
                + "," + ExceptionUtils.getStackTrace(e.getCause()));
            return VertexState.FAILED;
        }
      }

      vertex.numInitializedInputs++;
      if (vertex.numInitializedInputs == vertex.inputsWithInitializers.size()) {
        // All inputs initialized, shutdown the initializer.
        vertex.rootInputInitializerManager.shutdown();
        vertex.rootInputInitializerManager = null;
      }
      
      // the return of these events from the VM will complete initialization and move into 
      // INITED state if possible via InputDataInformationTransition

      return vertex.getState();
    }
  }

  public static class InputDataInformationTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      vertex.numInitializerCompletionsHandled++;
      VertexEventInputDataInformation iEvent = (VertexEventInputDataInformation) event;
      List<TezEvent> inputInfoEvents = iEvent.getEvents();
      try {
        if (inputInfoEvents != null && !inputInfoEvents.isEmpty()) {
          vertex.initGeneratedEvents.addAll(inputInfoEvents);
          vertex.handleRoutedTezEvents(inputInfoEvents, false);
        }
      } catch (AMUserCodeException e) {
        String msg = "Exception in " + e.getSource() + ", vertex:" + vertex.getLogIdentifier();
        LOG.error(msg, e);
        vertex.finished(VertexState.FAILED, VertexTerminationCause.AM_USERCODE_FAILURE, msg + ","
            + ExceptionUtils.getStackTrace(e.getCause()));
        return VertexState.FAILED;
      }

      // done. check if we need to do the initialization
      if (vertex.getState() == VertexState.INITIALIZING && vertex.initWaitsForRootInitializers) {
        if (vertex.numInitializedInputs == vertex.inputsWithInitializers.size()
            && vertex.numInitializerCompletionsHandled == vertex.inputsWithInitializers.size()) {
          // set the wait flag to false if all initializers are done and InputDataInformation are received from VM
          vertex.initWaitsForRootInitializers = false;
        }
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
      vertex.startTimeRequested = vertex.clock.getTime();
      LOG.info("Source vertex started: " + startEvent.getSourceVertexId() +
          " for vertex: " + vertex.logIdentifier + " numStartedSources: " +
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
      vertex.startSignalPending = true;
      vertex.startTimeRequested = vertex.clock.getTime();
    }

  }
  
  public static class StartTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      Preconditions.checkState(vertex.getState() == VertexState.INITED,
          "Unexpected state " + vertex.getState() + " for " + vertex.logIdentifier);
      // if the start signal is pending this event is a fake start event to trigger this transition
      if (!vertex.startSignalPending) {
        vertex.startTimeRequested = vertex.clock.getTime();
      }
      return vertex.startVertex();
    }
  }
  
  private void maybeSendConfiguredEvent() {
    // the vertex is fully configured by the time it starts. Always notify completely configured
    // unless the vertex manager has told us that it is going to reconfigure it further
    Preconditions.checkState(canInitVertex(), "Vertex: " + getLogIdentifier());
    if (!this.vertexToBeReconfiguredByManager) {
      // this vertex will not be reconfigured by its manager
      if (completelyConfiguredSent.compareAndSet(false, true)) {
        stateChangeNotifier.stateChanged(vertexId, new VertexStateUpdate(vertexName,
            org.apache.tez.dag.api.event.VertexState.CONFIGURED));
        logVertexConfigurationDoneEvent();
      }
    }
  }

  private VertexState startVertex() {
    Preconditions.checkState(getState() == VertexState.INITED,
        "Vertex must be inited " + logIdentifier);

    if (recoveryData != null && recoveryData.isVertexStarted()) {
      VertexStartedEvent vertexStartedEvent = recoveryData.getVertexStartedEvent();
      this.startedTime = vertexStartedEvent.getStartTime();
    } else {
      this.startedTime = clock.getTime();
    }

    try {
      vertexManager.onVertexStarted(getTaskAttemptIdentifiers(dag, pendingReportedSrcCompletions));
    } catch (AMUserCodeException e) {
      String msg = "Exception in " + e.getSource() +", vertex=" + logIdentifier;
      LOG.error(msg, e);
      addDiagnostic(msg + "," + ExceptionUtils.getStackTrace(e.getCause()));
      tryEnactKill(VertexTerminationCause.AM_USERCODE_FAILURE, TaskTerminationCause.AM_USERCODE_FAILURE);
      return VertexState.TERMINATING;
    }
    pendingReportedSrcCompletions.clear();
    logJobHistoryVertexStartedEvent();
    
    // the vertex is fully configured by the time it starts. Always notify completely configured
    // unless the vertex manager has told us that it is going to reconfigure it further.
    // If the vertex was pre-configured then the event would have been sent out earlier. Calling again 
    // would be a no-op. If the vertex was not fully configured and waiting for that to complete then
    // we would start immediately after that. Either parallelism updated (now) or IPO changed (future) 
    // or vertex added (future). Simplify these cases by sending the event now automatically for the 
    // user as if they had invoked the planned()/done() API's.
    maybeSendConfiguredEvent();
    
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

  void abortVertex(final VertexStatus.State finalState) {
    if (this.aborted.getAndSet(true)) {
      LOG.info("Ignoring multiple aborts for vertex: " + logIdentifier);
      return;
    }

    if (outputCommitters != null) {
      LOG.info("Invoking committer abort for vertex, vertexId=" + logIdentifier);
      try {
        TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(), vertexId);
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
      } finally {
        appContext.getHadoopShim().clearHadoopCallerContext();
      }
    }
    if (finishTime == 0) {
      setFinishTime();
    }
  }

  private void mayBeConstructFinalFullCounters() {
    // Calculating full-counters. This should happen only once for the vertex.
    synchronized (this.fullCountersLock) {
      // TODO this is broken after rerun
      if (this.fullCounters != null) {
        // Already constructed. Just return.
        return;
      }
      this.constructFinalFullcounters();
    }
  }

  private VertexStatisticsImpl constructStatistics() {
    return completedTasksStatsCache;
  }

  @Private
  public void constructFinalFullcounters() {
    this.fullCounters = new TezCounters();
    this.fullCounters.incrAllCounters(counters);
    this.vertexStats = new VertexStats();

    for (Task t : this.tasks.values()) {
      vertexStats.updateStats(t.getReport());
      TezCounters counters = t.getCounters();
      this.fullCounters.incrAllCounters(counters);
    }
  }

  private static class RootInputInitFailedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventRootInputFailed fe = (VertexEventRootInputFailed) event;
      String msg = "Vertex Input: " + fe.getInputName()
          + " initializer failed, vertex=" + vertex.getLogIdentifier();
      LOG.error(msg, fe.getError());
      if (vertex.getState() == VertexState.RUNNING) {
        vertex.addDiagnostic(msg
              + ", " + ExceptionUtils.getStackTrace(fe.getError().getCause()));
        vertex.tryEnactKill(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE,
            TaskTerminationCause.AM_USERCODE_FAILURE);
        return VertexState.TERMINATING;
      } else {
        vertex.finished(VertexState.FAILED,
            VertexTerminationCause.ROOT_INPUT_INIT_FAILURE, msg
              + ", " + ExceptionUtils.getStackTrace(fe.getError().getCause()));
        return VertexState.FAILED;
      }
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
      vertex.addDiagnostic("Vertex received Kill in INITED state.");
      vertex.finished(VertexState.KILLED);
    }
  }

  private static class TerminateInitingVertexTransition extends TerminateInitedVertexTransition {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      super.transition(vertex, event);
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
        case DAG_TERMINATED: vertex.tryEnactKill(trigger, TaskTerminationCause.DAG_KILL); break;
        case OWN_TASK_FAILURE: vertex.tryEnactKill(trigger, TaskTerminationCause.OTHER_TASK_FAILURE); break;
        case ROOT_INPUT_INIT_FAILURE:
        case COMMIT_FAILURE:
        case INVALID_NUM_OF_TASKS:
        case INIT_FAILURE:
        case INTERNAL_ERROR:
        case AM_USERCODE_FAILURE:
        case VERTEX_RERUN_IN_COMMITTING:
        case VERTEX_RERUN_AFTER_COMMIT:
        case OTHER_VERTEX_FAILURE: vertex.tryEnactKill(trigger, TaskTerminationCause.OTHER_VERTEX_FAILURE); break;
        default://should not occur
          throw new TezUncheckedException("VertexKilledTransition: event.terminationCause is unexpected: " + trigger);
      }

      // TODO: Metrics
      //job.metrics.endRunningJob(job);
    }
  }

  private static class VertexKilledWhileCommittingTransition
    implements SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTermination vet = (VertexEventTermination) event;
      VertexTerminationCause trigger = vet.getTerminationCause();
      String msg = "Vertex received Kill while in COMMITTING state, terminationCause="
          + trigger +", vertex=" + vertex.logIdentifier;
      LOG.info(msg);
      vertex.addDiagnostic(msg);
      vertex.trySetTerminationCause(trigger);
      vertex.cancelCommits();
    }
  }

  private static class VertexManagerUserCodeErrorTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {
    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventManagerUserCodeError errEvent = ((VertexEventManagerUserCodeError) event);
      AMUserCodeException e = errEvent.getError();
      String msg = "Exception in " + e.getSource() + ", vertex:" + vertex.getLogIdentifier();
      LOG.error(msg, e);
      
      if (vertex.getState() == VertexState.RUNNING || vertex.getState() == VertexState.COMMITTING) {
        vertex.addDiagnostic(msg + "," + ExceptionUtils.getStackTrace(e.getCause()));
        vertex.tryEnactKill(VertexTerminationCause.AM_USERCODE_FAILURE,
            TaskTerminationCause.AM_USERCODE_FAILURE);
        vertex.cancelCommits();
        return VertexState.TERMINATING;
      } else {
        vertex.finished(VertexState.FAILED,
            VertexTerminationCause.AM_USERCODE_FAILURE, msg
              + ", " + ExceptionUtils.getStackTrace(e.getCause()));
        return VertexState.FAILED;
      }
    }
  }
  
  /**
   * Here, the Vertex is being told that one of it's source task-attempts
   * completed.
   */
  private static class SourceTaskAttemptCompletedEventTransition implements
  MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {
    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTaskAttemptCompleted completionEvent =
          ((VertexEventSourceTaskAttemptCompleted) event).getCompletionEvent();
      LOG.info("Source task attempt completed for vertex: " + vertex.getLogIdentifier()
            + " attempt: " + completionEvent.getTaskAttemptId()
            + " with state: " + completionEvent.getTaskAttemptState()
            + " vertexState: " + vertex.getState());


      if (TaskAttemptStateInternal.SUCCEEDED.equals(completionEvent
          .getTaskAttemptState())) {
        vertex.numSuccessSourceAttemptCompletions++;

        if (vertex.getState() == VertexState.RUNNING) {
          try {
            // Inform the vertex manager about the source task completing.
            TezTaskAttemptID taId = completionEvent.getTaskAttemptId();
            vertex.vertexManager.onSourceTaskCompleted(
                getTaskAttemptIdentifier(vertex.dag.getName(), 
                vertex.dag.getVertex(taId.getTaskID().getVertexID()).getName(), 
                taId));
          } catch (AMUserCodeException e) {
            String msg = "Exception in " + e.getSource() + ", vertex:" + vertex.getLogIdentifier();
            LOG.error(msg, e);
            vertex.addDiagnostic(msg + "," + ExceptionUtils.getStackTrace(e.getCause()));
            vertex.tryEnactKill(VertexTerminationCause.AM_USERCODE_FAILURE,
                TaskTerminationCause.AM_USERCODE_FAILURE);
            return VertexState.TERMINATING;
          }
        } else {
          vertex.pendingReportedSrcCompletions.add(completionEvent.getTaskAttemptId());
        }
      }
      return vertex.getState();
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
      if (vertex.completedTasksStatsCache == null) {
        vertex.resetCompletedTaskStatsCache(false);
      }
      boolean forceTransitionToKillWait = false;
      vertex.completedTaskCount++;
      VertexEventTaskCompleted taskEvent = (VertexEventTaskCompleted) event;
      Task task = vertex.tasks.get(taskEvent.getTaskID());
      if (taskEvent.getState() == TaskState.SUCCEEDED) {
        taskSucceeded(vertex, task);
        if (!vertex.completedTasksStatsCache.containsTask(task.getTaskId())) {
          vertex.completedTasksStatsCache.addTask(task.getTaskId());
          vertex.completedTasksStatsCache.mergeFrom(((TaskImpl) task).getStatistics());
        }
      } else if (taskEvent.getState() == TaskState.FAILED) {
        taskFailed(vertex, task);
        if (vertex.failedTaskCount * 100 > vertex.maxFailuresPercent * vertex.numTasks) {
          LOG.info("Failing vertex: " + vertex.logIdentifier +
                  " because task failed: " + taskEvent.getTaskID());
          vertex.tryEnactKill(VertexTerminationCause.OWN_TASK_FAILURE, TaskTerminationCause.OTHER_TASK_FAILURE);
          forceTransitionToKillWait = true;
        }
      } else if (taskEvent.getState() == TaskState.KILLED) {
        taskKilled(vertex, task);
      }

      VertexState state = VertexImpl.checkTasksForCompletion(vertex);
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
      vertex.resetCompletedTaskStatsCache(true);
    }
  }

  private static class VertexNoTasksCompletedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      return VertexImpl.checkTasksForCompletion(vertex);
    }
  }

  private static class TaskCompletedAfterVertexSuccessTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {
    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTaskCompleted vEvent = (VertexEventTaskCompleted) event;
      VertexState finalState;
      String diagnosticMsg;
      if (vEvent.getState() == TaskState.FAILED) {
        finalState = VertexState.FAILED;
        diagnosticMsg = "Vertex " + vertex.logIdentifier +" failed as task " + vEvent.getTaskID() +
          " failed after vertex succeeded.";
      } else {
        finalState = VertexState.ERROR;
        diagnosticMsg = "Vertex " + vertex.logIdentifier + " error as task " + vEvent.getTaskID() +
            " completed with state " + vEvent.getState() + " after vertex succeeded.";
      }
      LOG.info(diagnosticMsg);
      vertex.finished(finalState, VertexTerminationCause.OWN_TASK_FAILURE, diagnosticMsg);
      return finalState;
    }
  }

  private static class TaskRescheduledWhileCommittingTransition implements 
    SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      // terminate any running tasks
      String diagnosticMsg = vertex.getLogIdentifier() + " failed due to in-committing rescheduling of "
          + ((VertexEventTaskReschedule)event).getTaskID();
      LOG.info(diagnosticMsg);
      vertex.addDiagnostic(diagnosticMsg);
      vertex.tryEnactKill(VertexTerminationCause.VERTEX_RERUN_IN_COMMITTING,
          TaskTerminationCause.TASK_RESCHEDULE_IN_COMMITTING);
      vertex.cancelCommits();
    }
  }

  private static class TaskRescheduledAfterVertexSuccessTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      if (vertex.outputCommitters == null // no committer
          || vertex.outputCommitters.isEmpty() // no committer
          || !vertex.commitVertexOutputs) { // committer does not commit on vertex success
        LOG.info(vertex.getLogIdentifier() + " back to running due to rescheduling "
            + ((VertexEventTaskReschedule)event).getTaskID());
        (new TaskRescheduledTransition()).transition(vertex, event);
        // inform the DAG that we are re-running
        vertex.eventHandler.handle(new DAGEventVertexReRunning(vertex.getVertexId()));
        // back to running. so reset final cached stats
        vertex.finalStatistics = null;
        return VertexState.RUNNING;
      }

      // terminate any running tasks
      String diagnosticMsg = vertex.getLogIdentifier() + " failed due to post-commit rescheduling of "
          + ((VertexEventTaskReschedule)event).getTaskID();
      LOG.info(diagnosticMsg);
      vertex.tryEnactKill(VertexTerminationCause.OWN_TASK_FAILURE,
          TaskTerminationCause.OWN_TASK_FAILURE);
      vertex.finished(VertexState.FAILED, VertexTerminationCause.OWN_TASK_FAILURE, diagnosticMsg);
      return VertexState.FAILED;
    }
  }

  private void commitCompleted(VertexEventCommitCompleted commitCompletedEvent) {
    Preconditions.checkState(commitFutures.remove(commitCompletedEvent.getOutputName()) != null,
        "Unknown commit:" + commitCompletedEvent.getOutputName() + ", vertex=" + logIdentifier);
    if (commitCompletedEvent.isSucceeded()) {
      LOG.info("Commit succeeded for output:" + commitCompletedEvent.getOutputName()
          + ", vertexId=" + logIdentifier);
    } else {
      String diag = "Commit failed for output:" + commitCompletedEvent.getOutputName()
          + ", vertexId=" + logIdentifier + ", "
          + ExceptionUtils.getStackTrace(commitCompletedEvent.getException());;
      LOG.info(diag);
      addDiagnostic(diag);
      trySetTerminationCause(VertexTerminationCause.COMMIT_FAILURE);
      cancelCommits();
    }
  }

  private static class CommitCompletedTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      vertex.commitCompleted((VertexEventCommitCompleted)event);
      return checkCommitsForCompletion(vertex);
    }
  }

  private void cancelCommits() {
    if (!this.commitCanceled.getAndSet(true)) {
      for (Map.Entry<String, ListenableFuture<Void>> entry : commitFutures.entrySet()) {
        LOG.info("Canceling commit of output:" + entry.getKey() + ", vertexId=" + logIdentifier);
        entry.getValue().cancel(true);
      }
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
  MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {
    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexEventRouteEvent rEvent = (VertexEventRouteEvent) event;
      List<TezEvent> tezEvents = rEvent.getEvents();
      try {
        vertex.handleRoutedTezEvents(tezEvents, false);
      } catch (AMUserCodeException e) {
        String msg = "Exception in " + e.getSource() + ", vertex=" + vertex.getLogIdentifier();
        LOG.error(msg, e);
        if (vertex.getState() == VertexState.RUNNING || vertex.getState() == VertexState.COMMITTING) {
          vertex.addDiagnostic(msg + ", " + e.getMessage() + ", " + ExceptionUtils.getStackTrace(e.getCause()));
          vertex.tryEnactKill(VertexTerminationCause.AM_USERCODE_FAILURE, TaskTerminationCause.AM_USERCODE_FAILURE);
          vertex.cancelCommits();
          return VertexState.TERMINATING;
        } else {
          vertex.finished(VertexState.FAILED, VertexTerminationCause.AM_USERCODE_FAILURE,
              msg + "," + ExceptionUtils.getStackTrace(e.getCause()));
          return VertexState.FAILED;
        }
      }
      return vertex.getState();
    }
  }
  
  @Override
  public TaskAttemptEventInfo getTaskAttemptTezEvents(TezTaskAttemptID attemptID,
      int fromEventId, int preRoutedFromEventId, int maxEvents) {
    Task task = getTask(attemptID.getTaskID());
    ArrayList<TezEvent> events = task.getTaskAttemptTezEvents(
        attemptID, preRoutedFromEventId, maxEvents);
    int nextPreRoutedFromEventId = preRoutedFromEventId + events.size();
    int nextFromEventId = fromEventId;
    onDemandRouteEventsReadLock.lock();
    try {
      int currEventCount = onDemandRouteEvents.size();
      try {
        if (currEventCount > fromEventId) {
          if (events != TaskImpl.EMPTY_TASK_ATTEMPT_TEZ_EVENTS) {
            events.ensureCapacity(maxEvents);
          } else {
            events = Lists.newArrayListWithCapacity(maxEvents);
          }
          int numPreRoutedEvents = events.size();
          int taskIndex = attemptID.getTaskID().getId();
          Preconditions.checkState(taskIndex < tasks.size(), "Invalid task index for TA: " + attemptID
              + " vertex: " + getLogIdentifier());
          boolean isFirstEvent = true;
          boolean firstEventObsoleted = false;
          for (nextFromEventId = fromEventId; nextFromEventId < currEventCount; ++nextFromEventId) {
            boolean earlyExit = false;
            if (events.size() == maxEvents) {
              break;
            }
            EventInfo eventInfo = onDemandRouteEvents.get(nextFromEventId);
            if (eventInfo.isObsolete) {
              // ignore obsolete events
              firstEventObsoleted = true;
              continue;
            }
            TezEvent tezEvent = eventInfo.tezEvent;
            switch(tezEvent.getEventType()) {
            case INPUT_FAILED_EVENT:
            case DATA_MOVEMENT_EVENT:
            case COMPOSITE_DATA_MOVEMENT_EVENT:
              {
                int srcTaskIndex = eventInfo.eventTaskIndex;
                Edge srcEdge = eventInfo.eventEdge;
                PendingEventRouteMetadata pendingRoute = null;
                if (isFirstEvent) {
                  // the first event is the one that can have pending routes because its expanded
                  // events had not been completely sent in the last round.
                  isFirstEvent = false;
                  pendingRoute = srcEdge.removePendingEvents(attemptID);
                  if (pendingRoute != null) {
                    // the first event must match the pending route event
                    // the only reason it may not match is if in between rounds that event got
                    // obsoleted
                    if(tezEvent != pendingRoute.getTezEvent()) {
                      Preconditions.checkState(firstEventObsoleted);
                      // pending routes can be ignored for obsoleted events
                      pendingRoute = null;
                    }
                  }
                }
                if (!srcEdge.maybeAddTezEventForDestinationTask(tezEvent, attemptID, srcTaskIndex,
                    events, maxEvents, pendingRoute)) {
                  // not enough space left for this iteration events.
                  // Exit and start from here next time
                  earlyExit = true;
                }
              }
              break;
            case ROOT_INPUT_DATA_INFORMATION_EVENT:
              {
                InputDataInformationEvent riEvent = (InputDataInformationEvent) tezEvent.getEvent();
                if (riEvent.getTargetIndex() == taskIndex) {
                  events.add(tezEvent);
                }
              }
              break;
            default:
              throw new TezUncheckedException("Unexpected event type for task: "
                  + tezEvent.getEventType());
            }
            if (earlyExit) {
              break;
            }
          }
          int numEventsSent = events.size() - numPreRoutedEvents;
          if (numEventsSent > 0) {
            StringBuilder builder = new StringBuilder();
            builder.append("Sending ").append(attemptID).append(" ")
                .append(numEventsSent)
                .append(" events [").append(fromEventId).append(",").append(nextFromEventId)
                .append(") total ").append(currEventCount).append(" ")
                .append(getLogIdentifier());
            LOG.info(builder.toString());
          }
        }
      } catch (AMUserCodeException e) {
        String msg = "Exception in " + e.getSource() + ", vertex=" + getLogIdentifier();
        LOG.error(msg, e);
        eventHandler.handle(new VertexEventManagerUserCodeError(getVertexId(), e));
        nextFromEventId = fromEventId;
        events.clear();
      }
    } finally {
      onDemandRouteEventsReadLock.unlock();
    }
    if (!events.isEmpty()) {
      for (int i=(events.size() - 1); i>=0; --i) {
        TezEvent lastEvent = events.get(i);
              // record the last event sent by the AM to the task
        EventType lastEventType = lastEvent.getEventType();
        // if the following changes then critical path logic/recording may need revision
        if (lastEventType == EventType.COMPOSITE_DATA_MOVEMENT_EVENT ||
            lastEventType == EventType.COMPOSITE_ROUTED_DATA_MOVEMENT_EVENT ||
            lastEventType == EventType.DATA_MOVEMENT_EVENT ||
            lastEventType == EventType.ROOT_INPUT_DATA_INFORMATION_EVENT) {
          task.getAttempt(attemptID).setLastEventSent(lastEvent);
          break;
        }
      }
    }
    return new TaskAttemptEventInfo(nextFromEventId, events, nextPreRoutedFromEventId);
  }

  private void handleRoutedTezEvents(List<TezEvent> tezEvents, boolean isPendingEvents) throws AMUserCodeException {
    for(TezEvent tezEvent : tezEvents) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Vertex: " + getLogIdentifier() + " routing event: "
            + tezEvent.getEventType());
      }
      EventMetaData sourceMeta = tezEvent.getSourceInfo();
      switch(tezEvent.getEventType()) {
      case CUSTOM_PROCESSOR_EVENT:
        {
          // set version as app attempt id
          ((CustomProcessorEvent) tezEvent.getEvent()).setVersion(
            appContext.getApplicationAttemptId().getAttemptId());
          // route event to task
          EventMetaData destinationMeta = tezEvent.getDestinationInfo();
          Task targetTask = getTask(destinationMeta.getTaskAttemptID().getTaskID());
          targetTask.registerTezEvent(tezEvent);
        }
        break;
      case INPUT_FAILED_EVENT:
      case DATA_MOVEMENT_EVENT:
      case COMPOSITE_DATA_MOVEMENT_EVENT:
        {
          if (isEventFromVertex(this, sourceMeta)) {
            // event from this vertex. send to destination vertex
            TezTaskAttemptID srcTaId = sourceMeta.getTaskAttemptID();
            if (tezEvent.getEventType() == EventType.DATA_MOVEMENT_EVENT) {
              ((DataMovementEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
            } else if (tezEvent.getEventType() == EventType.COMPOSITE_DATA_MOVEMENT_EVENT) {
              ((CompositeDataMovementEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
            } else {
              ((InputFailedEvent) tezEvent.getEvent()).setVersion(srcTaId.getId());
            }
            Vertex destVertex = getDAG().getVertex(sourceMeta.getEdgeVertexName());
            Edge destEdge = targetVertices.get(destVertex);
            if (destEdge == null) {
              throw new TezUncheckedException("Bad destination vertex: " +
                  sourceMeta.getEdgeVertexName() + " for event vertex: " +
                  getLogIdentifier());
            }
            eventHandler.handle(new VertexEventRouteEvent(destVertex
                .getVertexId(), Collections.singletonList(tezEvent)));
          } else {
            if (tasksNotYetScheduled) {
              // this is only needed to support mixed mode routing. Else for
              // on demand routing events can be directly added to taskEvents
              // when legacy routing is removed then pending task events can be
              // removed.
              pendingTaskEvents.add(tezEvent);
            } else {
              // event not from this vertex. must have come from source vertex.
              int srcTaskIndex = sourceMeta.getTaskAttemptID().getTaskID().getId();
              Vertex edgeVertex = getDAG().getVertex(sourceMeta.getTaskVertexName());
              Edge srcEdge = sourceVertices.get(edgeVertex);
              if (srcEdge == null) {
                throw new TezUncheckedException("Bad source vertex: " +
                    sourceMeta.getTaskVertexName() + " for destination vertex: " +
                    getLogIdentifier());
              }
              if (srcEdge.hasOnDemandRouting()) {
                processOnDemandEvent(tezEvent, srcEdge, srcTaskIndex);
              } else {
                // send to tasks            
                srcEdge.sendTezEventToDestinationTasks(tezEvent);
              }
            }
          }
        }
        break;
      case ROOT_INPUT_DATA_INFORMATION_EVENT:
      {   
        checkEventSourceMetadata(this, sourceMeta);
        if (tasksNotYetScheduled) {
          // this is only needed to support mixed mode routing. Else for
          // on demand routing events can be directly added to taskEvents
          // when legacy routing is removed then pending task events can be
          // removed.
          pendingTaskEvents.add(tezEvent);          
        } else {
          InputDataInformationEvent riEvent = (InputDataInformationEvent) tezEvent.getEvent();
          Task targetTask = getTask(riEvent.getTargetIndex());
          targetTask.registerTezEvent(tezEvent);
        }
      }
        break;
      case VERTEX_MANAGER_EVENT:
      {
        // VM events on task success only can be changed as part of TEZ-1532
        VertexManagerEvent vmEvent = (VertexManagerEvent) tezEvent.getEvent();
        Vertex target = getDAG().getVertex(vmEvent.getTargetVertexName());
        Preconditions.checkArgument(target != null,
            "Event sent to unkown vertex: " + vmEvent.getTargetVertexName());
        TezTaskAttemptID srcTaId = sourceMeta.getTaskAttemptID();
        if (srcTaId.getTaskID().getVertexID().equals(vertexId)) {
          // this is the producer tasks' vertex
          vmEvent.setProducerAttemptIdentifier(
              getTaskAttemptIdentifier(dag.getName(), getName(), srcTaId));
        }
        if (target == this) {
          if (!vmIsInitialized.get()) {
            // The VM hasn't been setup yet, defer event consumption
            pendingVmEvents.add(vmEvent);
          } else {
            vertexManager.onVertexManagerEventReceived(vmEvent);
          }
        } else {
          checkEventSourceMetadata(this, sourceMeta);
          eventHandler.handle(new VertexEventRouteEvent(target
              .getVertexId(), Collections.singletonList(tezEvent)));
        }
      }
        break;
      case ROOT_INPUT_INITIALIZER_EVENT:
      {
        InputInitializerEvent riEvent = (InputInitializerEvent) tezEvent.getEvent();
        Vertex target = getDAG().getVertex(riEvent.getTargetVertexName());
        Preconditions.checkArgument(target != null,
            "Event sent to unknown vertex: " + riEvent.getTargetVertexName());
        riEvent.setSourceVertexName(tezEvent.getSourceInfo().getTaskVertexName());
        if (target == this) {
          if (rootInputDescriptors == null ||
              !rootInputDescriptors.containsKey(riEvent.getTargetInputName())) {
            throw new TezUncheckedException(
                "InputInitializerEvent targeted at unknown initializer on vertex " +
                    logIdentifier + ", Event=" + riEvent);
          }
          if (getState() == VertexState.NEW) {
            pendingInitializerEvents.add(tezEvent);
          } else  if (getState() == VertexState.INITIALIZING) {
            rootInputInitializerManager.handleInitializerEvents(Collections.singletonList(tezEvent));
          } else {
            // Currently, INITED and subsequent states means Initializer complete / failure
            if (LOG.isDebugEnabled()) {
              LOG.debug("Dropping event" + tezEvent + " since state is not INITIALIZING in "
                  + getLogIdentifier() + ", state=" + getState());
            }
          }
        } else {
          checkEventSourceMetadata(this, sourceMeta);
          eventHandler.handle(new VertexEventRouteEvent(target.getVertexId(),
              Collections.singletonList(tezEvent)));
        }
      }
        break;
      case INPUT_READ_ERROR_EVENT:
        {
          checkEventSourceMetadata(this, sourceMeta);
          Edge srcEdge = sourceVertices.get(this.getDAG().getVertex(
              sourceMeta.getEdgeVertexName()));
          srcEdge.sendTezEventToSourceTasks(tezEvent);
        }
        break;
      default:
        throw new TezUncheckedException("Unhandled tez event type: "
            + tezEvent.getEventType());
      }
    }
  }
  
  private void processOnDemandEvent(TezEvent tezEvent, Edge srcEdge, int srcTaskIndex) {
    onDemandRouteEventsWriteLock.lock();
    try {
      if (tezEvent.getEventType() == EventType.DATA_MOVEMENT_EVENT ||
          tezEvent.getEventType() == EventType.COMPOSITE_DATA_MOVEMENT_EVENT) {
        // Prevent any failed task (due to INPUT_FAILED_EVENT) sending events downstream. E.g LLAP
        if (failedTaskAttemptIDs.contains(tezEvent.getSourceInfo().getTaskAttemptID())) {
          return;
        }
      }
      onDemandRouteEvents.add(new EventInfo(tezEvent, srcEdge, srcTaskIndex));
      if (tezEvent.getEventType() == EventType.INPUT_FAILED_EVENT) {
        for (EventInfo eventInfo : onDemandRouteEvents) {
          if (eventInfo.eventEdge == srcEdge 
              && eventInfo.tezEvent.getSourceInfo().getTaskAttemptID().equals(
                 tezEvent.getSourceInfo().getTaskAttemptID())
              && (eventInfo.tezEvent.getEventType() == EventType.DATA_MOVEMENT_EVENT
                  || eventInfo.tezEvent
                      .getEventType() == EventType.COMPOSITE_DATA_MOVEMENT_EVENT)) {
            // any earlier data movement events from the same source
            // edge+task
            // can be obsoleted by an input failed event from the
            // same source edge+task
            eventInfo.isObsolete = true;
            failedTaskAttemptIDs.add(tezEvent.getSourceInfo().getTaskAttemptID());
          }
        }
      }
    } finally {
      onDemandRouteEventsWriteLock.unlock();
    }
  }

  private static class InternalErrorTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      String msg = "Invalid event on Vertex " + vertex.getLogIdentifier();
      LOG.error(msg);
      vertex.eventHandler.handle(new DAGEventDiagnosticsUpdate(vertex.getDAGId(), msg));
      vertex.setFinishTime();
      vertex.trySetTerminationCause(VertexTerminationCause.INTERNAL_ERROR);
      vertex.cancelCommits();
      vertex.finished(VertexState.ERROR);
    }
  }

  private void setupInputInitializerManager() throws TezException {
    rootInputInitializerManager = createRootInputInitializerManager(
        getDAG().getName(), getName(), getVertexId(),
        eventHandler, getTotalTasks(),
        appContext.getTaskScheduler().getNumClusterNodes(),
        getTaskResource(),
        appContext.getTaskScheduler().getTotalResources(taskSchedulerIdentifier));
    List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>>
        inputList = Lists.newArrayListWithCapacity(inputsWithInitializers.size());
    for (String inputName : inputsWithInitializers) {
      inputList.add(rootInputDescriptors.get(inputName));
    }
    LOG.info("Starting " + inputsWithInitializers.size() + " inputInitializers for vertex " +
        logIdentifier);
    initWaitsForRootInitializers = true;
    rootInputInitializerManager.runInputInitializers(inputList);
    // Send pending rootInputInitializerEvents
    rootInputInitializerManager.handleInitializerEvents(pendingInitializerEvents);
    pendingInitializerEvents.clear();
  }

  private static class VertexStateChangedCallback
      implements OnStateChangedCallback<VertexState, VertexImpl> {

    @Override
    public void onStateChanged(VertexImpl vertex, VertexState vertexState) {
      vertex.stateChangeNotifier.stateChanged(vertex.getVertexId(),
          new VertexStateUpdate(vertex.getName(), convertInternalState(
              vertexState, vertex.getVertexId())));
    }

    private org.apache.tez.dag.api.event.VertexState convertInternalState(VertexState vertexState,
                                                                          TezVertexID vertexId) {
      switch (vertexState) {
        case RUNNING:
          return org.apache.tez.dag.api.event.VertexState.RUNNING;
        case SUCCEEDED:
          return org.apache.tez.dag.api.event.VertexState.SUCCEEDED;
        case FAILED:
          return org.apache.tez.dag.api.event.VertexState.FAILED;
        case KILLED:
          return org.apache.tez.dag.api.event.VertexState.KILLED;
        case INITIALIZING:
          return org.apache.tez.dag.api.event.VertexState.INITIALIZING;
        case NEW:
        case INITED:
        case ERROR:
        case TERMINATING:
        default:
          throw new TezUncheckedException(
              "Not expecting state updates for state: " + vertexState + ", VertexID: " + vertexId);
      }
    }
  }

  private static class VertexCommitCallback implements FutureCallback<Void>{

    private String outputName;
    private VertexImpl vertex;

    public VertexCommitCallback(VertexImpl vertex, String outputName) {
      this.vertex = vertex;
      this.outputName = outputName;
    }

    @Override
    public void onSuccess(Void result) {
      vertex.getEventHandler().handle(
          new VertexEventCommitCompleted(vertex.vertexId, outputName, true, null));
    }

    @Override
    public void onFailure(Throwable t) {
      vertex.getEventHandler().handle(
          new VertexEventCommitCompleted(vertex.vertexId, outputName, false, t));
    }

  }

  @Override
  public void setInputVertices(Map<Vertex, Edge> inVertices) {
    writeLock.lock();
    try {
      this.sourceVertices = inVertices;
      for (Vertex vertex : sourceVertices.keySet()) {
        addIO(vertex.getName());
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void setOutputVertices(Map<Vertex, Edge> outVertices) {
    writeLock.lock();
    try {
      this.targetVertices = outVertices;
      for (Vertex vertex : targetVertices.keySet()) {
        addIO(vertex.getName());
      }
    } finally {
      writeLock.unlock();;
    }
  }

  @Override
  public void setAdditionalInputs(List<RootInputLeafOutputProto> inputs) {
    LOG.info("Setting " + inputs.size() + " additional inputs for vertex" + this.logIdentifier);
    this.rootInputDescriptors = Maps.newHashMapWithExpectedSize(inputs.size());
    for (RootInputLeafOutputProto input : inputs) {
      addIO(input.getName());
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
  
  // not taking a lock by design. Speculator callbacks to the vertex will take locks if needed
  @Override
  public void handleSpeculatorEvent(SpeculatorEvent event) {
    if (isSpeculationEnabled()) {
      speculator.handle(event);
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
    LOG.info("Setting " + outputs.size() + " additional outputs for vertex " + this.logIdentifier);
    this.additionalOutputs = Maps.newHashMapWithExpectedSize(outputs.size());
    this.outputCommitters = Maps.newHashMapWithExpectedSize(outputs.size());
    for (RootInputLeafOutputProto output : outputs) {
      addIO(output.getName());
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
    readLock.lock();
    try {
      return this.rootInputDescriptors;
    } finally {
      readLock.unlock();
    }
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
    readLock.lock();
    try {
      return Collections.unmodifiableMap(this.sourceVertices);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Map<Vertex, Edge> getOutputVertices() {
    return Collections.unmodifiableMap(this.targetVertices);
  }
  
  @Override
  public VertexStatistics getStatistics() {
    readLock.lock();
    try {
      if (inTerminalState()) {
        Preconditions.checkState(this.finalStatistics != null);
        return this.finalStatistics;
      }
      return constructStatistics();
    } finally {
      readLock.unlock();
    }
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
    return dag;
  }

  private TezDAGID getDAGId() {
    return getDAG().getID();
  }

  public Resource getTaskResource() {
    readLock.lock();
    try {
      return taskResource;
    } finally {
      readLock.unlock();
    }
  }
  
  void addIO(String name) {
    ioIndices.put(StringInterner.weakIntern(name), ioIndices.size());
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

  @Override
  public List<InputSpec> getInputSpecList(int taskIndex) throws AMUserCodeException {
    // For locking strategy, please refer to getOutputSpecList()
    readLock.lock();
    List<InputSpec> inputSpecList = null;
    try {
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
    } finally {
      readLock.unlock();
    }

    for(Vertex vertex : getInputVertices().keySet()) {
      /**
       * It is possible that setParallelism is in the middle of processing in target vertex with
       * its write lock. So we need to get inputspec by acquiring read lock in target vertex to
       * get consistent view.
       * Refer TEZ-2251
       */
      InputSpec inputSpec = ((VertexImpl) vertex).getDestinationSpecFor(this, taskIndex);
      // TODO DAGAM This should be based on the edge type.
      inputSpecList.add(inputSpec);
    }
    return inputSpecList;
  }

  @Override
  public List<OutputSpec> getOutputSpecList(int taskIndex) throws AMUserCodeException {
    /**
     * Ref: TEZ-3297
     * Locking entire method could introduce a nested lock and
     * could lead to deadlock in corner cases. Example of deadlock with nested lock here:
     * 1. In thread#1, Downstream vertex is in the middle of processing setParallelism and gets
     * writeLock.
     * 2. In thread#2, currentVertex acquires read lock
     * 3. In thread#3, central dispatcher tries to process an event for current vertex,
     * so tries to acquire write lock.
     *
     * In further processing,
     * 4. In thread#1, it tries to acquire readLock on current vertex for setting edges. But
     * this would be blocked as #3 already requested for write lock
     * 5. In thread#2, getting readLock on downstream vertex would be blocked as writeLock
     * is held by thread#1.
     * 6. thread#3 is anyways blocked due to thread#2's read lock on current vertex.
     */

    List<OutputSpec> outputSpecList = null;
    readLock.lock();
    try {
      outputSpecList = new ArrayList<OutputSpec>(this.getOutputVerticesCount()
          + this.additionalOutputSpecs.size());
      outputSpecList.addAll(additionalOutputSpecs);
    } finally {
      readLock.unlock();
    }

    for(Vertex vertex : targetVertices.keySet()) {
      /**
       * It is possible that setParallelism (which could change numTasks) is in the middle of
       * processing in target vertex with its write lock. So we need to get outputspec by
       * acquiring read lock in target vertex to get consistent view.
       * Refer TEZ-2251
       */
      OutputSpec outputSpec = ((VertexImpl) vertex).getSourceSpecFor(this, taskIndex);
      outputSpecList.add(outputSpec);
    }
    return outputSpecList;
  }

  private OutputSpec getSourceSpecFor(VertexImpl vertex, int taskIndex) throws
      AMUserCodeException {
    readLock.lock();
    try {
      Edge edge = sourceVertices.get(vertex);
      Preconditions.checkState(edge != null, getLogIdentifier());
      return edge.getSourceSpec(taskIndex);
    } finally {
      readLock.unlock();
    }
  }

  private InputSpec getDestinationSpecFor(VertexImpl vertex, int taskIndex) throws
      AMUserCodeException {
    readLock.lock();
    try {
      Edge edge = targetVertices.get(vertex);
      Preconditions.checkState(edge != null, getLogIdentifier());
      return edge.getDestinationSpec(taskIndex);
    } finally {
      readLock.unlock();
    }
  }


  @Override
  public List<GroupInputSpec> getGroupInputSpecList() {
    readLock.lock();
    try {
      return groupInputSpecList;
    } finally {
      readLock.unlock();
    }
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
      if (taskLocationHint.getHosts() == null) {
        sb.append("No Hosts");
      } else {
        sb.append("Hosts: ");
        for (String host : taskLocationHint.getHosts()) {
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

  /**
   * This is for recovery when VertexReconfigureDoneEvent is seen. 
   */
  public static class NoOpVertexManager extends VertexManagerPlugin {

    private VertexConfigurationDoneEvent configurationDoneEvent;
    private boolean setParallelismInInitializing = false;

    public NoOpVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("initialize NoOpVertexManager");
      }
      configurationDoneEvent = new VertexConfigurationDoneEvent();
      configurationDoneEvent.fromProtoStream(new NonSyncByteArrayInputStream(getContext().getUserPayload().deepCopyAsArray()));
      String vertexName = getContext().getVertexName();
      if (getContext().getVertexNumTasks(vertexName) == -1) {
        Preconditions.checkArgument(configurationDoneEvent.isSetParallelismCalled(), "SetParallelism must be called "
            + "when numTasks is -1");
        setParallelismInInitializing = true;
        getContext().registerForVertexStateUpdates(vertexName,
            Sets.newHashSet(org.apache.tez.dag.api.event.VertexState.INITIALIZING));
      }
      getContext().vertexReconfigurationPlanned();
    }

    @Override
    public void onVertexStarted(List<TaskAttemptIdentifier> completions)
        throws Exception {
      // apply the ReconfigureDoneEvent and then schedule all the tasks.
      if (LOG.isDebugEnabled()) {
        LOG.debug("onVertexStarted is invoked in NoOpVertexManager, vertex=" + getContext().getVertexName());
      }
      if (!setParallelismInInitializing && configurationDoneEvent.isSetParallelismCalled()) {
        reconfigureVertex();
      }
      getContext().doneReconfiguringVertex();
      int numTasks = getContext().getVertexNumTasks(getContext().getVertexName());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Schedule all the tasks, numTask=" + numTasks);
      }
      List<ScheduleTaskRequest> tasks = new ArrayList<ScheduleTaskRequest>();
      for (int i=0;i<numTasks;++i) {
        tasks.add(ScheduleTaskRequest.create(i, null));
      }
      getContext().scheduleTasks(tasks);
    }

    @Override
    public void onSourceTaskCompleted(TaskAttemptIdentifier attempt)
        throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("onSourceTaskCompleted is invoked in NoOpVertexManager, vertex=" + getContext().getVertexName());
      }
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent)
        throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("onVertexManagerEventReceived is invoked in NoOpVertexManager, vertex=" + getContext().getVertexName());
      }
    }

    @Override
    public void onRootVertexInitialized(String inputName,
        InputDescriptor inputDescriptor, List<Event> events) throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("onRootVertexInitialized is invoked in NoOpVertexManager, vertex=" + getContext().getVertexName());
      }
    }

    @Override
    public void onVertexStateUpdated(VertexStateUpdate stateUpdate)
        throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("onVertexStateUpdated is invoked in NoOpVertexManager, vertex=" + getContext().getVertexName());
      }
      Preconditions.checkArgument(stateUpdate.getVertexState() ==
          org.apache.tez.dag.api.event.VertexState.INITIALIZING, "NoOpVertexManager get unexpected notification of "
          + " VertexStateUpdate:" + stateUpdate.getVertexState());
      reconfigureVertex();
    }

    private void reconfigureVertex() {
      getContext().reconfigureVertex(configurationDoneEvent.getNumTasks(),
          configurationDoneEvent.getVertexLocationHint(),
          configurationDoneEvent.getSourceEdgeProperties(),
          configurationDoneEvent.getRootInputSpecUpdates());
    }
  }

  @Private
  @VisibleForTesting
  void setCounters(TezCounters counters) {
    try {
      writeLock.lock();
      this.fullCounters = counters;
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  static class VertexConfigImpl implements VertexConfig {

    private final int maxFailedTaskAttempts;
    private final boolean taskRescheduleHigherPriority;
    private final boolean taskRescheduleRelaxedLocality;

    public VertexConfigImpl(Configuration conf) {
      this.maxFailedTaskAttempts = conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
          TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
      this.taskRescheduleHigherPriority =
          conf.getBoolean(TezConfiguration.TEZ_AM_TASK_RESCHEDULE_HIGHER_PRIORITY,
              TezConfiguration.TEZ_AM_TASK_RESCHEDULE_HIGHER_PRIORITY_DEFAULT);
      this.taskRescheduleRelaxedLocality =
          conf.getBoolean(TezConfiguration.TEZ_AM_TASK_RESCHEDULE_RELAXED_LOCALITY,
              TezConfiguration.TEZ_AM_TASK_RESCHEDULE_RELAXED_LOCALITY_DEFAULT);
    }

    @Override
    public int getMaxFailedTaskAttempts() {
      return maxFailedTaskAttempts;
    }

    @Override
    public boolean getTaskRescheduleHigherPriority() {
      return taskRescheduleHigherPriority;
    }

    @Override
    public boolean getTaskRescheduleRelaxedLocality() {
      return taskRescheduleRelaxedLocality;
    }
  }
}
