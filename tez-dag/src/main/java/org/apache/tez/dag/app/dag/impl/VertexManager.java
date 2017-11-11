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

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tez.dag.app.dag.event.DAGEventInternalError;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.events.CustomProcessorEvent;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.CallableEvent;
import org.apache.tez.dag.app.dag.event.VertexEventInputDataInformation;
import org.apache.tez.dag.app.dag.event.VertexEventManagerUserCodeError;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException.Source;
import org.apache.tez.dag.app.dag.VertexStateUpdateListener;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.VertexStatistics;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

@SuppressWarnings({"unchecked", "deprecation"})
public class VertexManager {
  final VertexManagerPluginDescriptor pluginDesc;
  final UserGroupInformation dagUgi;
  final VertexManagerPlugin plugin;
  final Vertex managedVertex;
  final VertexManagerPluginContextImpl pluginContext;
  final UserPayload payload;
  final AppContext appContext;
  final BlockingQueue<TezEvent> rootInputInitEventQueue;
  final StateChangeNotifier stateChangeNotifier;
  
  private final ListeningExecutorService execService;
  private final LinkedBlockingQueue<VertexManagerEvent> eventQueue;
  private final AtomicBoolean eventInFlight;
  private final AtomicBoolean pluginFailed;

  private static final Logger LOG = LoggerFactory.getLogger(VertexManager.class);
  private final VertexManagerCallback VM_CALLBACK = new VertexManagerCallback();

  class VertexManagerPluginContextImpl implements VertexManagerPluginContext, VertexStateUpdateListener {

    private EventMetaData rootEventSourceMetadata = new EventMetaData(EventProducerConsumerType.INPUT,
        managedVertex.getName(), "NULL_VERTEX", null);
    private Map<String, EventMetaData> destinationEventMetadataMap = Maps.newHashMap();
    private final List<String> notificationRegisteredVertices = Lists.newArrayList();
    AtomicBoolean isComplete = new AtomicBoolean(false);

    private void checkAndThrowIfDone() {
      if (isComplete()) {
        throw new TezUncheckedException("Cannot invoke context methods after reporting done");
      }
      if (pluginFailed.get()) {
        throw new TezUncheckedException("Cannot invoke context methods after throwing an exception");
      }
    }
    
    @Override
    public synchronized Map<String, EdgeProperty> getInputVertexEdgeProperties() {
      checkAndThrowIfDone();
      Map<Vertex, Edge> inputs = managedVertex.getInputVertices();
      Map<String, EdgeProperty> vertexEdgeMap =
                          Maps.newHashMapWithExpectedSize(inputs.size());
      for (Map.Entry<Vertex, Edge> entry : inputs.entrySet()) {
        vertexEdgeMap.put(entry.getKey().getName(), entry.getValue().getEdgeProperty());
      }
      return vertexEdgeMap;
    }

    @Override
    public synchronized Map<String, EdgeProperty> getOutputVertexEdgeProperties() {
      checkAndThrowIfDone();
      Map<Vertex, Edge> outputs = managedVertex.getOutputVertices();
      Map<String, EdgeProperty> vertexEdgeMap =
                          Maps.newHashMapWithExpectedSize(outputs.size());
      for (Map.Entry<Vertex, Edge> entry : outputs.entrySet()) {
        vertexEdgeMap.put(entry.getKey().getName(), entry.getValue().getEdgeProperty());
      }
      return vertexEdgeMap;
    }
    
    @Override
    public synchronized VertexStatistics getVertexStatistics(String vertexName) {
      checkAndThrowIfDone();
      return appContext.getCurrentDAG().getVertex(vertexName).getStatistics();      
    }

    @Override
    public synchronized String getVertexName() {
      checkAndThrowIfDone();
      return managedVertex.getName();
    }

    @Override
    public synchronized int getVertexNumTasks(String vertexName) {
      checkAndThrowIfDone();
      return appContext.getCurrentDAG().getVertex(vertexName).getTotalTasks();
    }

    @Override
    public synchronized void setVertexParallelism(int parallelism, VertexLocationHint vertexLocationHint,
        Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
        Map<String, InputSpecUpdate> rootInputSpecUpdate) {
      checkAndThrowIfDone();
      try {
        managedVertex.setParallelism(parallelism, vertexLocationHint, sourceEdgeManagers,
            rootInputSpecUpdate, true);
      } catch (AMUserCodeException e) {
        throw new TezUncheckedException(e);
      }
    }
    
    @Override
    public synchronized void reconfigureVertex(int parallelism, VertexLocationHint vertexLocationHint,
        Map<String, EdgeProperty> sourceEdgeProperties,
        Map<String, InputSpecUpdate> rootInputSpecUpdate) {
      checkAndThrowIfDone();
      try {
        managedVertex.reconfigureVertex(parallelism, vertexLocationHint, sourceEdgeProperties,
            rootInputSpecUpdate);
      } catch (AMUserCodeException e) {
        throw new TezUncheckedException(e);
      }
    }
    
    @Override
    public synchronized void reconfigureVertex(int parallelism,
        @Nullable VertexLocationHint locationHint,
        @Nullable Map<String, EdgeProperty> sourceEdgeProperties) {
      checkAndThrowIfDone();
      try {
        managedVertex.reconfigureVertex(parallelism, locationHint, sourceEdgeProperties);
      } catch (AMUserCodeException e) {
        throw new TezUncheckedException(e);
      }
    }
    
    @Override
    public void reconfigureVertex(@Nullable Map<String, InputSpecUpdate> rootInputSpecUpdate,
        @Nullable VertexLocationHint locationHint,
        int parallelism) {
      checkAndThrowIfDone();
      try {
        managedVertex.reconfigureVertex(rootInputSpecUpdate, parallelism, locationHint);
      } catch (AMUserCodeException e) {
        throw new TezUncheckedException(e);
      }
    }

    @Override
    public synchronized void scheduleTasks(List<ScheduleTaskRequest> tasks) {
      checkAndThrowIfDone();
      managedVertex.scheduleTasks(tasks);
    }
    
    @Override
    public synchronized void scheduleVertexTasks(List<TaskWithLocationHint> tasks) {
      checkAndThrowIfDone();
      List<ScheduleTaskRequest> schedTasks = new ArrayList<ScheduleTaskRequest>(tasks.size());
      for (TaskWithLocationHint task : tasks) {
        schedTasks.add(ScheduleTaskRequest.create(
            task.getTaskIndex(), task.getTaskLocationHint()));
      }
      scheduleTasks(schedTasks);
    }

    @Nullable
    @Override
    public synchronized Set<String> getVertexInputNames() {
      checkAndThrowIfDone();
      Set<String> inputNames = null;
      Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>>
          inputs = managedVertex.getAdditionalInputs();
      if (inputs != null) {
        inputNames = inputs.keySet();
      }
      return inputNames;
    }

    @Override
    public synchronized UserPayload getUserPayload() {
      checkAndThrowIfDone();
      return payload;
    }

    @Override
    public synchronized void addRootInputEvents(final String inputName,
        Collection<InputDataInformationEvent> events) {
      checkAndThrowIfDone();
      verifyIsRootInput(inputName);
      final long currTime = appContext.getClock().getTime();
      Collection<TezEvent> tezEvents = Collections2.transform(events,
          new Function<InputDataInformationEvent, TezEvent>() {
            @Override
            public TezEvent apply(InputDataInformationEvent riEvent) {
              TezEvent tezEvent = new TezEvent(riEvent, rootEventSourceMetadata, currTime);
              tezEvent.setDestinationInfo(getDestinationMetaData(inputName));
              return tezEvent;
            }
          });

      if (LOG.isDebugEnabled()) {
        LOG.debug("vertex:" + managedVertex.getLogIdentifier() + "; Added " + events.size() + " for input " +
                "name " + inputName);
      }
      rootInputInitEventQueue.addAll(tezEvents);
      // Recovery handling is taken care of by the Vertex.
    }

    @Override
    public void sendEventToProcessor(Collection<CustomProcessorEvent> events, int taskId) {
      checkAndThrowIfDone();
      Preconditions.checkArgument(taskId >= 0 && taskId < managedVertex.getTotalTasks(),
        "Invalid taskId " + taskId + "; " + "There are " + managedVertex.getTotalTasks()
          + " tasks in total.");

      if (events != null && events.size() > 0) {
        List<TezEvent> tezEvents = new ArrayList<>();
        for (CustomProcessorEvent event : events) {
          TezEvent tezEvent = new TezEvent(event, null);
          // use dummy task attempt id since this is not an task attempt specific event and task
          // attempt id won't be used anyway
          EventMetaData destinationMeta = new EventMetaData(EventProducerConsumerType.PROCESSOR,
            managedVertex.getName(), managedVertex.getName(),
            TezTaskAttemptID.getInstance(managedVertex.getTask(taskId).getTaskId(), -1));
          tezEvent.setDestinationInfo(destinationMeta);
          tezEvents.add(tezEvent);
        }
        appContext.getEventHandler().handle(
          new VertexEventRouteEvent(managedVertex.getVertexId(), tezEvents));
      }
    }

    @Override
    public synchronized void setVertexLocationHint(VertexLocationHint locationHint) {
      checkAndThrowIfDone();
      Preconditions.checkNotNull(locationHint, "locationHint is null");
      managedVertex.setVertexLocationHint(locationHint);
    }

    @Override
    public synchronized int getDAGAttemptNumber() {
      checkAndThrowIfDone();
      return appContext.getApplicationAttemptId().getAttemptId();
    }

    private void verifyIsRootInput(String inputName) {
      Preconditions.checkState(managedVertex.getAdditionalInputs().get(inputName) != null,
          "Cannot add events for non-root inputs");
    }

    private EventMetaData getDestinationMetaData(String inputName) {
      EventMetaData destMeta = destinationEventMetadataMap.get(inputName);
      if (destMeta == null) {
        destMeta = new EventMetaData(EventProducerConsumerType.INPUT, managedVertex.getName(),
            inputName, null);
        destinationEventMetadataMap.put(inputName, destMeta);
      }
      return destMeta;
    }

    @Override
    public synchronized Resource getVertexTaskResource() {
      checkAndThrowIfDone();
      return managedVertex.getTaskResource();
    }

    @Override
    public synchronized Resource getTotalAvailableResource() {
      checkAndThrowIfDone();
      return appContext.getTaskScheduler().getTotalResources(managedVertex.getTaskSchedulerIdentifier());
    }

    @Override
    public synchronized int getNumClusterNodes() {
      checkAndThrowIfDone();
      return appContext.getTaskScheduler().getNumClusterNodes();
    }

    @Override
    public synchronized void registerForVertexStateUpdates(String vertexName, Set<VertexState> stateSet) {
      checkAndThrowIfDone();
      synchronized(notificationRegisteredVertices) {
        notificationRegisteredVertices.add(vertexName);
      }
      stateChangeNotifier.registerForVertexUpdates(vertexName, stateSet, this);
    }

    private void unregisterForVertexStateUpdates() {
      synchronized (notificationRegisteredVertices) {
        for (String vertexName : notificationRegisteredVertices) {
          stateChangeNotifier.unregisterForVertexUpdates(vertexName, this);
        }

      }
    }
    
    boolean isComplete() {
      return (isComplete.get() == true);
    }

    // TODO add later after TEZ-1714 @Override
    public synchronized void vertexManagerDone() {
      checkAndThrowIfDone();
      LOG.info("Vertex Manager reported done for : " + managedVertex.getLogIdentifier());
      this.isComplete.set(true);
      unregisterForVertexStateUpdates();
    }

    @Override
    public synchronized void vertexReconfigurationPlanned() {
      checkAndThrowIfDone();
      managedVertex.vertexReconfigurationPlanned();
    }

    @Override
    public synchronized void doneReconfiguringVertex() {
      checkAndThrowIfDone();
      managedVertex.doneReconfiguringVertex();
    }

    @Override
    public Map<String, List<String>> getInputVertexGroups() {
      checkAndThrowIfDone();
      Map<String, List<String>> inputGroups = Maps.newHashMap();
      if (managedVertex.getGroupInputSpecList() != null) {
        for (GroupInputSpec group : managedVertex.getGroupInputSpecList()) {
          inputGroups.put(group.getGroupName(), Collections.unmodifiableList(group.getGroupVertices()));
        }
      }
      return inputGroups;
    }

    @Override
    public void onStateUpdated(VertexStateUpdate event) {
      // this is not called by the vertex manager plugin. 
      // no need to synchronize this. similar to other external notification methods
      enqueueAndScheduleNextEvent(new VertexManagerEventOnVertexStateUpdate(event));
    }

  }

  public VertexManager(VertexManagerPluginDescriptor pluginDesc, UserGroupInformation dagUgi,
      Vertex managedVertex, AppContext appContext, StateChangeNotifier stateChangeNotifier) throws TezException {
    checkNotNull(pluginDesc, "pluginDesc is null");
    checkNotNull(managedVertex, "managedVertex is null");
    checkNotNull(appContext, "appContext is null");
    checkNotNull(stateChangeNotifier, "notifier is null");
    this.pluginDesc = pluginDesc;
    this.dagUgi = dagUgi;
    this.managedVertex = managedVertex;
    this.appContext = appContext;
    this.stateChangeNotifier = stateChangeNotifier;
    // don't specify the size of rootInputInitEventQueue, otherwise it will fail when addAll
    this.rootInputInitEventQueue = new LinkedBlockingQueue<TezEvent>();
    
    pluginContext = new VertexManagerPluginContextImpl();
    Preconditions.checkArgument(pluginDesc != null);
    payload = pluginDesc.getUserPayload();
    pluginFailed = new AtomicBoolean(false);
    plugin = ReflectionUtils.createClazzInstance(pluginDesc.getClassName(),
        new Class[] { VertexManagerPluginContext.class }, new Object[] { pluginContext });
    execService = appContext.getExecService();
    eventQueue = new LinkedBlockingQueue<VertexManagerEvent>();
    eventInFlight = new AtomicBoolean(false);
  }

  public VertexManagerPlugin getPlugin() {
    return plugin;
  }

  public void initialize() throws AMUserCodeException {
    try {
      if (!pluginContext.isComplete()) {
        // TODO TEZ-2066 tracks moving this async.
        synchronized (VertexManager.this) {
          plugin.initialize();
        }
      }
    } catch (Exception e) {
      throw new AMUserCodeException(Source.VertexManager, e);
    }
  }
  
  private boolean pluginInvocationAllowed(String msg) {
    if (pluginFailed.get()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(msg + " . Manager failed. Vertex=" + managedVertex.getLogIdentifier());
      }
      return false;
    }
    if (pluginContext.isComplete()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(msg+ " . Manager complete. Not scheduling event. Vertex=" + managedVertex.getLogIdentifier());
      }
      return false;
    }
    return true;
  }
  
  private void enqueueAndScheduleNextEvent(VertexManagerEvent e) {
    if (!pluginInvocationAllowed("Dropping event")) {
      return;
    }
    eventQueue.add(e);
    tryScheduleNextEvent();
  }
  
  private void tryScheduleNextEvent() {
    if (!pluginInvocationAllowed("Not scheduling")) {
      return;
    }
    if (eventQueue.isEmpty()) {
      return;
    }
    if (eventInFlight.compareAndSet(false, true)) {
      // no event was in flight
      // ensures only 1 event is in flight
      VertexManagerEvent e = eventQueue.poll();
      if (e != null) {
        ListenableFuture<Void> future = execService.submit(e);
        Futures.addCallback(future, e.getCallback());
      } else {
        // This may happen. Lets say Callback succeeded on threadA. It set eventInFlight to false 
        // and called tryScheduleNextEvent() and found queue not empty but got paused before it 
        // could check eventInFlight.compareAndSet(). Another thread managed to dequeue the event 
        // and schedule a callback. That callback succeeded and set eventInFlight to false, found 
        // the queue empty and completed. Now threadA woke up and successfully did compareAndSet()
        // tried to dequeue an event and got null.
        // This could also happen if there is a bug and we manage to schedule for than 1 callback
        // verify that is not the case
        Preconditions.checkState(eventInFlight.compareAndSet(true, false));
      }
    }
  }

  public void onVertexStarted(List<TaskAttemptIdentifier> completions) throws AMUserCodeException {
    enqueueAndScheduleNextEvent(new VertexManagerEventOnVertexStarted(completions));
  }

  public void onSourceTaskCompleted(TaskAttemptIdentifier attempt) throws AMUserCodeException {
    enqueueAndScheduleNextEvent(new VertexManagerEventSourceTaskCompleted(attempt));
  }

  public void onVertexManagerEventReceived(
      org.apache.tez.runtime.api.events.VertexManagerEvent vmEvent) throws AMUserCodeException {
    enqueueAndScheduleNextEvent(new VertexManagerEventReceived(vmEvent));
  }

  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) throws AMUserCodeException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("vertex:" + managedVertex.getLogIdentifier() + "; enqueueing onRootVertexInitialized"
          + " on input:" + inputName + ", current task events size is " + rootInputInitEventQueue.size());
    }
    enqueueAndScheduleNextEvent(new VertexManagerEventRootInputInitialized(inputName,
        inputDescriptor, events));
  }

  private class VertexManagerCallback implements FutureCallback<Void> {

    @Override
    public void onFailure(Throwable t) {
      try {
        Preconditions.checkState(eventInFlight.get());
        // stop further event processing
        pluginFailed.set(true);
        eventQueue.clear();
        // catch real root cause of failure, it would throw UndeclaredThrowableException
        // if using UGI.doAs
        if (t instanceof UndeclaredThrowableException) {
          t = t.getCause();
        }
        Preconditions.checkState(appContext != null);
        Preconditions.checkState(managedVertex != null);
        // state change must be triggered via an event transition
        appContext.getEventHandler().handle(
            new VertexEventManagerUserCodeError(managedVertex.getVertexId(),
                new AMUserCodeException(Source.VertexManager, t)));
        // enqueue no further events due to user code error
      } catch (Exception e) {
        sendInternalError(e);
      }
    }
    
    @Override
    public void onSuccess(Void result) {
      try {
        onSuccessDerived(result);
        Preconditions.checkState(eventInFlight.compareAndSet(true, false));
        tryScheduleNextEvent();
      } catch (Exception e) {
        sendInternalError(e);
      }
    }
    
    protected void onSuccessDerived(Void result) {
    }
    
    private void sendInternalError(Exception e) {
      // fail the DAG so that we dont hang
      // state change must be triggered via an event transition
      LOG.error("Error after vertex manager callback " + managedVertex.getLogIdentifier(), e);
      appContext.getEventHandler().handle(
          (new DAGEventInternalError(managedVertex.getVertexId().getDAGId(),
              "Error in VertexManager for vertex: " + managedVertex.getLogIdentifier()
              + ", error=" + ExceptionUtils.getStackTrace(e))));
    }
  }
  
  private class VertexManagerRootInputInitializedCallback extends VertexManagerCallback {

    @Override
    protected void onSuccessDerived(Void result) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("vertex:" + managedVertex.getLogIdentifier()
            + "; after call of VertexManagerPlugin.onRootVertexInitialized" + " on input:"
            + ", current task events size is " + rootInputInitEventQueue.size());
      }
      List<TezEvent> resultEvents = new ArrayList<TezEvent>();
      rootInputInitEventQueue.drainTo(resultEvents);
      appContext.getEventHandler().handle(
          new VertexEventInputDataInformation(managedVertex.getVertexId(), resultEvents));
    }
  }
  
  class VertexManagerEventOnVertexStateUpdate extends VertexManagerEvent {
    private final VertexStateUpdate event;
    
    public VertexManagerEventOnVertexStateUpdate(VertexStateUpdate event) {
      this.event = event;
    }

    @Override
    public void invoke() throws Exception {
      plugin.onVertexStateUpdated(event);
    }
    
  }
  
  class VertexManagerEventOnVertexStarted extends VertexManagerEvent {
    private final List<TaskAttemptIdentifier> pluginCompletions;

    public VertexManagerEventOnVertexStarted(List<TaskAttemptIdentifier> pluginCompletions) {
      this.pluginCompletions = pluginCompletions;
    }
    
    @Override
    public void invoke() throws Exception {
      plugin.onVertexStarted(pluginCompletions);
    }
    
  }
  
  class VertexManagerEventSourceTaskCompleted extends VertexManagerEvent {
    private final TaskAttemptIdentifier attempt;
    
    public VertexManagerEventSourceTaskCompleted(TaskAttemptIdentifier attempt) {
      this.attempt = attempt;
    }
    
    @Override
    public void invoke() throws Exception {
      plugin.onSourceTaskCompleted(attempt);      
    }
    
  }
  
  class VertexManagerEventReceived extends VertexManagerEvent {
    private final org.apache.tez.runtime.api.events.VertexManagerEvent vmEvent;
    
    public VertexManagerEventReceived(org.apache.tez.runtime.api.events.VertexManagerEvent vmEvent) {
      this.vmEvent = vmEvent;
    }
    
    @Override
    public void invoke() throws Exception {
      plugin.onVertexManagerEventReceived(vmEvent);
    }
    
  }
  
  class VertexManagerEventRootInputInitialized extends VertexManagerEvent {
    private final String inputName;
    private final InputDescriptor inputDescriptor;
    private final List<Event> events;
    
    public VertexManagerEventRootInputInitialized(String inputName,
        InputDescriptor inputDescriptor, List<Event> events) {
      super(new VertexManagerRootInputInitializedCallback());
      this.inputName = inputName;
      this.inputDescriptor = inputDescriptor;
      this.events = events;
    }

    @Override
    public void invoke() throws Exception {
      plugin.onRootVertexInitialized(inputName, inputDescriptor, events);
    }

  }
  
  abstract class VertexManagerEvent extends CallableEvent {
    public VertexManagerEvent() {
      this(VM_CALLBACK);
    }
    public VertexManagerEvent(VertexManagerCallback callback) {
      super(callback);
    }

    @Override
    public Void call() throws Exception {
      final VertexManager manager = VertexManager.this;
      manager.dagUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          synchronized (manager) {
            if (manager.pluginInvocationAllowed("Not invoking")) {
              invoke();
            }
          }
          return null;
        }
      });
      return null;
    }

    public abstract void invoke() throws Exception;
  }
}
