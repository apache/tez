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

package org.apache.tez.dag.app.dag;

import javax.annotation.Nullable;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.*;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputInitialized;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException;
import org.apache.tez.dag.app.dag.impl.TezRootInputInitializerContextImpl;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException.Source;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.impl.TezEvent;

public class RootInputInitializerManager {

  private static final Log LOG = LogFactory.getLog(RootInputInitializerManager.class);

  private final ExecutorService rawExecutor;
  private final ListeningExecutorService executor;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private volatile boolean isStopped = false;
  private final UserGroupInformation dagUgi;
  private final StateChangeNotifier entityStateTracker;

  private final Vertex vertex;
  private final AppContext appContext;

  @VisibleForTesting
  final Map<String, InitializerWrapper> initializerMap = new HashMap<String, InitializerWrapper>();

  public RootInputInitializerManager(Vertex vertex, AppContext appContext,
                                     UserGroupInformation dagUgi, StateChangeNotifier stateTracker) {
    this.appContext = appContext;
    this.vertex = vertex;
    this.eventHandler = appContext.getEventHandler();
    this.rawExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("InputInitializer [" + this.vertex.getName() + "] #%d").build());
    this.executor = MoreExecutors.listeningDecorator(rawExecutor);
    this.dagUgi = dagUgi;
    this.entityStateTracker = stateTracker;
  }
  
  public void runInputInitializers(List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
      inputs) {
    for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input : inputs) {

      InputInitializerContext context =
          new TezRootInputInitializerContextImpl(input, vertex, appContext, this);
      InputInitializer initializer = createInitializer(input, context);

      InitializerWrapper initializerWrapper =
          new InitializerWrapper(input, initializer, context, vertex, entityStateTracker, appContext);

      // Register pending vertex update registrations
      List<VertexUpdateRegistrationHolder> vertexUpdateRegistrations = pendingVertexRegistrations.removeAll(input.getName());
      if (vertexUpdateRegistrations != null) {
        for (VertexUpdateRegistrationHolder h : vertexUpdateRegistrations) {
          initializerWrapper.registerForVertexStateUpdates(h.vertexName, h.stateSet);
        }
      }

      initializerMap.put(input.getName(), initializerWrapper);
      ListenableFuture<List<Event>> future = executor
          .submit(new InputInitializerCallable(initializerWrapper, dagUgi));
      Futures.addCallback(future, createInputInitializerCallback(initializerWrapper));
    }
  }

  @VisibleForTesting
  protected InputInitializer createInitializer(RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>
      input, InputInitializerContext context) {
    InputInitializer initializer = ReflectionUtils
        .createClazzInstance(input.getControllerDescriptor().getClassName(),
            new Class[]{InputInitializerContext.class}, new Object[]{context});
    return initializer;
  }

  public void handleInitializerEvents(List<TezEvent> events) {
    ListMultimap<InitializerWrapper, TezEvent> eventMap = LinkedListMultimap.create();

    for (TezEvent tezEvent : events) {
      Preconditions.checkState(tezEvent.getEvent() instanceof InputInitializerEvent);
      InputInitializerEvent event = (InputInitializerEvent)tezEvent.getEvent();
      Preconditions.checkState(vertex.getName().equals(event.getTargetVertexName()),
          "Received event for incorrect vertex");
      Preconditions.checkNotNull(event.getTargetInputName(), "target input name must be set");
      InitializerWrapper initializer = initializerMap.get(event.getTargetInputName());
      Preconditions.checkState(initializer != null,
          "Received event for unknown input : " + event.getTargetInputName());
      eventMap.put(initializer, tezEvent);
    }

    // This is a restriction based on current flow - i.e. events generated only by initialize().
    // TODO Rework the flow as per the first comment on TEZ-1076
    if (isStopped) {
      LOG.warn("InitializerManager already stopped for " + vertex.getLogIdentifier() +
          " Dropping " + events.size() + " events");
    }

    for (Map.Entry<InitializerWrapper, Collection<TezEvent>> entry : eventMap.asMap().entrySet()) {
      InitializerWrapper initializerWrapper = entry.getKey();
      if (initializerWrapper.isComplete()) {
        LOG.warn(entry.getValue().size() +
            " events targeted at vertex " + vertex.getLogIdentifier() +
            ", initializerWrapper for Input: " +
            initializerWrapper.getInput().getName() +
            " will be dropped, since Input has already been initialized.");
      } else {
        initializerWrapper.handleInputInitializerEvents(entry.getValue());
      }

    }
  }

  private static class VertexUpdateRegistrationHolder {
    private VertexUpdateRegistrationHolder(String vertexName, Set<org.apache.tez.dag.api.event.VertexState> stateSet) {
      this.vertexName = vertexName;
      this.stateSet = stateSet;
    }
    private final String vertexName;
    private final Set<org.apache.tez.dag.api.event.VertexState> stateSet;
  }

  // This doesn't need to be thread safe, since initializers are not created in separate threads,
  // they're only executed in separate threads.
  private final ListMultimap<String, VertexUpdateRegistrationHolder> pendingVertexRegistrations =
      LinkedListMultimap.create();

  public void registerForVertexUpdates(String vertexName, String inputName,
                                       @Nullable Set<org.apache.tez.dag.api.event.VertexState> stateSet) {
    Preconditions.checkNotNull(vertexName, "VertexName cannot be null: " + vertexName);
    Preconditions.checkNotNull(inputName, "InputName cannot be null");
    InitializerWrapper initializer = initializerMap.get(inputName);
    if (initializer == null) {
      pendingVertexRegistrations.put(inputName, new VertexUpdateRegistrationHolder(vertexName, stateSet));
    } else {
      initializer.registerForVertexStateUpdates(vertexName, stateSet);
    }
  }

  @VisibleForTesting
  protected InputInitializerCallback createInputInitializerCallback(InitializerWrapper initializer) {
    return new InputInitializerCallback(initializer, eventHandler, vertex.getVertexId());
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public InitializerWrapper getInitializerWrapper(String inputName) {
    return initializerMap.get(inputName);
  }

  public void shutdown() {
    if (executor != null && !isStopped) {
      // Don't really care about what is running if an error occurs. If no error
      // occurs, all execution is complete.
      executor.shutdownNow();
      isStopped = true;
    }
  }

  private static class InputInitializerCallable implements
      Callable<List<Event>> {

    private final InitializerWrapper initializerWrapper;
    private final UserGroupInformation ugi;

    public InputInitializerCallable(InitializerWrapper initializer, UserGroupInformation ugi) {
      this.initializerWrapper = initializer;
      this.ugi = ugi;
    }

    @Override
    public List<Event> call() throws Exception {
      List<Event> events = ugi.doAs(new PrivilegedExceptionAction<List<Event>>() {
        @Override
        public List<Event> run() throws Exception {
          LOG.info(
              "Starting InputInitializer for Input: " + initializerWrapper.getInput().getName() +
                  " on vertex " + initializerWrapper.getVertexLogIdentifier());
          return initializerWrapper.getInitializer().initialize();
        }
      });
      return events;
    }
  }

  @SuppressWarnings("rawtypes")
  @VisibleForTesting
  private static class InputInitializerCallback implements
      FutureCallback<List<Event>> {

    private final InitializerWrapper initializer;
    private final EventHandler eventHandler;
    private final TezVertexID vertexID;

    public InputInitializerCallback(InitializerWrapper initializer,
        EventHandler eventHandler, TezVertexID vertexID) {
      this.initializer = initializer;
      this.eventHandler = eventHandler;
      this.vertexID = vertexID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onSuccess(List<Event> result) {
      initializer.setComplete();
      LOG.info(
          "Succeeded InputInitializer for Input: " + initializer.getInput().getName() +
              " on vertex " + initializer.getVertexLogIdentifier());
      eventHandler.handle(new VertexEventRootInputInitialized(vertexID,
          initializer.getInput().getName(), result));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onFailure(Throwable t) {
      // catch real root cause of failure, it would throw UndeclaredThrowableException
      // if using UGI.doAs
      if (t instanceof UndeclaredThrowableException) {
        t = t.getCause();
      }
      initializer.setComplete();
      LOG.info(
          "Failed InputInitializer for Input: " + initializer.getInput().getName() +
              " on vertex " + initializer.getVertexLogIdentifier());
      eventHandler
          .handle(new VertexEventRootInputFailed(vertexID, initializer.getInput().getName(),
              new AMUserCodeException(Source.InputInitializer,t)));
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public static class InitializerWrapper implements VertexStateUpdateListener, TaskStateUpdateListener {


    private final RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input;
    private final InputInitializer initializer;
    private final InputInitializerContext context;
    private final AtomicBoolean isComplete = new AtomicBoolean(false);
    private final String vertexLogIdentifier;
    private final TezVertexID vertexId;
    private final StateChangeNotifier stateChangeNotifier;
    private final List<String> notificationRegisteredVertices = Lists.newArrayList();
    private final AppContext appContext;

    InitializerWrapper(RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input,
                       InputInitializer initializer, InputInitializerContext context,
                       Vertex vertex, StateChangeNotifier stateChangeNotifier,
                       AppContext appContext) {
      this.input = input;
      this.initializer = initializer;
      this.context = context;
      this.vertexLogIdentifier = vertex.getLogIdentifier();
      this.vertexId = vertex.getVertexId();
      this.stateChangeNotifier = stateChangeNotifier;
      this.appContext = appContext;
    }

    public RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> getInput() {
      return input;
    }

    public InputInitializer getInitializer() {
      return initializer;
    }

    public String getVertexLogIdentifier() {
      return vertexLogIdentifier;
    }

    public boolean isComplete() {
      return isComplete.get();
    }

    public void setComplete() {
      this.isComplete.set(true);
      unregisterForVertexStatusUpdates();
      unregisterForTaskStatusUpdates();
    }

    public void registerForVertexStateUpdates(String vertexName, Set<VertexState> stateSet) {
      synchronized(notificationRegisteredVertices) {
        notificationRegisteredVertices.add(vertexName);
      }
      stateChangeNotifier.registerForVertexUpdates(vertexName, stateSet, this);
    }

    private void unregisterForVertexStatusUpdates() {
      synchronized (notificationRegisteredVertices) {
        for (String vertexName : notificationRegisteredVertices) {
          stateChangeNotifier.unregisterForVertexUpdates(vertexName, this);
        }

      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onStateUpdated(VertexStateUpdate event) {
      if (isComplete()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dropping state update for vertex=" + event.getVertexName() + ", state=" +
              event.getVertexState() +
              " since initializer " + input.getName() + " is already complete.");
        }
      } else {
        try {
          initializer.onVertexStateUpdated(event);
        } catch (Exception e) {
          appContext.getEventHandler().handle(
              new VertexEventRootInputFailed(vertexId, input.getName(),
                  new AMUserCodeException(Source.InputInitializer,e)));
        }
      }
    }

    private final Map<String, Map<Integer, Integer>> firstSuccessfulAttemptMap = new HashMap<String, Map<Integer, Integer>>();
    private final ListMultimap<String, TezEvent> pendingEvents = LinkedListMultimap.create();
    private final List<String> taskNotificationRegisteredVertices = Lists.newLinkedList();

    @InterfaceAudience.Private
    @VisibleForTesting
    public Map<String, Map<Integer, Integer>> getFirstSuccessfulAttemptMap() {
      return this.firstSuccessfulAttemptMap;
    }

    @InterfaceAudience.Private
    @VisibleForTesting
    public ListMultimap<String, TezEvent> getPendingEvents() {
      return this.pendingEvents;
    }

    @Override
    public void onTaskSucceeded(String vertexName, TezTaskID taskId, int attemptId) {
      // Notifications will only start coming in after an event is received, which is when we register for notifications.
      // TODO TEZ-1577. Get rid of this.
      if (attemptId == -1) {
        throw new TezUncheckedException(
            "AttemptId is -1. This is likely caused by TEZ-1577; recovery not supported when InputInitializerEvents are used");
      }
      Map<Integer, Integer> vertexSuccessfulAttemptMap = firstSuccessfulAttemptMap.get(vertexName);
      Integer successfulAttempt = vertexSuccessfulAttemptMap.get(taskId.getId());
      if (successfulAttempt == null) {
        successfulAttempt = attemptId;
        vertexSuccessfulAttemptMap.put(taskId.getId(), successfulAttempt);
      }

      // Run through all the pending events for this srcVertex to see if any of them need to be dispatched.
      List<TezEvent> events = pendingEvents.get(vertexName);
      if (events != null && !events.isEmpty()) {
        List<InputInitializerEvent> toForwardEvents = new LinkedList<InputInitializerEvent>();
        Iterator<TezEvent> eventIterator = events.iterator();
        while (eventIterator.hasNext()) {
          TezEvent tezEvent = eventIterator.next();
          int taskIndex = tezEvent.getSourceInfo().getTaskAttemptID().getTaskID().getId();
          int taskAttemptIndex = tezEvent.getSourceInfo().getTaskAttemptID().getId();
          if (taskIndex == taskId.getId()) {
            // Process only if there's a pending event for the specific succeeded task
            if (taskAttemptIndex == successfulAttempt) {
              toForwardEvents.add((InputInitializerEvent) tezEvent.getEvent());
            }
            eventIterator.remove();
          }
        }
        sendEvents(toForwardEvents);
      }
    }

    public void handleInputInitializerEvents(Collection<TezEvent> tezEvents) {
      List<InputInitializerEvent> toForwardEvents = new LinkedList<InputInitializerEvent>();
      for (TezEvent tezEvent : tezEvents) {
        String srcVertexName = tezEvent.getSourceInfo().getTaskVertexName();
        int taskIndex = tezEvent.getSourceInfo().getTaskAttemptID().getTaskID().getId();
        int taskAttemptIndex = tezEvent.getSourceInfo().getTaskAttemptID().getId();

        Map<Integer, Integer> vertexSuccessfulAttemptMap =
            firstSuccessfulAttemptMap.get(srcVertexName);
        if (vertexSuccessfulAttemptMap == null) {
          vertexSuccessfulAttemptMap = new HashMap<Integer, Integer>();
          firstSuccessfulAttemptMap.put(srcVertexName, vertexSuccessfulAttemptMap);
          // Seen first time. Register for task updates
          stateChangeNotifier.registerForTaskSuccessUpdates(srcVertexName, this);
          taskNotificationRegisteredVertices.add(srcVertexName);
        }

        // Determine the successful attempt for the task
        Integer successfulAttemptInteger = vertexSuccessfulAttemptMap.get(taskIndex);
        if (successfulAttemptInteger == null) {
          // Check immediately if this task has succeeded, in case the notification came in before the event
          Vertex srcVertex = appContext.getCurrentDAG().getVertex(srcVertexName);
          Task task = srcVertex.getTask(taskIndex);
          if (task.getState() == TaskState.SUCCEEDED) {
            successfulAttemptInteger = task.getSuccessfulAttempt().getID().getId();
            vertexSuccessfulAttemptMap.put(taskIndex, successfulAttemptInteger);
          }
        }

        if (successfulAttemptInteger == null) {
          // Queue events and await a notification
          pendingEvents.put(srcVertexName, tezEvent);
        } else {
          // Handle the event immediately.
          if (taskAttemptIndex == successfulAttemptInteger) {
            toForwardEvents.add((InputInitializerEvent) tezEvent.getEvent());
          } // Otherwise the event can be dropped
        }
      }
      sendEvents(toForwardEvents);
    }

    @SuppressWarnings("unchecked")
    private void sendEvents(List<InputInitializerEvent> events) {
      if (events != null && !events.isEmpty()) {
        try {
          initializer.handleInputInitializerEvent(events);
        } catch (Exception e) {
          appContext.getEventHandler().handle(
              new VertexEventRootInputFailed(vertexId, input.getName(),
                  new AMUserCodeException(Source.InputInitializer,e)));
        }
      }
    }

    private void unregisterForTaskStatusUpdates() {
      for (String vertexName : taskNotificationRegisteredVertices) {
        stateChangeNotifier.unregisterForTaskSuccessUpdates(vertexName, this);
      }
    }
  }

}
