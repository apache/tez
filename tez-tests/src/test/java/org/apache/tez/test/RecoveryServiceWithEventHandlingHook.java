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
package org.apache.tez.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.test.RecoveryServiceWithEventHandlingHook.SimpleShutdownCondition.TIMING;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Add hook before/after processing RecoveryEvent & SummaryEvent
 *
 */
public class RecoveryServiceWithEventHandlingHook extends RecoveryService {

  public static final String AM_RECOVERY_SERVICE_HOOK_CLASS = "tez.test.am.recovery_service.hook";
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryServiceWithEventHandlingHook.class);
  private RecoveryServiceHook hook;
  private boolean shutdownInvoked = false;
  public RecoveryServiceWithEventHandlingHook(AppContext appContext) {
    super(appContext);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    String clazz = conf.get(AM_RECOVERY_SERVICE_HOOK_CLASS);
    Preconditions.checkArgument(clazz != null, "RecoveryServiceHook class is not specified");
    this.hook = ReflectionUtils.createClazzInstance(clazz, 
        new Class[]{RecoveryServiceWithEventHandlingHook.class, AppContext.class},
        new Object[]{this, super.appContext});
  }

  @Override
  protected void handleRecoveryEvent(DAGHistoryEvent event) throws IOException {
    hook.preHandleRecoveryEvent(event);
    if (shutdownInvoked) {
      return;
    }
    super.handleRecoveryEvent(event);
    hook.postHandleRecoveryEvent(event);
  }

  @Override
  protected void handleSummaryEvent(TezDAGID dagID, HistoryEventType eventType,
      SummaryEvent summaryEvent) throws IOException {
    hook.preHandleSummaryEvent(eventType, summaryEvent);
    if (shutdownInvoked) {
      return;
    }
    super.handleSummaryEvent(dagID, eventType, summaryEvent);
    hook.postHandleSummaryEvent(eventType, summaryEvent);
  }

  private void shutdown() {
    // start a new thread to shutdown AM otherwise will cause dead lock
    // (JVM exit will DAGAppMasterShutdownHook called and RecoveryService's stop will be called
    // which will drain all the events)
    Thread shutdownThread = new Thread("AMShutdown Thread") {
      @Override
      public void run() {
        LOG.info("Try to kill AM");
        System.exit(1);
      }
    };
    // stop process recovery events
    super.setStopped(true);
    shutdownInvoked = true;
    shutdownThread.start();
  }

  /**
   * Abstract class to allow do something before/after processing recovery events
   *
   */
  public static abstract class RecoveryServiceHook {

    protected RecoveryServiceWithEventHandlingHook recoveryService;
    protected AppContext appContext;

    public RecoveryServiceHook(RecoveryServiceWithEventHandlingHook recoveryService, AppContext appContext) {
      this.recoveryService = recoveryService;
      this.appContext = appContext;
    }

    public abstract void preHandleRecoveryEvent(DAGHistoryEvent event) throws IOException;

    public abstract void postHandleRecoveryEvent(DAGHistoryEvent event) throws IOException;

    public abstract void preHandleSummaryEvent(HistoryEventType eventType,
        SummaryEvent summaryEvent) throws IOException;

    public abstract void postHandleSummaryEvent(HistoryEventType eventType,
        SummaryEvent summaryEvent) throws IOException;

  }

  /**
   * Shutdown AM before/after a specified recovery event is processed.
   * Only do it in the first AM attempt
   *
   */
  public static class SimpleRecoveryEventHook extends RecoveryServiceHook {

    public static final String SIMPLE_SHUTDOWN_CONDITION = "tez.test.recovery.simple_shutdown_condition";
    private SimpleShutdownCondition shutdownCondition;

    public SimpleRecoveryEventHook(
        RecoveryServiceWithEventHandlingHook recoveryService, AppContext appContext) {
      super(recoveryService, appContext);
      this.shutdownCondition = new SimpleShutdownCondition();
      try {
        Preconditions.checkArgument(recoveryService.getConfig().get(SIMPLE_SHUTDOWN_CONDITION) != null,
            SIMPLE_SHUTDOWN_CONDITION + " is not set in TezConfiguration");
        this.shutdownCondition.deserialize(recoveryService.getConfig().get(SIMPLE_SHUTDOWN_CONDITION));
      } catch (IOException e) {
        throw new TezUncheckedException("Can not initialize SimpleShutdownCondition", e);
      }
    }

    @Override
    public void preHandleRecoveryEvent(DAGHistoryEvent event)
        throws IOException {
      if (shutdownCondition.timing.equals(TIMING.PRE)
          && appContext.getApplicationAttemptId().getAttemptId() == 1
          && shutdownCondition.match(event.getHistoryEvent())) {
        recoveryService.shutdown();
      }
    }

    @Override
    public void postHandleRecoveryEvent(DAGHistoryEvent event)
        throws IOException {
      if (shutdownCondition.timing.equals(TIMING.POST)
         && appContext.getApplicationAttemptId().getAttemptId() == 1
         && shutdownCondition.match(event.getHistoryEvent())) {
        recoveryService.shutdown();
      }
    }
 
    @Override
    public void preHandleSummaryEvent(HistoryEventType eventType,
        SummaryEvent summaryEvent) throws IOException {
    }

    @Override
    public void postHandleSummaryEvent(HistoryEventType eventType,
        SummaryEvent summaryEvent) throws IOException {
    }

  }

  /**
   * 
   * Shutdown AM based on one recovery event if it is matched.
   * This would be serialized as property of TezConfiguration and deserialized at runtime.
   */
  public static class SimpleShutdownCondition {

    public static enum TIMING {
      PRE, // before the event
      POST, // after the event
    }

    private TIMING timing;
    private HistoryEvent event;

    public SimpleShutdownCondition(TIMING timing, HistoryEvent event) {
      this.timing = timing;
      this.event = event;
    }

    public SimpleShutdownCondition() {
    }

    public HistoryEvent getHistoryEvent() {
      return this.event;
    }

    private String encodeHistoryEvent(HistoryEvent event) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      event.toProtoStream(out);
      return event.getClass().getName() + ","
          + Base64.encodeBase64String(out.toByteArray());
    }

    private HistoryEvent decodeHistoryEvent(String eventClass, String base64)
        throws IOException {
      ByteArrayInputStream in = new ByteArrayInputStream(
          Base64.decodeBase64(base64));
      try {
        HistoryEvent event = ReflectionUtils.createClazzInstance(eventClass);
        event.fromProtoStream(in);
        return event;
      } catch (TezReflectionException e) {
        throw new IOException(e);
      }
    }

    public String serialize() throws IOException {
      StringBuilder builder = new StringBuilder();
      builder.append(timing.name() + ",");
      builder.append(encodeHistoryEvent(event));
      return builder.toString();
    }

    public SimpleShutdownCondition deserialize(String str) throws IOException {
      String[] tokens = str.split(",");
      timing = TIMING.valueOf(tokens[0]);
      this.event = decodeHistoryEvent(tokens[1], tokens[2]);
      return this;
    }

    public HistoryEvent getEvent() {
      return event;
    }

    public TIMING getTiming() {
      return timing;
    }

    public boolean match(HistoryEvent incomingEvent) {
      switch (event.getEventType()) {
      case DAG_SUBMITTED:
        if (incomingEvent.getEventType() == HistoryEventType.DAG_SUBMITTED) {
          // only compare eventType
          return true;
        }
        break;

      case DAG_INITIALIZED:
        if (incomingEvent.getEventType() == HistoryEventType.DAG_INITIALIZED) {
          // only compare eventType
          return true;
        }
        break;

      case DAG_STARTED:
        if (incomingEvent.getEventType() == HistoryEventType.DAG_STARTED) {
          // only compare eventType
          return true;
        }
        break;

      case DAG_FINISHED:
        if (incomingEvent.getEventType() == HistoryEventType.DAG_FINISHED) {
          // only compare eventType
          return true;
        }
        break;

      case VERTEX_INITIALIZED:
        if (incomingEvent.getEventType() == HistoryEventType.VERTEX_INITIALIZED) {
          VertexInitializedEvent otherEvent = (VertexInitializedEvent) incomingEvent;
          VertexInitializedEvent conditionEvent = (VertexInitializedEvent) event;
          // compare vertexId;
          return otherEvent.getVertexID().getId() == conditionEvent.getVertexID().getId();
        }
        break;

      case VERTEX_STARTED:
        if (incomingEvent.getEventType() == HistoryEventType.VERTEX_STARTED) {
          VertexStartedEvent otherEvent = (VertexStartedEvent) incomingEvent;
          VertexStartedEvent conditionEvent = (VertexStartedEvent) event;
          // compare vertexId
          return otherEvent.getVertexID().getId() == conditionEvent.getVertexID().getId();
        }
        break;

      case VERTEX_FINISHED:
        if (incomingEvent.getEventType() == HistoryEventType.VERTEX_FINISHED) {
          VertexFinishedEvent otherEvent = (VertexFinishedEvent) incomingEvent;
          VertexFinishedEvent conditionEvent = (VertexFinishedEvent) event;
          // compare vertexId
          return otherEvent.getVertexID().getId() == conditionEvent.getVertexID().getId();
        }
        break;
      case VERTEX_CONFIGURE_DONE:
        if (incomingEvent.getEventType() == HistoryEventType.VERTEX_CONFIGURE_DONE) {
          VertexConfigurationDoneEvent otherEvent = (VertexConfigurationDoneEvent) incomingEvent;
          VertexConfigurationDoneEvent conditionEvent = (VertexConfigurationDoneEvent) event;
          // compare vertexId
          return otherEvent.getVertexID().getId() == conditionEvent.getVertexID().getId();
        }
        break;
      case TASK_STARTED:
        if (incomingEvent.getEventType() == HistoryEventType.TASK_STARTED) {
          TaskStartedEvent otherEvent = (TaskStartedEvent) incomingEvent;
          TaskStartedEvent conditionEvent = (TaskStartedEvent) event;
          // compare vertexId and taskId
          return otherEvent.getTaskID().getVertexID().getId() == conditionEvent.getTaskID().getVertexID().getId()
              && otherEvent.getTaskID().getId() == conditionEvent.getTaskID().getId();
        }
        break;

      case TASK_FINISHED:
        if (incomingEvent.getEventType() == HistoryEventType.TASK_FINISHED) {
          TaskFinishedEvent otherEvent = (TaskFinishedEvent) incomingEvent;
          TaskFinishedEvent conditionEvent = (TaskFinishedEvent) event;
          // compare vertexId and taskId
          return otherEvent.getTaskID().getVertexID().getId() == conditionEvent.getTaskID().getVertexID().getId()
              && otherEvent.getTaskID().getId() == conditionEvent.getTaskID().getId();
        }
        break;

      case TASK_ATTEMPT_STARTED:
        if (incomingEvent.getEventType() == HistoryEventType.TASK_ATTEMPT_STARTED) {
          TaskAttemptStartedEvent otherEvent = (TaskAttemptStartedEvent) incomingEvent;
          TaskAttemptStartedEvent conditionEvent = (TaskAttemptStartedEvent) event;
          // compare vertexId, taskId & taskAttemptId
          return otherEvent.getTaskAttemptID().getTaskID().getVertexID().getId() 
              == conditionEvent.getTaskAttemptID().getTaskID().getVertexID().getId()
              && otherEvent.getTaskAttemptID().getTaskID().getId() == conditionEvent.getTaskAttemptID().getTaskID().getId()
              && otherEvent.getTaskAttemptID().getId() == conditionEvent.getTaskAttemptID().getId();
        }
        break;

      case TASK_ATTEMPT_FINISHED:
        if (incomingEvent.getEventType() == HistoryEventType.TASK_ATTEMPT_FINISHED) {
          TaskAttemptFinishedEvent otherEvent = (TaskAttemptFinishedEvent) incomingEvent;
          TaskAttemptFinishedEvent conditionEvent = (TaskAttemptFinishedEvent) event;
          // compare vertexId, taskId & taskAttemptId
          return otherEvent.getTaskAttemptID().getTaskID().getVertexID().getId() 
              == conditionEvent.getTaskAttemptID().getTaskID().getVertexID().getId()
              && otherEvent.getTaskAttemptID().getTaskID().getId() == conditionEvent.getTaskAttemptID().getTaskID().getId()
              && otherEvent.getTaskAttemptID().getId() == conditionEvent.getTaskAttemptID().getId();
        }
        break;
      default:
        LOG.info("do nothing with event:"
            + event.getEventType());
      }

      return false;
    }
    
    public HistoryEventType getEventType() {
      return event.getEventType();
    }
  }

  public static class MultipleRoundRecoveryEventHook extends RecoveryServiceHook {

    public static final String MULTIPLE_ROUND_SHUTDOWN_CONDITION = "tez.test.recovery.multiple_round_shutdown_condition";
    private MultipleRoundShutdownCondition shutdownCondition;
    private int attemptId;

    public MultipleRoundRecoveryEventHook(RecoveryServiceWithEventHandlingHook recoveryService, AppContext appContext) {
      super(recoveryService, appContext);
      this.shutdownCondition = new MultipleRoundShutdownCondition();
      try {
        Preconditions.checkArgument(recoveryService.getConfig().get(MULTIPLE_ROUND_SHUTDOWN_CONDITION) != null,
                MULTIPLE_ROUND_SHUTDOWN_CONDITION + " is not set in TezConfiguration");
        this.shutdownCondition.deserialize(recoveryService.getConfig().get(MULTIPLE_ROUND_SHUTDOWN_CONDITION));
      } catch (IOException e) {
        throw new TezUncheckedException("Can not initialize MultipleRoundShutdownCondition", e);
      }
      this.attemptId = appContext.getApplicationAttemptId().getAttemptId();
    }

    @Override
    public void preHandleRecoveryEvent(DAGHistoryEvent event) throws IOException {
      if (attemptId <= shutdownCondition.size()) {
        SimpleShutdownCondition condition = shutdownCondition.getSimpleShutdownCondition(attemptId - 1);
        if (condition.timing.equals(TIMING.PRE)
                && condition.match(event.getHistoryEvent())) {
          recoveryService.shutdown();
        }
      }
    }

    @Override
    public void postHandleRecoveryEvent(DAGHistoryEvent event) throws IOException {
      for (int i=0;i<shutdownCondition.size();++i) {
        SimpleShutdownCondition condition = shutdownCondition.getSimpleShutdownCondition(i);
        LOG.info("condition:" + condition.getEvent().getEventType() + ":" + condition.getHistoryEvent());
      }
      if (attemptId <= shutdownCondition.size()) {
        SimpleShutdownCondition condition = shutdownCondition.getSimpleShutdownCondition(attemptId - 1);

        LOG.info("event:" + event.getHistoryEvent().getEventType());
        if (condition.timing.equals(TIMING.POST)
                && condition.match(event.getHistoryEvent())) {
          recoveryService.shutdown();
        }
      }
    }

    @Override
    public void preHandleSummaryEvent(HistoryEventType eventType, SummaryEvent summaryEvent) throws IOException {

    }

    @Override
    public void postHandleSummaryEvent(HistoryEventType eventType, SummaryEvent summaryEvent) throws IOException {

    }
  }

  public static class MultipleRoundShutdownCondition {

    private List<SimpleShutdownCondition> shutdownConditionList;

    public MultipleRoundShutdownCondition() {

    }

    public MultipleRoundShutdownCondition(List<SimpleShutdownCondition> shutdownConditionList) {
      this.shutdownConditionList = shutdownConditionList;
    }

    public String serialize() throws IOException {
      StringBuilder builder = new StringBuilder();
      for (int i=0; i< shutdownConditionList.size(); ++i) {
        builder.append(shutdownConditionList.get(i).serialize());
        if (i!=shutdownConditionList.size()-1) {
          builder.append(";");
        }
      }
      return builder.toString();
    }

    public MultipleRoundShutdownCondition deserialize(String str) throws IOException {
      String[] splits = str.split(";");
      shutdownConditionList = new ArrayList<SimpleShutdownCondition>();
      for (String split : splits) {
        SimpleShutdownCondition condition = new SimpleShutdownCondition();
        shutdownConditionList.add(condition.deserialize(split));
      }
      return this;
    }

    public SimpleShutdownCondition getSimpleShutdownCondition(int index) {
      return shutdownConditionList.get(index);
    }

    public int size() {
      return shutdownConditionList.size();
    }
  }
}
