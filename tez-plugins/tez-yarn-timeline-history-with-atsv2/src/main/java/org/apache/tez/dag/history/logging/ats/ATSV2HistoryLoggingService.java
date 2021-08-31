package org.apache.tez.dag.history.logging.ats;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.rm.TezAMRMClientAsync;
import org.apache.tez.dag.app.rm.TezAMRMClientAsyncProvider;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ATSV2HistoryLoggingService extends HistoryLoggingService {

  private static final Logger LOG =
    LoggerFactory.getLogger(ATSV2HistoryLoggingService.class);
  public static final String TEZ_PREFIX = "tez_";

  TimelineV2Client timelineV2Client;
  private HashSet<TezDAGID> skippedDAGs = new HashSet<TezDAGID>();

  BlockingQueue<DAGHistoryEvent> eventQueue =
    new LinkedBlockingQueue<DAGHistoryEvent>();
  protected Thread eventHandlingThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private AtomicBoolean registered = new AtomicBoolean(false);
  private final Object lock = new Object();
  private int eventCounter = 0;
  private int eventsProcessed = 0;
  private boolean timelineServiceEnabled = true;
  private long maxTimeToWaitOnShutdown;
  private boolean waitForeverOnShutdown = false;

  private long maxPollingTimeMillis;

  // Number of bytes of config which can be published in one shot to ATSv2.
  public static final int ATS_CONFIG_PUBLISH_SIZE_BYTES = 10 * 1024;

  public ATSV2HistoryLoggingService() {
    super(ATSV2HistoryLoggingService.class.getName());
  }

  @Override protected void serviceInit(Configuration conf) throws Exception {
    boolean historyLoggingEnabled =
      conf.getBoolean(TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED,
        TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED_DEFAULT);
    if (!historyLoggingEnabled) {
      LOG.info("ATSV2Service: History Logging disabled. "
        + TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED + " set to false");
      return;
    }

    maxTimeToWaitOnShutdown =
      conf.getLong(TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS,
        TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT);
    maxPollingTimeMillis =
      conf.getInt(TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT,
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT_DEFAULT);

    if (maxTimeToWaitOnShutdown < 0) {
      waitForeverOnShutdown = true;
    }

    timelineServiceEnabled = conf.getBoolean(
      YarnConfiguration.TIMELINE_SERVICE_ENABLED,
      YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED);
    if (!timelineServiceEnabled &&
      conf.get(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, "")
        .equals(ATSV2HistoryLoggingService.class.getName())) {
      LOG.warn(ATSV2HistoryLoggingService.class.getName()
        + " is disabled due to Timeline Service being disabled, "
        + YarnConfiguration.TIMELINE_SERVICE_ENABLED + " set to false");
    }
    super.serviceInit(conf);
  }

  @Override protected void serviceStart() throws Exception {
    if (!timelineServiceEnabled) {
      return;
    }
    super.serviceStart();
  }

  private void createAndRegisterTimelineClient(TezAMRMClientAsync amRmClient) throws Exception {
    if (amRmClient == null) {
      LOG.warn("Cannot register TimeClient without amRmClient");
      return;
    }
    Configuration conf = getConfig();
    if (timelineServiceEnabled) {
      timelineV2Client =
        TimelineV2Client.createTimelineClient(appContext.getApplicationID());
      timelineV2Client.init(conf);
      LOG.info("timelineV2Client inited.");
      amRmClient.registerTimelineV2Client(timelineV2Client);

      LOG.info("timelineV2Client registered using amRmClient.");
      timelineV2Client.start();

      eventHandlingThread = new Thread(createThread());
      eventHandlingThread.setName("ATSV2HistoryEventHandlingThread");
      eventHandlingThread.start();

      registered.set(true);
    }
  }

  @Override protected void serviceStop() throws Exception {
    if (!timelineServiceEnabled || !registered.get()) {
      return;
    }
    LOG.info(
      "Stopping ATSV2Service" + ", eventQueueBacklog=" + eventQueue.size());
    stopped.set(true);
    //   if (eventHandlingThread != null) {
    //    eventHandlingThread.interrupt();
    //  }
    try {
      TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(),
        appContext.getApplicationID());
      synchronized (lock) {
        if (eventHandlingThread != null) {
          eventHandlingThread.interrupt();
        }
        if (!eventQueue.isEmpty()) {
          LOG.warn(
            "ATSV2Service being stopped" + ", eventQueueBacklog=" + eventQueue
              .size() + ", maxTimeLeftToFlush=" + maxTimeToWaitOnShutdown
              + ", waitForever=" + waitForeverOnShutdown);
          long startTime = appContext.getClock().getTime();
          long endTime = startTime + maxTimeToWaitOnShutdown;
          while (waitForeverOnShutdown || (endTime >= appContext.getClock()
            .getTime())) {
            try {
              DAGHistoryEvent event =
                eventQueue.poll(maxPollingTimeMillis, TimeUnit.MILLISECONDS);
              if (event == null) {
                LOG.info("Event queue empty, stopping ATS Service");
                break;
              }
              try {
                handleEvents(event);
              } catch (Exception e) {
                LOG.warn("Error handling event", e);
              }
            } catch (InterruptedException e) {
              LOG.info("ATSService interrupted while shutting down. Exiting."
                + " EventQueueBacklog=" + eventQueue.size());
            }
          }
        }
      }
    } finally {
      appContext.getHadoopShim().clearHadoopCallerContext();
    }
    if (!eventQueue.isEmpty()) {
      LOG.warn("Did not finish flushing eventQueue before stopping ATSService"
        + ", eventQueueBacklog=" + eventQueue.size());
    }
    if (timelineV2Client != null) {
      timelineV2Client.stop();
    }
    // stop all the components
    super.serviceStop();
  }

  private void handleEvents(DAGHistoryEvent event) {
    TezDAGID dagID = event.getDagID();

    if (event.getDagID() != null && skippedDAGs.contains(event.getDagID())) {
      return;
    }
    LOG.info("createTimelineEntity called " + event.getHistoryEvent().getEventType());
    TimelineEntity timelineEntity = HistoryEventTimelineConversionAtsv2.createTimelineEntity(event.getHistoryEvent());

    try {
      // TODO identify which event need to publish in sync and async
      timelineV2Client.putEntitiesAsync(timelineEntity);
    } catch (IOException | YarnException e) {
      LOG.error(
        "Failed to publish Event " + event.getHistoryEvent().getEventType()
          + " for the dag : " + dagID, e);
      return;
    }

    if (event.getHistoryEvent().getEventType()
      .equals(HistoryEventType.DAG_SUBMITTED)) {
      publishConfigsOnDagSubmittedEvent(
        (DAGSubmittedEvent) event.getHistoryEvent());
    }
  }

  private void publishConfigsOnDagSubmittedEvent(DAGSubmittedEvent event) {
    String dagId = event.getDagID().toString();
    String entityId = TEZ_PREFIX + dagId;
    String entityType = EntityTypes.TEZ_DAG_ID.name();
    TimelineEntity dagConfigEntity = new TimelineEntity();
    dagConfigEntity.setId(entityId);
    dagConfigEntity.setType(entityType);

    try {
      int configSize = 0;
      for (Map.Entry<String, String> entry : event.getConf()) {
        int size = entry.getKey().length() + entry.getValue().length();
        configSize += size;
        if (configSize > ATS_CONFIG_PUBLISH_SIZE_BYTES) {
          if (dagConfigEntity.getConfigs().size() > 0) {
            timelineV2Client.putEntitiesAsync(dagConfigEntity);
            // create new entity
            dagConfigEntity = new TimelineEntity();
            dagConfigEntity.setId(entityId);
            dagConfigEntity.setType(entityType);
          }
          configSize = size;
        }
        dagConfigEntity.addConfig(entry.getKey(), entry.getValue());
      }
      if (configSize > 0) {
        LOG.info("before timelineV2Client putentities" + dagConfigEntity);
        timelineV2Client.putEntities(dagConfigEntity);
        LOG.info("after timelineV2Client putentities");
      }
    } catch (IOException | YarnException e) {
      LOG.error("Exception while publishing configs on JOB_SUBMITTED Event "
        + " for the job : " + event.getDagID(), e);
    }
  }

  @Override public void handle(DAGHistoryEvent event) {
    if (timelineServiceEnabled) {
      if (!registered.get()) {
        TezAMRMClientAsync amRmClient = TezAMRMClientAsyncProvider.getAMRMClientAsync();
        if (amRmClient != null) {
          try {
            createAndRegisterTimelineClient(amRmClient);
          } catch(Exception ex){
            LOG.error("Error while create and register timelineClient", ex);

          }
        }
      }
      eventQueue.add(event);
    }
  }

  Runnable createThread() {
    return new Runnable() {
      @Override public void run() {
        boolean interrupted = false;
        TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(),
          appContext.getApplicationID());
        while (!stopped.get() && !Thread.currentThread().isInterrupted()
          && !interrupted) {

          // Log the size of the event-queue every so often.
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            if (eventsProcessed != 0 && !eventQueue.isEmpty()) {
              LOG.info("Event queue stats" + ", eventsProcessedSinceLastUpdate="
                + eventsProcessed + ", eventQueueSize=" + eventQueue.size());
            }
            eventCounter = 0;
            eventsProcessed = 0;
          } else {
            ++eventCounter;
          }

          synchronized (lock) {
            try {
              DAGHistoryEvent event =
                eventQueue.poll(maxPollingTimeMillis, TimeUnit.MILLISECONDS);
              if (event == null) {
                continue;
              }
              if (!isValidEvent(event)) {
                continue;
              }

              try {
                handleEvents(event);
                eventsProcessed += 1;
              } catch (Exception e) {
                LOG.warn("Error handling events", e);
              }
            } catch (InterruptedException e) {
              // Finish processing events and then return
              interrupted = true;
            }
          }
        }
      }
    };
  }

  private boolean isValidEvent(DAGHistoryEvent event) {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    TezDAGID dagId = event.getDagID();

    if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
      DAGSubmittedEvent dagSubmittedEvent =
        (DAGSubmittedEvent) event.getHistoryEvent();
      String dagName = dagSubmittedEvent.getDAGName();
      if ((dagName != null && dagName
        .startsWith(TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX))
        || (!dagSubmittedEvent.isHistoryLoggingEnabled())) {
        // Skip recording pre-warm DAG events
        skippedDAGs.add(dagId);
        return false;
      }
    }
    if (eventType.equals(HistoryEventType.DAG_RECOVERED)) {
      DAGRecoveredEvent dagRecoveredEvent =
        (DAGRecoveredEvent) event.getHistoryEvent();
      if (!dagRecoveredEvent.isHistoryLoggingEnabled()) {
        skippedDAGs.add(dagRecoveredEvent.getDagID());
        return false;
      }
    }
    if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
      // Remove from set to keep size small
      // No more events should be seen after this point.
      if (skippedDAGs.remove(dagId)) {
        return false;
      }
    }

    if (dagId != null && skippedDAGs.contains(dagId)) {
      // Skip pre-warm DAGs
      return false;
    }

    return true;
  }

}


