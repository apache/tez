package org.apache.tez.dag.api.client;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.tez.client.PreWarmContext;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.records.TezDAGID;

public class DAGClientHandler {

  private Log LOG = LogFactory.getLog(DAGClientHandler.class);

  private DAGAppMaster dagAppMaster;
  
  public DAGClientHandler(DAGAppMaster dagAppMaster) {
    this.dagAppMaster = dagAppMaster;
  }

  private DAG getCurrentDAG() {
    return dagAppMaster.getContext().getCurrentDAG();
  }

  public List<String> getAllDAGs() throws TezException {
    return Collections.singletonList(getCurrentDAG().getID().toString());
  }

  public DAGStatus getDAGStatus(String dagIdStr,
      Set<StatusGetOpts> statusOptions) throws TezException {
    return getDAG(dagIdStr).getDAGStatus(statusOptions);
  }

  public VertexStatus getVertexStatus(String dagIdStr, String vertexName,
      Set<StatusGetOpts> statusOptions) throws TezException {
    VertexStatus status =
        getDAG(dagIdStr).getVertexStatus(vertexName, statusOptions);
    if (status == null) {
      throw new TezException("Unknown vertexName: " + vertexName);
    }

    return status;
  }

  DAG getDAG(String dagIdStr) throws TezException {
    TezDAGID dagId = TezDAGID.fromString(dagIdStr);
    if (dagId == null) {
      throw new TezException("Bad dagId: " + dagIdStr);
    }

    DAG currentDAG = getCurrentDAG();
    if (currentDAG == null) {
      throw new TezException("No running dag at present");
    }
    if (!currentDAG.getID().toString().equals(dagId.toString())) {
      LOG.warn("Current DAGID : "
          + (currentDAG.getID() == null ? "NULL" : currentDAG.getID())
          + ", Looking for string (not found): " + dagIdStr + ", dagIdObj: "
          + dagId);
      throw new TezException("Unknown dagId: " + dagIdStr);
    }

    return currentDAG;
  }

  public void tryKillDAG(String dagIdStr) throws TezException {
    DAG dag = getDAG(dagIdStr);
    LOG.info("Sending client kill to dag: " + dagIdStr);
    dagAppMaster.tryKillDAG(dag);
  }

  public synchronized String submitDAG(DAGPlan dagPlan,
      Map<String, LocalResource> additionalAmResources) throws TezException {
    return dagAppMaster.submitDAGToAppMaster(dagPlan, additionalAmResources);
  }

  public synchronized void shutdownAM() {
    LOG.info("Received message to shutdown AM");
    if (dagAppMaster != null) {
      dagAppMaster.shutdownTezAM();
    }
  }

  public synchronized TezAppMasterStatus getSessionStatus() throws TezException {
    if (!dagAppMaster.isSession()) {
      throw new TezException("Unsupported operation as AM not running in"
          + " session mode");
    }
    switch (dagAppMaster.getState()) {
    case NEW:
    case INITED:
      return TezAppMasterStatus.INITIALIZING;
    case IDLE:
      return TezAppMasterStatus.READY;
    case RECOVERING:
    case RUNNING:
      return TezAppMasterStatus.RUNNING;
    case ERROR:
    case FAILED:
    case SUCCEEDED:
    case KILLED:
      return TezAppMasterStatus.SHUTDOWN;
    }
    return TezAppMasterStatus.INITIALIZING;
  }

  public synchronized void preWarmContainers(PreWarmContext preWarmContext)
      throws TezException {
    if (dagAppMaster == null) {
      throw new TezException("DAG App Master is not initialized");
    }
    dagAppMaster.startPreWarmContainers(preWarmContext);
  }

}
