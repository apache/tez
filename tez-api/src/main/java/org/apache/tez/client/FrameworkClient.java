/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.client;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.RPCUtil;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DAGSubmissionTimedOut;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGClientImpl;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGResponseProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;

@Private
public abstract class FrameworkClient {
  protected static final Logger LOG = LoggerFactory.getLogger(FrameworkClient.class);

  public static FrameworkClient createFrameworkClient(TezConfiguration tezConf) {

    boolean isLocal = tezConf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE, TezConfiguration.TEZ_LOCAL_MODE_DEFAULT);
    if (isLocal) {
      try {
        return ReflectionUtils.createClazzInstance("org.apache.tez.client.LocalClient");
      } catch (TezReflectionException e) {
        throw new TezUncheckedException("Fail to create LocalClient", e);
      }
    }
    return new TezYarnClient(YarnClient.createYarnClient());
  }

  /**
   * Initialize the framework client. </p>
   * <p/>
   * The actual implementation of FramworkClient may modify the configuration instances that are
   * passed in to configure required functionality
   *
   * @param tezConf  the {@link org.apache.tez.dag.api.TezConfiguration} instance being used by the
   *                 cluster
   */
  public abstract void init(TezConfiguration tezConf);

  public abstract void start();

  public abstract void stop();

  public abstract void close() throws IOException;

  public abstract YarnClientApplication createApplication() throws YarnException, IOException;

  public abstract ApplicationId submitApplication(ApplicationSubmissionContext appSubmissionContext)
      throws YarnException, IOException, TezException;

  public abstract void killApplication(ApplicationId appId) throws YarnException, IOException;

  public abstract ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException;

  public abstract boolean isRunning() throws IOException;

  public TezAppMasterStatus getAMStatus(Configuration conf, ApplicationId appId,
                                        UserGroupInformation ugi) throws TezException, ServiceException, IOException {
    DAGClientAMProtocolBlockingPB proxy = getProxy(conf, appId, ugi);

    if (proxy == null) {
      return TezAppMasterStatus.INITIALIZING;
    }
    GetAMStatusResponseProto response =
        proxy.getAMStatus(null, GetAMStatusRequestProto.newBuilder().build());
    return DagTypeConverters.convertTezAppMasterStatusFromProto(response.getStatus());
  }

  public DAGClient submitDag(DAG dag, SubmitDAGRequestProto request, String clientName,
                             ApplicationId sessionAppId, long clientTimeout, UserGroupInformation ugi, TezConfiguration tezConf)
      throws IOException, TezException, DAGSubmissionTimedOut {
    DAGClientAMProtocolBlockingPB proxy = null;
    try {
      proxy = waitForProxy(clientTimeout, tezConf, sessionAppId, ugi);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while trying to create a connection to the AM", e);
    }
    if (proxy == null) {
      try {
        LOG.warn("DAG submission to session timed out, stopping session");
        stop();
      } catch (Throwable t) {
        LOG.info("Got an exception when trying to stop session", t);
      }
      throw new DAGSubmissionTimedOut("Could not submit DAG to Tez Session"
          + ", timed out after " + clientTimeout + " seconds");
    }

    String dagId = null;
    try {
      SubmitDAGResponseProto response = proxy.submitDAG(null, request);
      // the following check is only for testing since the final class
      // SubmitDAGResponseProto cannot be mocked
      if (response != null) {
        dagId = response.getDagId();
      }
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
    }

    LOG.info("Submitted dag to TezSession"
        + ", sessionName=" + clientName
        + ", applicationId=" + sessionAppId
        + ", dagId=" + dagId
        + ", dagName=" + dag.getName());
    return new DAGClientImpl(sessionAppId, dagId, tezConf, this, ugi);
  }

  protected DAGClientAMProtocolBlockingPB waitForProxy(long clientTimeout, Configuration conf,
                                                       ApplicationId sessionAppId, UserGroupInformation ugi)
      throws IOException, TezException, InterruptedException {
    long startTime = System.currentTimeMillis();
    long endTime = startTime + (clientTimeout * 1000);
    DAGClientAMProtocolBlockingPB proxy = null;
    while (true) {
      proxy = TezClientUtils.getAMProxy(this, conf, sessionAppId, ugi);
      if (proxy != null) {
        break;
      }
      Thread.sleep(100L);
      if (clientTimeout != -1 && System.currentTimeMillis() > endTime) {
        break;
      }
    }
    return proxy;
  }

  /**
   * Shuts down session and returns a boolean=true if a proxy was successfully created and through
   * that proxy a shutdownSession was called.
   */
  public boolean shutdownSession(Configuration conf, ApplicationId sessionAppId,
                                 UserGroupInformation ugi) throws TezException, IOException, ServiceException {
    DAGClientAMProtocolBlockingPB proxy = getProxy(conf, sessionAppId, ugi);
    if (proxy != null) {
      ShutdownSessionRequestProto request = ShutdownSessionRequestProto.newBuilder().build();
      proxy.shutdownSession(null, request);
      return true;
    }
    return false;
  }

  protected DAGClientAMProtocolBlockingPB getProxy(Configuration conf, ApplicationId sessionAppId,
                                                   UserGroupInformation ugi) throws TezException, IOException {
    return TezClientUtils.getAMProxy(this, conf, sessionAppId, ugi);
  }
}
