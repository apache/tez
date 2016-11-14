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

package org.apache.tez.dag.app.web;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;

public class WebUIService extends AbstractService {
  private static final String WS_PREFIX = "/ui/ws/v1/tez/";
  private static final String WS_PREFIX_V2 = "/ui/ws/v2/tez/";
  public static final String VERTEX_ID = "vertexID";
  public static final String DAG_ID = "dagID";
  public static final String TASK_ID = "taskID";
  public static final String ATTEMPT_ID = "attemptID";
  public static final String COUNTERS = "counters";

  public static final String LIMIT = "limit";

  private static final Logger LOG = LoggerFactory.getLogger(WebUIService.class);

  private final AppContext context;
  private TezAMWebApp tezAMWebApp;
  private WebApp webApp;
  private String trackingUrl = "";
  private String historyUrl = "";

  public WebUIService(AppContext context) {
    super(WebUIService.class.getName());
    this.context = context;
    this.tezAMWebApp = new TezAMWebApp(context);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration config = new Configuration(conf);
    if (historyUrl == null || historyUrl.isEmpty()) {
      LOG.error("Tez UI History URL is not set");
    } else {
      LOG.info("Tez UI History URL: " + historyUrl);
    }

    if (tezAMWebApp != null) {
      this.tezAMWebApp.setHistoryUrl(historyUrl);
    }
    super.serviceInit(config);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (tezAMWebApp != null) {
      // use AmIpFilter to restrict connections only from the rm proxy
      Configuration conf = getConfig();
      conf.set("hadoop.http.filter.initializers",
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      try {
        // Explicitly disabling SSL for the web service. For https we do not want AM users to allow
        // access to the keystore file for opening SSL listener. We can trust RM/NM to issue SSL
        // certificates, however AM user is not trusted.
        // ideally the withHttpPolicy should be used, however hadoop 2.2 does not have the api
        conf.set("yarn.http.policy", "HTTP_ONLY");
        this.webApp = WebApps
            .$for(this.tezAMWebApp)
            .with(conf)
            .start(this.tezAMWebApp);
        InetSocketAddress address = webApp.getListenerAddress();
        if (address != null) {
          InetSocketAddress bindAddress = NetUtils.createSocketAddrForHost(
              context.getAppMaster().getAppNMHost(), address.getPort());
          String hostname = context.getAppMaster().getAppNMHost();
          final int port = address.getPort();
          if (bindAddress.getAddress() != null
              && bindAddress.getAddress().getCanonicalHostName() != null) {
            hostname = bindAddress.getAddress().getCanonicalHostName();
          } else {
            LOG.warn("Failed to resolve canonical hostname for "
                + context.getAppMaster().getAppNMHost());
          }
          trackingUrl = "http://" + hostname + ":" + port + "/ui/";
          LOG.info("Instantiated WebUIService at " + trackingUrl);
        }
      } catch (Exception e) {
        LOG.error("Tez UI WebService failed to start.", e);
        throw new TezUncheckedException(e);
      }
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.webApp != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopping WebApp");
      }
      this.webApp.stop();
    }
    super.serviceStop();
  }

  public String getTrackingURL() {
    return trackingUrl;
  }

  public String getHistoryUrl() {
    return historyUrl;
  }

  public void setHistoryUrl(String historyUrl) {
    this.historyUrl = historyUrl;
  }

  private static class TezAMWebApp extends WebApp implements YarnWebParams {

    private String historyUrl;
    AppContext context;

    public TezAMWebApp(AppContext context) {
      this.context = context;
    }

    public void setHistoryUrl(String historyUrl) {
      this.historyUrl = historyUrl;
    }

    @Override
    public void setup() {
      Preconditions.checkArgument(historyUrl != null);
      bind(AppContext.class).toInstance(context);
      bind(String.class).annotatedWith(Names.named("TezUIHistoryURL")).toInstance(historyUrl);
      route("/", AMWebController.class, "ui");
      route("/ui", AMWebController.class, "ui");
      route("/main", AMWebController.class, "main");
      route(WS_PREFIX + "about", AMWebController.class, "about");
      route(WS_PREFIX + pajoin("dagProgress", DAG_ID), AMWebController.class, "getDagProgress");
      route(WS_PREFIX + pajoin("vertexProgress", VERTEX_ID), AMWebController.class,
          "getVertexProgress");
      route(WS_PREFIX + pajoin("vertexProgresses", VERTEX_ID, DAG_ID), AMWebController.class,
          "getVertexProgresses");

      /**
       *  AM Web Service API V2
       *  The API facilitates end points that would serve the user with real-time data on dag,
       *  vertex and tasks.
       *
       *  Query Params:
       *    dagID    - Same as dagIndex. Expects one single value. (Its mandatory in all APIs)
       *    vertexID - Same as vertex index. Can be a list of comma separated values
       *    taskID   - Should be of the format <vertexIndex>_<taskIndex>. For instance task with
       *               index 5 in vertex 3 can be referenced using the id 3_5
       *    limit    - The max number of records returned. Currently supported only in tasksInfo.
       *               If not passed, limit would be taken as 100
       *
       *  APIs:
       *    /ui/ws/v2/tez/dagInfo
       *      Query param:
       *        - Accepts one single parameter, dagID
       *      Data returned:
       *        - Full id, progress, status
       *
       *    /ui/ws/v2/tez/verticesInfo
       *      Query params:
       *        - Accepts dagID and vertexID
       *        - vertexID is optional
       *        - If specified the respective vertices will be returned, else all vertices
       *          in the DAG will be returned
       *      Data returned:
       *        - Full id, progress, status, totalTasks, runningTasks, succeededTasks
       *          failedTaskAttempts, killedTaskAttempts
       *
       *    /ui/ws/v2/tez/tasksInfo
       *      Query params:
       *        - Accepts dagID, vertexID, taskID & limit
       *        - vertex and task IDs are optional
       *        - If taskID is passed: All (capped by limit) the specified tasks will be
       *          returned. vertexID if present wont be considered
       *        - IF vertexID is passed: All (capped by limit) tasks under the vertices
       *          will be returned
       *        - If just dagID is passed: All (capped by limit) tasks under the DAG
       *          will be returned
       *      Data returned:
       *        - Full id, progress, status
       */
      route(WS_PREFIX_V2 + pajoin("dagInfo", DAG_ID), AMWebController.class, "getDagInfo");
      route(WS_PREFIX_V2 + pajoin("verticesInfo", VERTEX_ID, DAG_ID), AMWebController.class, "getVerticesInfo");
      route(WS_PREFIX_V2 + pajoin("tasksInfo", TASK_ID, VERTEX_ID, DAG_ID), AMWebController.class,
          "getTasksInfo");
      route(WS_PREFIX_V2 + pajoin("attemptsInfo", ATTEMPT_ID, DAG_ID), AMWebController.class,
          "getAttemptsInfo");
    }
  }
}
