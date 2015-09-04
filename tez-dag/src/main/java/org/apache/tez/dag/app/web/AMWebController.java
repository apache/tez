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

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.WebAppException;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;

public class AMWebController extends Controller {

  private final static Logger LOG = LoggerFactory.getLogger(AMWebController.class);

  // HTTP CORS Response Headers
  static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  static final String ACCESS_CONTROL_ALLOW_CREDENTIALS = "Access-Control-Allow-Credentials";
  static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  static final String ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
  static final String ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";

  // CORS default responses.
  static final String ALLOWED_METHODS = "GET, HEAD";
  static final String ALLOWED_HEADERS = "X-Requested-With,Content-Type,Accept,Origin";

  static final String DAG_PROGRESS = "dagProgress";
  static final String VERTEX_PROGRESS = "vertexProgress";
  static final String VERTEX_PROGRESSES = "vertexProgresses";

  static final int MAX_VERTICES_QUERIED = 100;
  public static final String VERSION = "2";

  private AppContext appContext;
  private String historyUrl;

  @Inject
  public AMWebController(RequestContext requestContext,
                         AppContext appContext,
                         @Named("TezUIHistoryURL") String historyUrl) {
    super(requestContext);
    this.appContext = appContext;
    this.historyUrl = historyUrl;
  }

  @Override
  public void index() {
    ui();
  }

  public void ui() {
    render(StaticAMView.class);
  }

  public void main() {
    ui();
  }

  public void about() {
    renderJSON("Tez AM UI WebServices");
  }

  @VisibleForTesting
  public void setCorsHeaders() {
    final HttpServletResponse res = response();

    /*
     * ideally the Origin and other CORS headers should be checked and response headers set only
     * if it matches the allowed origins. however rm does not forward these headers.
     */
    String historyUrlBase = appContext.getAMConf().get(TezConfiguration.TEZ_HISTORY_URL_BASE, "");
    String origin = null;
    try {
      URL url = new URL(historyUrlBase);
      origin = url.getProtocol() + "://" + url.getAuthority();
    } catch (MalformedURLException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invalid url set for tez history url base: " + historyUrlBase, e);
      }
    }

    if (origin != null) {
      res.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    res.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    res.setHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, Boolean.TRUE.toString());
    res.setHeader(ACCESS_CONTROL_ALLOW_HEADERS, ALLOWED_HEADERS);
    res.setHeader(ACCESS_CONTROL_MAX_AGE, "1800");
  }

  void sendErrorResponse(int sc, String msg, Exception e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(msg, e);
    }

    try {
      response().sendError(sc, msg);
    } catch (IOException e1) {
      throw new WebAppException(e);
    }
  }

  @VisibleForTesting
  static boolean _hasAccess(UserGroupInformation callerUGI, AppContext appContext) {
    if (callerUGI == null) {
      // Allow anonymous access iff acls disabled
      return !appContext.getAMACLManager().isAclsEnabled();
    }
    return appContext.getAMACLManager().checkDAGViewAccess(callerUGI);
  }

  public boolean hasAccess() {
    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null && !remoteUser.isEmpty()) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }

    return _hasAccess(callerUGI, appContext);
  }

  public void getDagProgress() {

    setCorsHeaders();

    if (!hasAccess()) {
      sendErrorResponse(HttpServletResponse.SC_UNAUTHORIZED, "Access denied for user: " +
          request().getRemoteUser(), null);
      return;
    }

    int dagID;
    try {
      dagID = getQueryParamInt(WebUIService.DAG_ID);
    } catch (NumberFormatException e) {
      sendErrorResponse(HttpServletResponse.SC_BAD_REQUEST, "Invalid dag id:", e);
      return;
    }

    DAG currentDAG = appContext.getCurrentDAG();

    if (currentDAG == null || dagID != currentDAG.getID().getId()) {
      sendErrorResponse(HttpServletResponse.SC_NOT_FOUND, "Not current Dag: " + dagID, null);
      return;
    }

    Map<String, ProgressInfo> result = new HashMap<String, ProgressInfo>();
    result.put(DAG_PROGRESS,
        new ProgressInfo(currentDAG.getID().toString(), currentDAG.getProgress()));
    renderJSON(result);
  }

  public void getVertexProgress() {
    int dagID;
    int vertexID;

    setCorsHeaders();

    if (!hasAccess()) {
      sendErrorResponse(HttpServletResponse.SC_UNAUTHORIZED, "Access denied for user: " +
          request().getRemoteUser(), null);
      return;
    }

    try {
      dagID = getQueryParamInt(WebUIService.DAG_ID);
      vertexID = getQueryParamInt(WebUIService.VERTEX_ID);
    } catch (NumberFormatException e) {
      sendErrorResponse(HttpServletResponse.SC_BAD_REQUEST, "Invalid dag or vertex id", e);
      return;
    }

    DAG currentDAG = appContext.getCurrentDAG();

    if (currentDAG == null || currentDAG.getID().getId() != dagID) {
      sendErrorResponse(HttpServletResponse.SC_NOT_FOUND, "Not current Dag: " + dagID, null);
      return;
    }

    final TezVertexID tezVertexID = TezVertexID.getInstance(currentDAG.getID(), vertexID);
    Vertex vertex = currentDAG.getVertex(tezVertexID);
    if (vertex == null) {
      sendErrorResponse(HttpServletResponse.SC_NOT_FOUND, "vertex not found: " + vertexID, null);
      return;
    }

    Map<String, ProgressInfo> result = new HashMap<String, ProgressInfo>();
    result.put(VERTEX_PROGRESS, new ProgressInfo(tezVertexID.toString(), vertex.getProgress()));
    renderJSON(result);
  }


  Collection<Vertex> getVerticesByIdx(DAG dag, Collection<Integer> indexes) {
    Collection<Vertex> vertices = new ArrayList<Vertex>(indexes.size());
    final TezDAGID tezDAGID = dag.getID();

    for (Integer idx : indexes) {
      final TezVertexID tezVertexID = TezVertexID.getInstance(tezDAGID, idx);
      if (tezVertexID == null) {
        continue;
      }
      final Vertex vertex = dag.getVertex(tezVertexID);
      if (vertex != null) {
        vertices.add(vertex);
      }
    }

    return  vertices;
  }

  int getQueryParamInt(String name) throws NumberFormatException {
    final String valueStr = $(name).trim();

    return Integer.parseInt(valueStr);
  }

  public void getVertexProgresses() {
    int dagID;

    setCorsHeaders();
    if (!hasAccess()) {
      sendErrorResponse(HttpServletResponse.SC_UNAUTHORIZED, "Access denied for user: " +
          request().getRemoteUser(), null);
      return;
    }

    List<Integer> vertexIDs = new ArrayList<Integer>();
    try {
      dagID = getQueryParamInt(WebUIService.DAG_ID);
      for (String vertexIDStr : $(WebUIService.VERTEX_ID).trim().split(",", MAX_VERTICES_QUERIED)) {
        vertexIDs.add(Integer.parseInt(vertexIDStr));
      }
    } catch (NumberFormatException e) {
      sendErrorResponse(HttpServletResponse.SC_BAD_REQUEST, "Invalid dag or vertices id", e);
      return;
    }

    DAG currentDAG = appContext.getCurrentDAG();
    if (currentDAG == null || currentDAG.getID().getId() != dagID) {
      sendErrorResponse(HttpServletResponse.SC_NOT_FOUND, "Not current Dag: " + dagID, null);
      return;
    }

    Collection<Vertex> vertices;
    if (vertexIDs.isEmpty()) {
      vertices = currentDAG.getVertices().values();
    } else {
      vertices = getVerticesByIdx(currentDAG, vertexIDs);
    }

    Collection<ProgressInfo> progresses = new ArrayList<ProgressInfo>(vertices.size());
    for(Vertex vertex : vertices) {
      progresses.add(new ProgressInfo(vertex.getVertexId().toString(), vertex.getProgress()));
    }

    Map<String, Collection<ProgressInfo>> result = new HashMap<String, Collection<ProgressInfo>>();
    result.put(VERTEX_PROGRESSES, progresses);
    renderJSON(result);
  }

  // AM WebApi V2.
  @VisibleForTesting
  protected boolean setupResponse() {
    setCorsHeaders();

    if (!hasAccess()) {
      sendErrorResponse(HttpServletResponse.SC_UNAUTHORIZED, "Access denied for user: " +
          request().getRemoteUser(), null);
      return false;
    }

    return true;
  }

  DAG checkAndGetDAGFromRequest() {
    DAG dag = null;
    int errorCode = HttpServletResponse.SC_OK;
    String message = null;
    Exception ex = null;
    try {
      int dagID = getQueryParamInt(WebUIService.DAG_ID);
      dag = appContext.getCurrentDAG();
      if (dag == null || dag.getID().getId() != dagID) {
        errorCode = HttpServletResponse.SC_NOT_FOUND;
        message = "Not current Dag: " + dagID;
      }
    } catch (NumberFormatException e) {
      errorCode = HttpServletResponse.SC_BAD_REQUEST;
      message = "Invalid dag id";
      ex = e;
    }

    if (errorCode != HttpServletResponse.SC_OK) {
      dag = null;
      sendErrorResponse(errorCode, message, ex);
    }

    return dag;
  }

  Collection<Integer> getVertexIDsFromRequest() {
    final String valueStr = $(WebUIService.VERTEX_ID).trim();

    List<Integer> vertexIDs = new ArrayList<Integer>();
    if (!valueStr.equals("")) {
      String[] vertexIdsStr = valueStr.split(",", MAX_VERTICES_QUERIED);

      try {
        for (String vertexIdStr : vertexIdsStr) {
          int vertexId = Integer.parseInt(vertexIdStr);
          vertexIDs.add(vertexId);
        }
      } catch (NumberFormatException nfe) {
        sendErrorResponse(HttpServletResponse.SC_BAD_REQUEST,
            "invalid vertex ID passed in as parameter", nfe);
        vertexIDs = null;
      }
    }

    return vertexIDs;
  }

  public void getDagInfo() {
    if (!setupResponse()) {
      return;
    }

    DAG dag = checkAndGetDAGFromRequest();
    if (dag == null) {
      return;
    }

    Map<String, String> dagInfo = new HashMap<String, String>();
    dagInfo.put("id", dag.getID().toString());
    dagInfo.put("progress", Float.toString(dag.getProgress()));
    dagInfo.put("status", dag.getState().toString());

    renderJSON(ImmutableMap.of(
        "dag", dagInfo
    ));
  }

  private Map<String,String> getVertexInfoMap(Vertex vertex) {
    Map<String, String> vertexInfo = new HashMap<String, String>();
    vertexInfo.put("id", vertex.getVertexId().toString());
    vertexInfo.put("status", vertex.getState().toString());
    vertexInfo.put("progress", Float.toString(vertex.getProgress()));

    ProgressBuilder vertexProgress = vertex.getVertexProgress();
    vertexInfo.put("totalTasks", Integer.toString(vertexProgress.getTotalTaskCount()));
    vertexInfo.put("runningTasks", Integer.toString(vertexProgress.getRunningTaskCount()));
    vertexInfo.put("succeededTasks", Integer.toString(vertexProgress.getSucceededTaskCount()));

    vertexInfo.put("failedTaskAttempts", Integer.toString(vertexProgress.getFailedTaskAttemptCount()));
    vertexInfo.put("killedTaskAttempts", Integer.toString(vertexProgress.getKilledTaskAttemptCount()));

    return vertexInfo;
  }

  public void getVerticesInfo() {
    if (!setupResponse()) {
      return;
    }

    DAG dag = checkAndGetDAGFromRequest();
    if (dag == null) {
      return;
    }

    Collection<Integer> requestedIDs = getVertexIDsFromRequest();

    if (requestedIDs == null) {
      return;
    }

    Collection<Vertex> vertexList;
    if (requestedIDs.size() == 0) {
      // no ids specified return all.
      vertexList = dag.getVertices().values();
    } else {
      vertexList = getVerticesByIdx(dag, requestedIDs);
    }

    ArrayList<Map<String, String>> verticesInfo = new ArrayList<Map<String, String>>();
    for(Vertex v : vertexList) {
      verticesInfo.add(getVertexInfoMap(v));
    }

    renderJSON(ImmutableMap.of(
        "vertices", verticesInfo
    ));
  }

  @Override
  @VisibleForTesting
  public void renderJSON(Object object) {
    super.renderJSON(object);
  }

  public static class StaticAMView extends View {
    @Inject
    AppContext appContext;
    @Inject
    @Named("TezUIHistoryURL") String historyUrl;

    @Override
    public void render() {
      response().setContentType(MimeType.HTML);
      PrintWriter pw = writer();
      pw.write("<html>");
      pw.write("<head>");
      pw.write("<meta charset=\"utf-8\">");
      pw.write("<title>Redirecting to Tez UI</title>");
      pw.write("</head>");
      pw.write("<body>");
      if (historyUrl == null || historyUrl.isEmpty()) {
        pw.write("<h1>Tez UI Url is not defined.</h1>" +
            "<p>To enable tracking url pointing to Tez UI, set the config <b>" +
            TezConfiguration.TEZ_HISTORY_URL_BASE + "</b> in the tez-site.xml.</p>");
      } else {
        pw.write("<h1>Redirecting to Tez UI</h1>. <p>If you are not redirected shortly, click " +
            "<a href='" + historyUrl + "'><b>here</b></a></p>"
        );
        pw.write("<script type='text/javascript'>setTimeout(function() { " +
          "window.location.replace('" + historyUrl + "');" +
          "}, 0); </script>");
      }
      pw.write("</body>");
      pw.write("</html>");
      pw.flush();
    }
  }

  @VisibleForTesting
  static class ProgressInfo {
    private String id;

    public float getProgress() {
      return progress;
    }

    public String getId() {
      return id;
    }

    private float progress;

    public ProgressInfo(String id, float progress) {
      this.id = id;
      this.progress = progress;
    }
  }
}
