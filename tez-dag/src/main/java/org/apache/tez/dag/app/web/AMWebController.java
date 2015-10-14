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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.records.TezTaskAttemptID;
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

  static final int MAX_QUERIED = 100;
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
        new ProgressInfo(currentDAG.getID().toString(),
            currentDAG.getCompletedTaskProgress()));
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
    result.put(VERTEX_PROGRESS, new ProgressInfo(tezVertexID.toString(),
        vertex.getCompletedTaskProgress()));
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
      for (String vertexIDStr : $(WebUIService.VERTEX_ID).trim().split(",", MAX_QUERIED)) {
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
      progresses.add(new ProgressInfo(vertex.getVertexId().toString(),
          vertex.getCompletedTaskProgress()));
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
      String[] vertexIdsStr = valueStr.split(",", MAX_QUERIED);

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

  /**
   * Parse a params list in the format: CtrGroup/CtrName1,CtrName2;CtrGroup2;
   * @return nested structure of counter groups and names. Null if nothing specified.
   */
  Map<String, Set<String>> getCounterListFromRequest() {
    final String counterStr = $(WebUIService.COUNTERS).trim();
    if (counterStr == null || counterStr.isEmpty()) {
      return null;
    }

    String delimiter = ";";
    String groupDelimiter = "/";
    String counterDelimiter = ",";

    StringTokenizer tokenizer = new StringTokenizer(counterStr, delimiter);

    Map<String, Set<String>> counterList = new TreeMap<String, Set<String>>();
    while (tokenizer.hasMoreElements()) {
      String token = tokenizer.nextToken().trim();
      int pos = token.indexOf(groupDelimiter);
      if (pos == -1) {
        counterList.put(token, Collections.<String>emptySet());
        continue;
      }
      String counterGroup = token.substring(0, pos);
      Set<String> counters = Collections.<String>emptySet();
      if (pos < token.length() - 1) {
        String counterNames = token.substring(pos+1, token.length());
        counters = Sets.newHashSet(
            Splitter.on(counterDelimiter).omitEmptyStrings()
                .trimResults().split(counterNames));
      }
      counterList.put(counterGroup, counters);
    }
    return counterList;
  }


  List<String> splitString(String str, String delimiter, Integer limit) {
    List<String> items = new ArrayList<String>();

    StringTokenizer tokenizer = new StringTokenizer(str, delimiter);
    for(int count = 0; tokenizer.hasMoreElements() && count < limit; count ++) {
      items.add(tokenizer.nextToken());
    }

    return items;
  }

  /**
   * getIntegersFromRequest
   * Parses a query parameter with comma separated values and returns an array of integers.
   * The function returns null if any of the value is not an integer
   *
   * @param paramName {String}
   * @param limit {Integer} Maximum number of values to be taken
   *
   * @return {List<Integer>} List of parsed values
   */
  List<Integer> getIntegersFromRequest(String paramName, Integer limit) {
    String valuesStr = $(paramName).trim();

    List<Integer> values = new ArrayList<Integer>();
    if (!valuesStr.equals("")) {
      try {
        for (String valueStr : splitString(valuesStr, ",", limit)) {
          int value = Integer.parseInt(valueStr);
          values.add(value);
        }
      } catch (NumberFormatException nfe) {
        sendErrorResponse(HttpServletResponse.SC_BAD_REQUEST,
            String.format("invalid %s passed in as parameter", paramName), nfe);
        values = null;
      }
    }

    return values;
  }

  /**
   * getTaskIDsFromRequest
   * Takes in "1_0,1_3" and returns [[1,0],[1,3]]
   * Mainly to parse a query parameter with comma separated indexes. For vertex its the index,
   * for task its vertexIndex_taskIndex and for attempts its vertexIndex_taskIndex_attemptNo
   * The function returns null if any of the value is not an integer
   *
   * @param paramName {String}
   * @param limit {Integer} Maximum number of values to be taken
   *
   * @return {List<List<Integer>>} List of parsed values
   */
  List<List<Integer>> getIDsFromRequest(String paramName, Integer limit, Integer count) {
    String valuesStr = $(paramName).trim();

    List<List<Integer>> values = new ArrayList<List<Integer>>();
    if (!valuesStr.equals("")) {
      try {
        for (String valueStr : splitString(valuesStr, ",", limit)) {
          List<Integer> innerValues = new ArrayList<Integer>();
          String innerValueStrs[] = valueStr.split("_");
          if(innerValueStrs.length == count) {
            for (String innerValueStr : innerValueStrs) {
              int value = Integer.parseInt(innerValueStr);
              innerValues.add(value);
            }
            values.add(innerValues);
          }
        }
      } catch (NumberFormatException nfe) {
        sendErrorResponse(HttpServletResponse.SC_BAD_REQUEST,
            String.format("invalid %s passed in as parameter", paramName), nfe);
        values = null;
      }
    }

    return values;
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
    dagInfo.put("progress", Float.toString(dag.getCompletedTaskProgress()));
    dagInfo.put("status", dag.getState().toString());

    renderJSON(ImmutableMap.of(
        "dag", dagInfo
    ));
  }

  Map<String, Map<String, Long>> constructCounterMapInfo(TezCounters counters,
      Map<String, Set<String>> counterNames) {
    if (counterNames == null || counterNames.isEmpty()) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested counter names=" + counterNames.entrySet());
      LOG.debug("actual counters=" + counters);
    }

    Map<String, Map<String, Long>> counterInfo = new TreeMap<String, Map<String, Long>>();

    for (Entry<String, Set<String>> entry : counterNames.entrySet()) {
      Map<String, Long> matchedCounters = new HashMap<String, Long>();
      CounterGroup grpCounters = counters.getGroup(entry.getKey());
      for (TezCounter counter : grpCounters) {
        if (entry.getValue().isEmpty() || entry.getValue().contains(counter.getName())) {
          matchedCounters.put(counter.getName(), counter.getValue());
        }
      }
      counterInfo.put(entry.getKey(), matchedCounters);
    }

    return counterInfo;
  }

  private Map<String, Object> getVertexInfoMap(Vertex vertex,
                                               Map<String, Set<String>> counterNames) {
    Map<String, Object> vertexInfo = new HashMap<String, Object>();
    vertexInfo.put("id", vertex.getVertexId().toString());
    vertexInfo.put("status", vertex.getState().toString());
    vertexInfo.put("progress", Float.toString(vertex.getCompletedTaskProgress()));

    ProgressBuilder vertexProgress = vertex.getVertexProgress();
    vertexInfo.put("totalTasks", Integer.toString(vertexProgress.getTotalTaskCount()));
    vertexInfo.put("runningTasks", Integer.toString(vertexProgress.getRunningTaskCount()));
    vertexInfo.put("succeededTasks", Integer.toString(vertexProgress.getSucceededTaskCount()));

    vertexInfo.put("failedTaskAttempts",
        Integer.toString(vertexProgress.getFailedTaskAttemptCount()));
    vertexInfo.put("killedTaskAttempts",
        Integer.toString(vertexProgress.getKilledTaskAttemptCount()));

    if (counterNames != null && !counterNames.isEmpty()) {
      TezCounters counters = vertex.getCachedCounters();
      Map<String, Map<String, Long>> counterMap = constructCounterMapInfo(counters, counterNames);
      if (counterMap != null && !counterMap.isEmpty()) {
        vertexInfo.put("counters", counterMap);
      }
    }

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

    Map<String, Set<String>> counterNames = getCounterListFromRequest();

    Collection<Vertex> vertexList;
    if (requestedIDs.isEmpty()) {
      // no ids specified return all.
      vertexList = dag.getVertices().values();
    } else {
      vertexList = getVerticesByIdx(dag, requestedIDs);
    }

    ArrayList<Map<String, Object>> verticesInfo = new ArrayList<Map<String, Object>>();
    for(Vertex v : vertexList) {
      verticesInfo.add(getVertexInfoMap(v, counterNames));
    }

    renderJSON(ImmutableMap.of(
        "vertices", verticesInfo
    ));
  }

  Vertex getVertexFromIndex(DAG dag, Integer vertexIndex) {
    final TezVertexID tezVertexID = TezVertexID.getInstance(dag.getID(), vertexIndex);
    Vertex vertex = dag.getVertex(tezVertexID);
    return vertex;
  }

  /**
   * getRequestedTasks
   * Heart of getTasksInfo. Given a dag and a limit, based on the incoming query parameters
   * returns a list of task instances
   *
   * @param dag {DAG}
   * @param limit {Integer}
   */
  List<Task> getRequestedTasks(DAG dag, Integer limit) {
    List<Task> tasks = new ArrayList<Task>();

    List<List<Integer>> taskIDs = getIDsFromRequest(WebUIService.TASK_ID, limit, 2);
    if(taskIDs == null) {
      return null;
    }
    else if(!taskIDs.isEmpty()) {
      for (List<Integer> indexes : taskIDs) {
        Vertex vertex = getVertexFromIndex(dag, indexes.get(0));
        if(vertex == null) {
          continue;
        }
        Task task = vertex.getTask(indexes.get(1));
        if(task == null) {
          continue;
        }
        else {
          tasks.add(task);
        }

        if(tasks.size() >= limit) {
          break;
        }
      }
    }
    else {
      List<Integer> vertexIDs = getIntegersFromRequest(WebUIService.VERTEX_ID, limit);
      if(vertexIDs == null) {
        return null;
      }
      else if(!vertexIDs.isEmpty()) {
        for (Integer vertexID : vertexIDs) {
          Vertex vertex = getVertexFromIndex(dag, vertexID);
          if(vertex == null) {
            continue;
          }
          List<Task> vertexTasks = new ArrayList<Task>(vertex.getTasks().values());
          tasks.addAll(vertexTasks.subList(0, Math.min(vertexTasks.size(), limit - tasks.size())));

          if(tasks.size() >= limit) {
            break;
          }
        }
      }
      else {
        Collection<Vertex> vertices = dag.getVertices().values();
        for (Vertex vertex : vertices) {
          List<Task> vertexTasks = new ArrayList<Task>(vertex.getTasks().values());
          tasks.addAll(vertexTasks.subList(0, Math.min(vertexTasks.size(), limit - tasks.size())));

          if(tasks.size() >= limit) {
            break;
          }
        }
      }
    }

    return tasks;
  }

  /**
   * Renders the response JSON for tasksInfo API
   * The JSON will have an array of task objects under the key tasks.
   */
  public void getTasksInfo() {
    if (!setupResponse()) {
      return;
    }

    DAG dag = checkAndGetDAGFromRequest();
    if (dag == null) {
      return;
    }

    int limit = MAX_QUERIED;
    try {
      limit = getQueryParamInt(WebUIService.LIMIT);
    } catch (NumberFormatException e) {
      //Ignore
    }

    List<Task> tasks = getRequestedTasks(dag, limit);
    if(tasks == null) {
      return;
    }

    Map<String, Set<String>> counterNames = getCounterListFromRequest();

    ArrayList<Map<String, Object>> tasksInfo = new ArrayList<Map<String, Object>>();
    for(Task t : tasks) {
      Map<String, Object> taskInfo = new HashMap<String, Object>();
      taskInfo.put("id", t.getTaskId().toString());
      taskInfo.put("progress", Float.toString(t.getProgress()));
      taskInfo.put("status", t.getState().toString());

      TezCounters counters = t.getCounters();
      Map<String, Map<String, Long>> counterMap = constructCounterMapInfo(counters, counterNames);
      if (counterMap != null && !counterMap.isEmpty()) {
        taskInfo.put("counters", counterMap);
      }
      tasksInfo.add(taskInfo);
    }

    renderJSON(ImmutableMap.of(
      "tasks", tasksInfo
    ));
  }

  /**
   * getRequestedAttempts
   * Given a dag and a limit, based on the incoming query parameters. Used by getAttemptsInfo
   * returns a list of task instances
   *
   * @param dag {DAG}
   * @param limit {Integer}
   */
  List<TaskAttempt> getRequestedAttempts(DAG dag, Integer limit) {
    List<TaskAttempt> attempts = new ArrayList<TaskAttempt>();

    List<List<Integer>> attemptIDs = getIDsFromRequest(WebUIService.ATTEMPT_ID, limit, 3);
    if(attemptIDs == null) {
      return null;
    }
    else if(!attemptIDs.isEmpty()) {
      for (List<Integer> indexes : attemptIDs) {
        Vertex vertex = getVertexFromIndex(dag, indexes.get(0));
        if(vertex == null) {
          continue;
        }
        Task task = vertex.getTask(indexes.get(1));
        if(task == null) {
          continue;
        }

        TaskAttempt attempt = task.
            getAttempt(TezTaskAttemptID.getInstance(task.getTaskId(), indexes.get(2)));
        if(attempt == null) {
          continue;
        }
        else {
          attempts.add(attempt);
        }

        if(attempts.size() >= limit) {
          break;
        }
      }
    }

    return attempts;
  }

  /**
   * Renders the response JSON for attemptsInfo API
   * The JSON will have an array of attempt objects under the key attempts.
   */
  public void getAttemptsInfo() {
    if (!setupResponse()) {
      return;
    }

    DAG dag = checkAndGetDAGFromRequest();
    if (dag == null) {
      return;
    }

    int limit = MAX_QUERIED;
    try {
      limit = getQueryParamInt(WebUIService.LIMIT);
    } catch (NumberFormatException e) {
      //Ignore
    }

    List<TaskAttempt> attempts = getRequestedAttempts(dag, limit);
    if(attempts == null) {
      return;
    }

    Map<String, Set<String>> counterNames = getCounterListFromRequest();

    ArrayList<Map<String, Object>> attemptsInfo = new ArrayList<Map<String, Object>>();
    for(TaskAttempt a : attempts) {
      Map<String, Object> attemptInfo = new HashMap<String, Object>();
      attemptInfo.put("id", a.getID().toString());
      attemptInfo.put("progress", Float.toString(a.getProgress()));
      attemptInfo.put("status", a.getState().toString());

      TezCounters counters = a.getCounters();
      Map<String, Map<String, Long>> counterMap = constructCounterMapInfo(counters, counterNames);
      if (counterMap != null && !counterMap.isEmpty()) {
        attemptInfo.put("counters", counterMap);
      }
      attemptsInfo.add(attemptInfo);
    }

    renderJSON(ImmutableMap.of("attempts", attemptsInfo));
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
