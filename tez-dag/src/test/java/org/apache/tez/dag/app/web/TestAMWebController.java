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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

public class TestAMWebController {
  AppContext mockAppContext;
  Controller.RequestContext mockRequestContext;
  HttpServletResponse mockResponse;
  HttpServletRequest mockRequest;
  String[] userGroups = {};

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    mockAppContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.set(TezConfiguration.TEZ_HISTORY_URL_BASE, "http://uihost:9001/foo");
    when(mockAppContext.getAMConf()).thenReturn(conf);
    mockRequestContext = mock(Controller.RequestContext.class);
    mockResponse = mock(HttpServletResponse.class);
    mockRequest = mock(HttpServletRequest.class);
  }

  @Test(timeout = 5000)
  public void testCorsHeadersAreSet() {
    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);
    doReturn(mockResponse).when(spy).response();
    spy.setCorsHeaders();

    verify(mockResponse).setHeader("Access-Control-Allow-Origin", "http://uihost:9001");
    verify(mockResponse).setHeader("Access-Control-Allow-Credentials", "true");
    verify(mockResponse).setHeader("Access-Control-Allow-Methods", "GET, HEAD");
    verify(mockResponse).setHeader("Access-Control-Allow-Headers",
        "X-Requested-With,Content-Type,Accept,Origin");
  }

  @Test (timeout = 5000)
  public void sendErrorResponseIfNoAccess() throws Exception {
    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);

    doReturn(false).when(spy).hasAccess();
    doNothing().when(spy).setCorsHeaders();
    doReturn(mockResponse).when(spy).response();
    doReturn(mockRequest).when(spy).request();
    doReturn("dummyuser").when(mockRequest).getRemoteUser();

    spy.getDagProgress();
    verify(mockResponse).sendError(eq(HttpServletResponse.SC_UNAUTHORIZED), anyString());
    reset(mockResponse);

    spy.getVertexProgress();
    verify(mockResponse).sendError(eq(HttpServletResponse.SC_UNAUTHORIZED), anyString());
    reset(mockResponse);

    spy.getVertexProgresses();
    verify(mockResponse).sendError(eq(HttpServletResponse.SC_UNAUTHORIZED), anyString());
  }

  @Captor
  ArgumentCaptor<Map<String, AMWebController.ProgressInfo>> singleResultCaptor;

  @Test (timeout = 5000)
  public void testDagProgressResponse() {
    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);
    DAG mockDAG = mock(DAG.class);

    doReturn(true).when(spy).hasAccess();
    doNothing().when(spy).setCorsHeaders();
    doReturn("42").when(spy).$(WebUIService.DAG_ID);
    doReturn(mockResponse).when(spy).response();
    doReturn(TezDAGID.fromString("dag_1422960590892_0007_42")).when(mockDAG).getID();
    doReturn(66.0f).when(mockDAG).getCompletedTaskProgress();
    doReturn(mockDAG).when(mockAppContext).getCurrentDAG();
    doNothing().when(spy).renderJSON(any());
    spy.getDagProgress();
    verify(spy).renderJSON(singleResultCaptor.capture());

    final Map<String, AMWebController.ProgressInfo> result = singleResultCaptor.getValue();
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("dagProgress"));
    AMWebController.ProgressInfo progressInfo = result.get("dagProgress");
    Assert.assertTrue("dag_1422960590892_0007_42".equals(progressInfo.getId()));
    Assert.assertEquals(66.0, progressInfo.getProgress(), 0.1);
  }

  @Test (timeout = 5000)
  public void testVertexProgressResponse() {
    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);
    DAG mockDAG = mock(DAG.class);
    Vertex mockVertex = mock(Vertex.class);

    doReturn(true).when(spy).hasAccess();
    doReturn("42").when(spy).$(WebUIService.DAG_ID);
    doReturn("43").when(spy).$(WebUIService.VERTEX_ID);
    doReturn(mockResponse).when(spy).response();

    doReturn(TezDAGID.fromString("dag_1422960590892_0007_42")).when(mockDAG).getID();
    doReturn(mockDAG).when(mockAppContext).getCurrentDAG();
    doReturn(mockVertex).when(mockDAG).getVertex(any(TezVertexID.class));
    doReturn(66.0f).when(mockVertex).getCompletedTaskProgress();
    doNothing().when(spy).renderJSON(any());
    doNothing().when(spy).setCorsHeaders();

    spy.getVertexProgress();
    verify(spy).renderJSON(singleResultCaptor.capture());

    final Map<String, AMWebController.ProgressInfo> result = singleResultCaptor.getValue();
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("vertexProgress"));
    AMWebController.ProgressInfo progressInfo = result.get("vertexProgress");
    Assert.assertTrue("vertex_1422960590892_0007_42_43".equals(progressInfo.getId()));
    Assert.assertEquals(66.0f, progressInfo.getProgress(), 0.1);
  }

  @Test (timeout = 5000)
  public void testHasAccessWithAclsDisabled() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.TEZ_AM_ACLS_ENABLED, false);
    ACLManager aclManager = new ACLManager("amUser", conf);

    when(mockAppContext.getAMACLManager()).thenReturn(aclManager);

    Assert.assertEquals(true, AMWebController._hasAccess(null, mockAppContext));

    UserGroupInformation mockUser = UserGroupInformation.createUserForTesting(
        "mockUser", userGroups);
    Assert.assertEquals(true, AMWebController._hasAccess(mockUser, mockAppContext));
  }

  @Test (timeout = 5000)
  public void testHasAccess() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.TEZ_AM_ACLS_ENABLED, true);
    ACLManager aclManager = new ACLManager("amUser", conf);

    when(mockAppContext.getAMACLManager()).thenReturn(aclManager);

    Assert.assertEquals(false, AMWebController._hasAccess(null, mockAppContext));

    UserGroupInformation mockUser = UserGroupInformation.createUserForTesting(
        "mockUser", userGroups);
    Assert.assertEquals(false, AMWebController._hasAccess(mockUser, mockAppContext));

    UserGroupInformation testUser = UserGroupInformation.createUserForTesting(
        "amUser", userGroups);
    Assert.assertEquals(true, AMWebController._hasAccess(testUser, mockAppContext));
  }


  // AM Webservice Version 2
  //ArgumentCaptor<Map<String, Object>> returnResultCaptor;
  @Captor
  ArgumentCaptor<Map<String,Object>> returnResultCaptor;

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetDagInfo() {
    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);
    DAG mockDAG = mock(DAG.class);


    doReturn(TezDAGID.fromString("dag_1422960590892_0007_42")).when(mockDAG).getID();
    doReturn(66.0f).when(mockDAG).getCompletedTaskProgress();
    doReturn(DAGState.RUNNING).when(mockDAG).getState();
    TezCounters counters = new TezCounters();
    counters.addGroup("g1", "g1");
    counters.addGroup("g2", "g2");
    counters.addGroup("g3", "g3");
    counters.addGroup("g4", "g4");
    counters.findCounter("g1", "g1_c1").setValue(100);
    counters.findCounter("g1", "g1_c2").setValue(100);
    counters.findCounter("g2", "g2_c3").setValue(100);
    counters.findCounter("g2", "g2_c4").setValue(100);
    counters.findCounter("g3", "g3_c5").setValue(100);
    counters.findCounter("g3", "g3_c6").setValue(100);

    doReturn(counters).when(mockDAG).getAllCounters();
    doReturn(counters).when(mockDAG).getCachedCounters();

    doReturn(true).when(spy).setupResponse();
    doReturn(mockDAG).when(spy).checkAndGetDAGFromRequest();
    doNothing().when(spy).renderJSON(any());

    Map<String, Set<String>> counterNames = new HashMap<String, Set<String>>();
    counterNames.put("*", null);
    doReturn(counterNames).when(spy).getCounterListFromRequest();

    spy.getDagInfo();
    verify(spy).renderJSON(returnResultCaptor.capture());

    final Map<String, Object> result = returnResultCaptor.getValue();
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("dag"));
    Map<String, String> dagInfo = (Map<String, String>) result.get("dag");

    Assert.assertEquals(4, dagInfo.size());
    Assert.assertTrue("dag_1422960590892_0007_42".equals(dagInfo.get("id")));
    Assert.assertEquals("66.0", dagInfo.get("progress"));
    Assert.assertEquals("RUNNING", dagInfo.get("status"));
    Assert.assertNotNull(dagInfo.get("counters"));

  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetVerticesInfoGetAll() {
    Vertex mockVertex1 = createMockVertex("vertex_1422960590892_0007_42_00", VertexState.RUNNING,
        0.33f, 3);
    Vertex mockVertex2 = createMockVertex("vertex_1422960590892_0007_42_01", VertexState.SUCCEEDED,
        1.0f, 5);

    final Map<String, Object> result = getVerticesTestHelper(0, mockVertex1, mockVertex2);

    Assert.assertEquals(1, result.size());

    Assert.assertTrue(result.containsKey("vertices"));
    ArrayList<Map<String, String>> verticesInfo = (ArrayList<Map<String, String>>) result.get("vertices");
    Assert.assertEquals(2, verticesInfo.size());

    Map<String, String> vertex1Result = verticesInfo.get(0);
    Map<String, String> vertex2Result = verticesInfo.get(1);

    verifySingleVertexResult(mockVertex1, vertex1Result);
    verifySingleVertexResult(mockVertex2, vertex2Result);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetVerticesInfoGetPartial() {
    Vertex mockVertex1 = createMockVertex("vertex_1422960590892_0007_42_00", VertexState.RUNNING,
        0.33f, 3);
    Vertex mockVertex2 = createMockVertex("vertex_1422960590892_0007_42_01", VertexState.SUCCEEDED,
        1.0f, 5);

    final Map<String, Object> result = getVerticesTestHelper(1, mockVertex1, mockVertex2);

    Assert.assertEquals(1, result.size());

    Assert.assertTrue(result.containsKey("vertices"));
    List<Map<String, String>> verticesInfo = (List<Map<String, String>>) result.get("vertices");
    Assert.assertEquals(1, verticesInfo.size());

    Map<String, String> vertex1Result = verticesInfo.get(0);

    verifySingleVertexResult(mockVertex1, vertex1Result);
  }

  Map<String, Object> getVerticesTestHelper(int numVerticesRequested, Vertex mockVertex1,
                                            Vertex mockVertex2) {
    DAG mockDAG = mock(DAG.class);
    doReturn(TezDAGID.fromString("dag_1422960590892_0007_42")).when(mockDAG).getID();

    TezVertexID vertexId1 = mockVertex1.getVertexId();
    doReturn(mockVertex1).when(mockDAG).getVertex(vertexId1);
    TezVertexID vertexId2 = mockVertex2.getVertexId();
    doReturn(mockVertex2).when(mockDAG).getVertex(vertexId2);

    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);

    doReturn(ImmutableMap.of(
        mockVertex1.getVertexId(), mockVertex1,
        mockVertex2.getVertexId(), mockVertex2
    )).when(mockDAG).getVertices();

    doReturn(true).when(spy).setupResponse();
    doReturn(mockDAG).when(spy).checkAndGetDAGFromRequest();

    Map<String, Set<String>> counterList = new TreeMap<String, Set<String>>();
    doReturn(counterList).when(spy).getCounterListFromRequest();

    List<Integer> requested;
    if (numVerticesRequested == 0) {
      requested = ImmutableList.of();
    } else {
      requested = ImmutableList.of(mockVertex1.getVertexId().getId());
    }

    doReturn(requested).when(spy).getVertexIDsFromRequest();
    doNothing().when(spy).renderJSON(any());

    spy.getVerticesInfo();
    verify(spy).renderJSON(returnResultCaptor.capture());

    return returnResultCaptor.getValue();
  }

  private Vertex createMockVertex(String vertexIDStr, VertexState status, float progress,
                                  int taskCounts) {
    ProgressBuilder pb = new ProgressBuilder();
    pb.setTotalTaskCount(taskCounts);
    pb.setSucceededTaskCount(taskCounts * 2);
    pb.setFailedTaskAttemptCount(taskCounts * 3);
    pb.setKilledTaskAttemptCount(taskCounts * 4);
    pb.setRunningTaskCount(taskCounts * 5);

    Vertex mockVertex = mock(Vertex.class);
    doReturn(TezVertexID.fromString(vertexIDStr)).when(mockVertex).getVertexId();
    doReturn(status).when(mockVertex).getState();
    doReturn(progress).when(mockVertex).getProgress();
    doReturn(pb).when(mockVertex).getVertexProgress();

    TezCounters counters = new TezCounters();
    counters.addGroup("g1", "g1");
    counters.addGroup("g2", "g2");
    counters.addGroup("g3", "g3");
    counters.addGroup("g4", "g4");
    counters.findCounter("g1", "g1_c1").setValue(100);
    counters.findCounter("g1", "g1_c2").setValue(100);
    counters.findCounter("g2", "g2_c3").setValue(100);
    counters.findCounter("g2", "g2_c4").setValue(100);
    counters.findCounter("g3", "g3_c5").setValue(100);
    counters.findCounter("g3", "g3_c6").setValue(100);

    doReturn(counters).when(mockVertex).getAllCounters();
    doReturn(counters).when(mockVertex).getCachedCounters();

    return mockVertex;
  }


  private void verifySingleVertexResult(Vertex mockVertex2, Map<String, String> vertex2Result) {
    ProgressBuilder progress;
    Assert.assertEquals(mockVertex2.getVertexId().toString(), vertex2Result.get("id"));
    Assert.assertEquals(mockVertex2.getState().toString(), vertex2Result.get("status"));
    Assert.assertEquals(Float.toString(mockVertex2.getCompletedTaskProgress()), vertex2Result.get("progress"));
    progress = mockVertex2.getVertexProgress();
    Assert.assertEquals(Integer.toString(progress.getTotalTaskCount()),
        vertex2Result.get("totalTasks"));
    Assert.assertEquals(Integer.toString(progress.getRunningTaskCount()),
        vertex2Result.get("runningTasks"));
    Assert.assertEquals(Integer.toString(progress.getSucceededTaskCount()),
        vertex2Result.get("succeededTasks"));
    Assert.assertEquals(Integer.toString(progress.getKilledTaskAttemptCount()),
        vertex2Result.get("killedTaskAttempts"));
    Assert.assertEquals(Integer.toString(progress.getFailedTaskAttemptCount()),
        vertex2Result.get("failedTaskAttempts"));
  }

  //-- Get Tasks Info Tests -----------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetTasksInfoWithTaskIds() {
    List <Task> tasks = createMockTasks();
    List <Integer> vertexMinIds = Arrays.asList();
    List <List <Integer>> taskMinIds = Arrays.asList(Arrays.asList(0, 0),
        Arrays.asList(0, 3),
        Arrays.asList(0, 1));

    // Fetch All
    Map<String, Object> result = getTasksTestHelper(tasks, taskMinIds, vertexMinIds,
        AMWebController.MAX_QUERIED);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("tasks"));

    ArrayList<Map<String, String>> tasksInfo = (ArrayList<Map<String, String>>) result.
        get("tasks");
    Assert.assertEquals(3, tasksInfo.size());

    verifySingleTaskResult(tasks.get(0), tasksInfo.get(0));
    verifySingleTaskResult(tasks.get(3), tasksInfo.get(1));
    verifySingleTaskResult(tasks.get(1), tasksInfo.get(2));

    // With limit
    result = getTasksTestHelper(tasks, taskMinIds, vertexMinIds, 2);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("tasks"));

    tasksInfo = (ArrayList<Map<String, String>>) result.get("tasks");
    Assert.assertEquals(2, tasksInfo.size());

    verifySingleTaskResult(tasks.get(0), tasksInfo.get(0));
    verifySingleTaskResult(tasks.get(3), tasksInfo.get(1));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetTasksInfoGracefulTaskFetch() {
    List <Task> tasks = createMockTasks();
    List <Integer> vertexMinIds = Arrays.asList();
    List <List <Integer>> taskMinIds = Arrays.asList(Arrays.asList(0, 0),
        Arrays.asList(0, 6),
        Arrays.asList(0, 1));

    // Fetch All
    Map<String, Object> result = getTasksTestHelper(tasks, taskMinIds, vertexMinIds,
        AMWebController.MAX_QUERIED);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("tasks"));

    ArrayList<Map<String, String>> tasksInfo = (ArrayList<Map<String, String>>) result.
        get("tasks");
    Assert.assertEquals(2, tasksInfo.size());

    verifySingleTaskResult(tasks.get(0), tasksInfo.get(0));
    verifySingleTaskResult(tasks.get(1), tasksInfo.get(1));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetTasksInfoWithVertexId() {
    List <Task> tasks = createMockTasks();
    List <Integer> vertexMinIds = Arrays.asList(0);
    List <List <Integer>> taskMinIds = Arrays.asList();

    Map<String, Object> result = getTasksTestHelper(tasks, taskMinIds, vertexMinIds,
        AMWebController.MAX_QUERIED);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("tasks"));

    ArrayList<Map<String, String>> tasksInfo = (ArrayList<Map<String, String>>) result.
        get("tasks");
    Assert.assertEquals(4, tasksInfo.size());

    sortMapList(tasksInfo, "id");
    verifySingleTaskResult(tasks.get(0), tasksInfo.get(0));
    verifySingleTaskResult(tasks.get(1), tasksInfo.get(1));
    verifySingleTaskResult(tasks.get(2), tasksInfo.get(2));
    verifySingleTaskResult(tasks.get(3), tasksInfo.get(3));

    // With limit
    result = getTasksTestHelper(tasks, taskMinIds, vertexMinIds, 2);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("tasks"));

    tasksInfo = (ArrayList<Map<String, String>>) result.get("tasks");
    Assert.assertEquals(2, tasksInfo.size());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetTasksInfoWithJustDAGId() {
    List <Task> tasks = createMockTasks();
    List <Integer> vertexMinIds = Arrays.asList();
    List <List <Integer>> taskMinIds = Arrays.asList();

    Map<String, Object> result = getTasksTestHelper(tasks, taskMinIds, vertexMinIds,
        AMWebController.MAX_QUERIED);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("tasks"));

    ArrayList<Map<String, String>> tasksInfo = (ArrayList<Map<String, String>>) result.
        get("tasks");
    Assert.assertEquals(4, tasksInfo.size());

    sortMapList(tasksInfo, "id");
    verifySingleTaskResult(tasks.get(0), tasksInfo.get(0));
    verifySingleTaskResult(tasks.get(1), tasksInfo.get(1));
    verifySingleTaskResult(tasks.get(2), tasksInfo.get(2));
    verifySingleTaskResult(tasks.get(3), tasksInfo.get(3));

    // With limit
    result = getTasksTestHelper(tasks, taskMinIds, vertexMinIds, 2);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("tasks"));

    tasksInfo = (ArrayList<Map<String, String>>) result.get("tasks");
    Assert.assertEquals(2, tasksInfo.size());
  }

  private void sortMapList(ArrayList<Map<String, String>> list, String propertyName) {
    class MapComparator implements Comparator<Map<String, String>> {
      private final String key;

      public MapComparator(String key) {
        this.key = key;
      }

      public int compare(Map<String, String> first, Map<String, String> second) {
        String firstValue = first.get(key);
        String secondValue = second.get(key);
        return firstValue.compareTo(secondValue);
      }
    }

    Collections.sort(list, new MapComparator(propertyName));
  }

  Map<String, Object> getTasksTestHelper(List<Task> tasks, List <List <Integer>> taskMinIds,
                                         List<Integer> vertexMinIds, Integer limit) {
    //Creating mock DAG
    DAG mockDAG = mock(DAG.class);
    doReturn(TezDAGID.fromString("dag_1441301219877_0109_1")).when(mockDAG).getID();

    //Creating mock vertex and attaching to mock DAG
    TezVertexID vertexID = TezVertexID.fromString("vertex_1441301219877_0109_1_00");
    Vertex mockVertex = mock(Vertex.class);
    doReturn(vertexID).when(mockVertex).getVertexId();

    doReturn(mockVertex).when(mockDAG).getVertex(vertexID);
    doReturn(ImmutableMap.of(
        vertexID, mockVertex
    )).when(mockDAG).getVertices();

    //Creating mock tasks and attaching to mock vertex
    Map<TezTaskID, Task> taskMap = Maps.newHashMap();
    for(Task task : tasks) {
      TezTaskID taskId = task.getTaskId();
      int taskIndex = taskId.getId();
      doReturn(task).when(mockVertex).getTask(taskIndex);
      taskMap.put(taskId, task);
    }
    doReturn(taskMap).when(mockVertex).getTasks();

    //Creates & setup controller spy
    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);
    doReturn(true).when(spy).setupResponse();
    doNothing().when(spy).renderJSON(any());

    // Set mock query params
    doReturn(limit).when(spy).getQueryParamInt(WebUIService.LIMIT);
    doReturn(vertexMinIds).when(spy).getIntegersFromRequest(WebUIService.VERTEX_ID, limit);
    doReturn(taskMinIds).when(spy).getIDsFromRequest(WebUIService.TASK_ID, limit, 2);

    // Set function mocks
    doReturn(mockDAG).when(spy).checkAndGetDAGFromRequest();

    Map<String, Set<String>> counterList = new TreeMap<String, Set<String>>();
    doReturn(counterList).when(spy).getCounterListFromRequest();

    spy.getTasksInfo();
    verify(spy).renderJSON(returnResultCaptor.capture());

    return returnResultCaptor.getValue();
  }

  private List<Task> createMockTasks() {
    Task mockTask1 = createMockTask("task_1441301219877_0109_1_00_000000", TaskState.RUNNING,
        0.33f);
    Task mockTask2 = createMockTask("task_1441301219877_0109_1_00_000001", TaskState.SUCCEEDED,
        1.0f);
    Task mockTask3 = createMockTask("task_1441301219877_0109_1_00_000002", TaskState.SUCCEEDED,
        .8f);
    Task mockTask4 = createMockTask("task_1441301219877_0109_1_00_000003", TaskState.SUCCEEDED,
        .8f);

    List <Task> tasks = Arrays.asList(mockTask1, mockTask2, mockTask3, mockTask4);
    return tasks;
  }

  private Task createMockTask(String taskIDStr, TaskState status, float progress) {
    Task mockTask = mock(Task.class);

    doReturn(TezTaskID.fromString(taskIDStr)).when(mockTask).getTaskId();
    doReturn(status).when(mockTask).getState();
    doReturn(progress).when(mockTask).getProgress();

    TezCounters counters = new TezCounters();
    counters.addGroup("g1", "g1");
    counters.addGroup("g2", "g2");
    counters.addGroup("g3", "g3");
    counters.addGroup("g4", "g4");
    counters.findCounter("g1", "g1_c1").setValue(101);
    counters.findCounter("g1", "g1_c2").setValue(102);
    counters.findCounter("g2", "g2_c3").setValue(103);
    counters.findCounter("g2", "g2_c4").setValue(104);
    counters.findCounter("g3", "g3_c5").setValue(105);
    counters.findCounter("g3", "g3_c6").setValue(106);

    doReturn(counters).when(mockTask).getCounters();

    return mockTask;
  }

  private void verifySingleTaskResult(Task mockTask, Map<String, String> taskResult) {
    Assert.assertEquals(3, taskResult.size());
    Assert.assertEquals(mockTask.getTaskId().toString(), taskResult.get("id"));
    Assert.assertEquals(mockTask.getState().toString(), taskResult.get("status"));
    Assert.assertEquals(Float.toString(mockTask.getProgress()), taskResult.get("progress"));
  }

  //-- Get Attempts Info Tests -----------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGetAttemptsInfoWithIds() {
    List <TaskAttempt> attempts = createMockAttempts();
    List <Integer> vertexMinIds = Arrays.asList();
    List <Integer> taskMinIds = Arrays.asList();
    List <List <Integer>> attemptMinIds = Arrays.asList(Arrays.asList(0, 0, 0),
        Arrays.asList(0, 0, 1),
        Arrays.asList(0, 0, 2),
        Arrays.asList(0, 0, 3));

    // Fetch All
    Map<String, Object> result = getAttemptsTestHelper(attempts, attemptMinIds, vertexMinIds,
        taskMinIds, AMWebController.MAX_QUERIED);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("attempts"));

    ArrayList<Map<String, String>> attemptsInfo = (ArrayList<Map<String, String>>) result.
        get("attempts");
    Assert.assertEquals(4, attemptsInfo.size());

    verifySingleAttemptResult(attempts.get(0), attemptsInfo.get(0));
    verifySingleAttemptResult(attempts.get(1), attemptsInfo.get(1));
    verifySingleAttemptResult(attempts.get(2), attemptsInfo.get(2));
    verifySingleAttemptResult(attempts.get(3), attemptsInfo.get(3));

    // With limit
    result = getAttemptsTestHelper(attempts, attemptMinIds, vertexMinIds, taskMinIds, 2);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("attempts"));

    attemptsInfo = (ArrayList<Map<String, String>>) result.get("attempts");
    Assert.assertEquals(2, attemptsInfo.size());

    verifySingleAttemptResult(attempts.get(0), attemptsInfo.get(0));
    verifySingleAttemptResult(attempts.get(1), attemptsInfo.get(1));
  }

  Map<String, Object> getAttemptsTestHelper(List<TaskAttempt> attempts, List <List <Integer>> attemptMinIds,
                                         List<Integer> vertexMinIds, List<Integer> taskMinIds, Integer limit) {
    //Creating mock DAG
    DAG mockDAG = mock(DAG.class);
    doReturn(TezDAGID.fromString("dag_1441301219877_0109_1")).when(mockDAG).getID();

    //Creating mock vertex and attaching to mock DAG
    TezVertexID vertexID = TezVertexID.fromString("vertex_1441301219877_0109_1_00");
    Vertex mockVertex = mock(Vertex.class);
    doReturn(vertexID).when(mockVertex).getVertexId();

    doReturn(mockVertex).when(mockDAG).getVertex(vertexID);
    doReturn(ImmutableMap.of(
        vertexID, mockVertex
    )).when(mockDAG).getVertices();

    //Creating mock task and attaching to mock Vertex
    TezTaskID taskID = TezTaskID.fromString("task_1441301219877_0109_1_00_000000");
    Task mockTask = mock(Task.class);
    doReturn(taskID).when(mockTask).getTaskId();
    int taskIndex = taskID.getId();
    doReturn(mockTask).when(mockVertex).getTask(taskIndex);
    doReturn(ImmutableMap.of(
        taskID, mockTask
    )).when(mockVertex).getTasks();

    //Creating mock tasks and attaching to mock vertex
    Map<TezTaskAttemptID, TaskAttempt> attemptsMap = Maps.newHashMap();
    for(TaskAttempt attempt : attempts) {
      TezTaskAttemptID attemptId = attempt.getID();
      doReturn(attempt).when(mockTask).getAttempt(attemptId);
      attemptsMap.put(attemptId, attempt);
    }
    doReturn(attemptsMap).when(mockTask).getAttempts();

    //Creates & setup controller spy
    AMWebController amWebController = new AMWebController(mockRequestContext, mockAppContext,
        "TEST_HISTORY_URL");
    AMWebController spy = spy(amWebController);
    doReturn(true).when(spy).setupResponse();
    doNothing().when(spy).renderJSON(any());

    // Set mock query params
    doReturn(limit).when(spy).getQueryParamInt(WebUIService.LIMIT);
    doReturn(vertexMinIds).when(spy).getIntegersFromRequest(WebUIService.VERTEX_ID, limit);
    doReturn(taskMinIds).when(spy).getIDsFromRequest(WebUIService.TASK_ID, limit, 2);
    doReturn(attemptMinIds).when(spy).getIDsFromRequest(WebUIService.ATTEMPT_ID, limit, 3);

    // Set function mocks
    doReturn(mockDAG).when(spy).checkAndGetDAGFromRequest();

    Map<String, Set<String>> counterList = new TreeMap<String, Set<String>>();
    doReturn(counterList).when(spy).getCounterListFromRequest();

    spy.getAttemptsInfo();
    verify(spy).renderJSON(returnResultCaptor.capture());

    return returnResultCaptor.getValue();
  }

  private List<TaskAttempt> createMockAttempts() {
    TaskAttempt mockAttempt1 = createMockAttempt("attempt_1441301219877_0109_1_00_000000_0", TaskAttemptState.RUNNING,
        0.33f);
    TaskAttempt mockAttempt2 = createMockAttempt("attempt_1441301219877_0109_1_00_000000_1", TaskAttemptState.SUCCEEDED,
        1.0f);
    TaskAttempt mockAttempt3 = createMockAttempt("attempt_1441301219877_0109_1_00_000000_2", TaskAttemptState.FAILED,
        .8f);
    TaskAttempt mockAttempt4 = createMockAttempt("attempt_1441301219877_0109_1_00_000000_3", TaskAttemptState.SUCCEEDED,
        .8f);

    List <TaskAttempt> attempts = Arrays.asList(mockAttempt1, mockAttempt2, mockAttempt3, mockAttempt4);
    return attempts;
  }

  private TaskAttempt createMockAttempt(String attemptIDStr, TaskAttemptState status, float progress) {
    TaskAttempt mockAttempt = mock(TaskAttempt.class);

    doReturn(TezTaskAttemptID.fromString(attemptIDStr)).when(mockAttempt).getID();
    doReturn(status).when(mockAttempt).getState();
    doReturn(progress).when(mockAttempt).getProgress();

    TezCounters counters = new TezCounters();
    counters.addGroup("g1", "g1");
    counters.addGroup("g2", "g2");
    counters.addGroup("g3", "g3");
    counters.addGroup("g4", "g4");
    counters.findCounter("g1", "g1_c1").setValue(101);
    counters.findCounter("g1", "g1_c2").setValue(102);
    counters.findCounter("g2", "g2_c3").setValue(103);
    counters.findCounter("g2", "g2_c4").setValue(104);
    counters.findCounter("g3", "g3_c5").setValue(105);
    counters.findCounter("g3", "g3_c6").setValue(106);

    doReturn(counters).when(mockAttempt).getCounters();

    return mockAttempt;
  }

  private void verifySingleAttemptResult(TaskAttempt mockTask, Map<String, String> taskResult) {
    Assert.assertEquals(3, taskResult.size());
    Assert.assertEquals(mockTask.getID().toString(), taskResult.get("id"));
    Assert.assertEquals(mockTask.getState().toString(), taskResult.get("status"));
    Assert.assertEquals(Float.toString(mockTask.getProgress()), taskResult.get("progress"));
  }

}
