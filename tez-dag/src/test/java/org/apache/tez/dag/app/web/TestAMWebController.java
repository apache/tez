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
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.records.TezDAGID;
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
    doReturn(66.0f).when(mockDAG).getProgress();
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
    doReturn(66.0f).when(mockVertex).getProgress();
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
    doReturn(66.0f).when(mockDAG).getProgress();
    doReturn(DAGState.RUNNING).when(mockDAG).getState();

    doReturn(true).when(spy).setupResponse();
    doReturn(mockDAG).when(spy).checkAndGetDAGFromRequest();
    doNothing().when(spy).renderJSON(any());

    spy.getDagInfo();
    verify(spy).renderJSON(returnResultCaptor.capture());

    final Map<String, Object> result = returnResultCaptor.getValue();
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey("dag"));
    Map<String, String> dagInfo = (Map<String, String>) result.get("dag");

    Assert.assertEquals(3, dagInfo.size());
    Assert.assertTrue("dag_1422960590892_0007_42".equals(dagInfo.get("id")));
    Assert.assertEquals("66.0", dagInfo.get("progress"));
    Assert.assertEquals("RUNNING", dagInfo.get("status"));
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

    return mockVertex;
  }


  private void verifySingleVertexResult(Vertex mockVertex2, Map<String, String> vertex2Result) {
    ProgressBuilder progress;
    Assert.assertEquals(mockVertex2.getVertexId().toString(), vertex2Result.get("id"));
    Assert.assertEquals(mockVertex2.getState().toString(), vertex2Result.get("status"));
    Assert.assertEquals(Float.toString(mockVertex2.getProgress()), vertex2Result.get("progress"));
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

}
