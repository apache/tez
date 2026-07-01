/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.dag.api.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGStatus.State;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestATSHttpClient {

  @BeforeAll
  public static void setup() {
    // Disable tests if hadoop version is less than 2.4.0
    // as Timeline is not supported in 2.2.x or 2.3.x
    // If enabled with the lower versions, tests fail due to incompatible use of an API
    // YarnConfiguration::useHttps which only exists in versions 2.4 and higher
    String hadoopVersion = System.getProperty("tez.hadoop.version");
    assumeFalse(hadoopVersion.startsWith("2.2.") || hadoopVersion.startsWith("2.3."));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testGetDagStatusThrowsExceptionOnEmptyJson() throws TezException {
    ApplicationId mockAppId = mock(ApplicationId.class);
    DAGClientTimelineImpl httpClient = new DAGClientTimelineImpl(mockAppId, "EXAMPLE_DAG_ID",
        new TezConfiguration(), null, 0);
    DAGClientTimelineImpl spyClient = spy(httpClient);
    spyClient.baseUri = "http://yarn.ats.webapp/ws/v1/timeline";
    final String expectedDagUrl = "http://yarn.ats.webapp/ws/v1/timeline/TEZ_DAG_ID/EXAMPLE_DAG_ID" +
        "?fields=primaryfilters,otherinfo";

    doReturn(new JSONObject()).when(spyClient).getJsonRootEntity(expectedDagUrl);
    boolean exceptionHappened = false;
    try {
      spyClient.getDAGStatus(null);
    } catch (TezException e) {
      exceptionHappened = true;
    } catch (IOException e) {
      fail("should not come here");
    }

    assertTrue(exceptionHappened, "Expected TezException but did not happen");
    verify(spyClient).getJsonRootEntity(expectedDagUrl);
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testGetDagStatusSimple() throws TezException, JSONException, IOException {
    DAGClientTimelineImpl
        httpClient = new DAGClientTimelineImpl(mock(ApplicationId.class),"EXAMPLE_DAG_ID",
        new TezConfiguration(), null, 0);
    DAGClientTimelineImpl spyClient = spy(httpClient);
    spyClient.baseUri = "http://yarn.ats.webapp/ws/v1/timeline";
    final String expectedDagUrl = "http://yarn.ats.webapp/ws/v1/timeline/TEZ_DAG_ID/EXAMPLE_DAG_ID" +
        "?fields=primaryfilters,otherinfo";
    final String expectedVertexUrl = "http://yarn.ats.webapp/ws/v1/timeline/TEZ_VERTEX_ID" +
        "?primaryFilter=TEZ_DAG_ID:EXAMPLE_DAG_ID&fields=primaryfilters,otherinfo";

    Set<StatusGetOpts> statusOptions = new HashSet<StatusGetOpts>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);


    final String jsonDagData =
            "{ " +
            "  otherInfo: { " +
            "    status: 'SUCCEEDED'," +
            "    diagnostics: 'SAMPLE_DIAGNOSTICS'," +
            "    counters: { counterGroups: [ " +
            "      { counterGroupName: 'CG1', counterGroupDisplayName: 'CGD1', counters: [" +
            "        {counterName:'C1', counterDisplayName: 'CD1', counterValue: 1 }," +
            "        {counterName:'C2', counterDisplayName: 'CD2', counterValue: 2 }" +
            "      ]}" +
            "    ]}" +
            "  }" +
            "}";

    final String jsonVertexData = "{entities:[ " +
        "{otherInfo: {vertexName:'v1', numTasks:5,numFailedTasks:1,numSucceededTasks:2," +
          "numKilledTasks:3,numCompletedTasks:3}}," +
        "{otherInfo: {vertexName:'v2',numTasks:10,numFailedTasks:1,numSucceededTasks:5," +
          "numKilledTasks:3,numCompletedTasks:4}}" +
        "]}";

    doReturn(new JSONObject(jsonDagData)).when(spyClient).getJsonRootEntity(expectedDagUrl);
    doReturn(new JSONObject(jsonVertexData)).when(spyClient).getJsonRootEntity(expectedVertexUrl);

    DAGStatus dagStatus = spyClient.getDAGStatus(statusOptions);

    assertEquals(State.SUCCEEDED, dagStatus.getState(), "DAG State");
    assertEquals(1, dagStatus.getDiagnostics().size(), "DAG Diagnostics size");
    assertEquals("SAMPLE_DIAGNOSTICS", dagStatus.getDiagnostics().get(0), "DAG diagnostics detail");
    assertEquals(2, dagStatus.getDAGCounters().countCounters(), "Counters Size");
    assertEquals(1, dagStatus.getDAGCounters().getGroup("CG1").findCounter("C1").getValue(), "Counter Value");
    assertEquals(15, dagStatus.getDAGProgress().getTotalTaskCount(), "total tasks");
    assertEquals(2, dagStatus.getDAGProgress().getFailedTaskCount(), "failed tasks");
    assertEquals(6, dagStatus.getDAGProgress().getKilledTaskCount(), "killed tasks");
    assertEquals(7, dagStatus.getDAGProgress().getSucceededTaskCount(), "succeeded tasks");
    assertEquals(8, dagStatus.getDAGProgress().getRunningTaskCount(), "running tasks");
    final Map<String, Progress> vertexProgress = dagStatus.getVertexProgress();
    assertEquals(2, vertexProgress.size(), "vertex progress count");
    assertTrue(vertexProgress.containsKey("v1"), "vertex name1");
    assertTrue(vertexProgress.containsKey("v2"), "vertex name2");
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testGetVertexStatusSimple() throws JSONException, TezException, IOException {
    DAGClientTimelineImpl
        httpClient = new DAGClientTimelineImpl(mock(ApplicationId.class), "EXAMPLE_DAG_ID",
        new TezConfiguration(), null, 0);
    DAGClientTimelineImpl spyClient = spy(httpClient);
    spyClient.baseUri = "http://yarn.ats.webapp/ws/v1/timeline";
    final String expectedVertexUrl = "http://yarn.ats.webapp/ws/v1/timeline/TEZ_VERTEX_ID" +
        "?primaryFilter=TEZ_DAG_ID:EXAMPLE_DAG_ID&secondaryFilter=vertexName:vertex1name&" +
        "fields=primaryfilters,otherinfo";

    Set<StatusGetOpts> statusOptions = new HashSet<StatusGetOpts>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);

    final String jsonData = "{entities:[ {otherInfo:{numFailedTasks:1,numSucceededTasks:2," +
        "status:'SUCCEEDED', vertexName:'vertex1name', numTasks:4, numKilledTasks: 3, " +
        "numCompletedTasks: 4, diagnostics: 'diagnostics1', " +
        "counters: { counterGroups: [ " +
        "      { counterGroupName: 'CG1', counterGroupDisplayName: 'CGD1', counters: [" +
        "        {counterName:'C1', counterDisplayName: 'CD1', counterValue: 1 }," +
        "        {counterName:'C2', counterDisplayName: 'CD2', counterValue: 2 }" +
        "      ]}" +
        "    ]}" +
        "}}]}";

    doReturn(new JSONObject(jsonData)).when(spyClient).getJsonRootEntity(expectedVertexUrl);

    VertexStatus vertexStatus = spyClient.getVertexStatus("vertex1name", statusOptions);
    assertEquals(VertexStatus.State.SUCCEEDED, vertexStatus.getState(), "status check");
    assertEquals("diagnostics1", vertexStatus.getDiagnostics().get(0), "diagnostics");
    final Progress progress = vertexStatus.getProgress();
    final TezCounters vertexCounters = vertexStatus.getVertexCounters();
    assertEquals(1, progress.getFailedTaskCount(), "failed task count");
    assertEquals(2, progress.getSucceededTaskCount(), "suceeded task count");
    assertEquals(3, progress.getKilledTaskCount(), "killed task count");
    assertEquals(4, progress.getTotalTaskCount(), "total task count");
    assertEquals(2, vertexCounters.countCounters(), "Counters Size");
    assertEquals(1, vertexCounters.getGroup("CG1").findCounter("C1").getValue(), "Counter Value");
  }
}
