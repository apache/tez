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

package org.apache.tez.dag.api.client;

import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestATSHttpClient {

  @Before
  public void setup() {
    // Disable tests if hadoop version is less than 2.4.0
    // as Timeline is not supported in 2.2.x or 2.3.x
    // If enabled with the lower versions, tests fail due to incompatible use of an API
    // YarnConfiguration::useHttps which only exists in versions 2.4 and higher
    String hadoopVersion = System.getProperty("tez.hadoop.version");
    Assume.assumeFalse(hadoopVersion.startsWith("2.2.") || hadoopVersion.startsWith("2.3."));
  }

  @Test(timeout = 5000)
  public void testGetDagStatusThrowsExceptionOnEmptyJson() throws TezException {
    ApplicationId mockAppId = mock(ApplicationId.class);
    DAGClientTimelineImpl httpClient = new DAGClientTimelineImpl(mockAppId, "EXAMPLE_DAG_ID",
        new TezConfiguration(), null);
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

    Assert.assertTrue("Expected TezException but did not happen", exceptionHappened);
    verify(spyClient).getJsonRootEntity(expectedDagUrl);
  }

  @Test(timeout = 5000)
  public void testGetDagStatusSimple() throws TezException, JSONException, IOException {
    DAGClientTimelineImpl
        httpClient = new DAGClientTimelineImpl(mock(ApplicationId.class),"EXAMPLE_DAG_ID",
        new TezConfiguration(), null);
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
            "  otherinfo: { " +
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
        "{otherinfo: {vertexName:'v1', numTasks:5,numFailedTasks:1,numSucceededTasks:2," +
          "numKilledTasks:3,numCompletedTasks:3}}," +
        "{otherinfo: {vertexName:'v2',numTasks:10,numFailedTasks:1,numSucceededTasks:5," +
          "numKilledTasks:3,numCompletedTasks:4}}" +
        "]}";

    doReturn(new JSONObject(jsonDagData)).when(spyClient).getJsonRootEntity(expectedDagUrl);
    doReturn(new JSONObject(jsonVertexData)).when(spyClient).getJsonRootEntity(expectedVertexUrl);

    DAGStatus dagStatus = spyClient.getDAGStatus(statusOptions);

    Assert.assertEquals("DAG State", DAGStatus.State.SUCCEEDED, dagStatus.getState());
    Assert.assertEquals("DAG Diagnostics size", 1, dagStatus.getDiagnostics().size());
    Assert.assertEquals("DAG diagnostics detail", "SAMPLE_DIAGNOSTICS",
        dagStatus.getDiagnostics().get(0));
    Assert.assertEquals("Counters Size", 2, dagStatus.getDAGCounters().countCounters());
    Assert.assertEquals("Counter Value", 1,
        dagStatus.getDAGCounters().getGroup("CG1").findCounter("C1").getValue());
    Assert.assertEquals("total tasks", 15, dagStatus.getDAGProgress().getTotalTaskCount());
    Assert.assertEquals("failed tasks", 2, dagStatus.getDAGProgress().getFailedTaskCount());
    Assert.assertEquals("killed tasks", 6, dagStatus.getDAGProgress().getKilledTaskCount());
    Assert.assertEquals("succeeded tasks", 7, dagStatus.getDAGProgress().getSucceededTaskCount());
    Assert.assertEquals("running tasks", 8, dagStatus.getDAGProgress().getRunningTaskCount());
    final Map<String, Progress> vertexProgress = dagStatus.getVertexProgress();
    Assert.assertEquals("vertex progress count", 2, vertexProgress.size());
    Assert.assertTrue("vertex name1", vertexProgress.containsKey("v1"));
    Assert.assertTrue("vertex name2", vertexProgress.containsKey("v2"));
  }

  @Test(timeout = 5000)
  public void testGetVertexStatusSimple() throws JSONException, TezException, IOException {
    DAGClientTimelineImpl
        httpClient = new DAGClientTimelineImpl(mock(ApplicationId.class), "EXAMPLE_DAG_ID",
        new TezConfiguration(), null);
    DAGClientTimelineImpl spyClient = spy(httpClient);
    spyClient.baseUri = "http://yarn.ats.webapp/ws/v1/timeline";
    final String expectedVertexUrl = "http://yarn.ats.webapp/ws/v1/timeline/TEZ_VERTEX_ID" +
        "?primaryFilter=TEZ_DAG_ID:EXAMPLE_DAG_ID&secondaryFilter=vertexName:vertex1name&" +
        "fields=primaryfilters,otherinfo";

    Set<StatusGetOpts> statusOptions = new HashSet<StatusGetOpts>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);

    final String jsonData = "{entities:[ {otherinfo:{numFailedTasks:1,numSucceededTasks:2," +
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
    Assert.assertEquals("status check", VertexStatus.State.SUCCEEDED, vertexStatus.getState());
    Assert.assertEquals("diagnostics", "diagnostics1", vertexStatus.getDiagnostics().get(0));
    final Progress progress = vertexStatus.getProgress();
    final TezCounters vertexCounters = vertexStatus.getVertexCounters();
    Assert.assertEquals("failed task count", 1, progress.getFailedTaskCount());
    Assert.assertEquals("suceeded task count", 2, progress.getSucceededTaskCount());
    Assert.assertEquals("killed task count", 3, progress.getKilledTaskCount());
    Assert.assertEquals("total task count", 4, progress.getTotalTaskCount());
    Assert.assertEquals("Counters Size", 2, vertexCounters.countCounters());
    Assert.assertEquals("Counter Value", 1,
        vertexCounters.getGroup("CG1").findCounter("C1").getValue());
  }
}
