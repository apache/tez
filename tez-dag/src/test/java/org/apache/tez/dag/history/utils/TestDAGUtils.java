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
package org.apache.tez.dag.history.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.CallerContext;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.OutputCommitter;

import org.codehaus.jettison.json.JSONException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestDAGUtils {

  @SuppressWarnings("deprecation")
  private DAGPlan createDAG(String dagName) {
    // Create a plan with 3 vertices: A, B, C. Group(A,B)->C
    Configuration conf = new Configuration(false);
    int dummyTaskCount = 1;
    Resource dummyTaskResource = Resource.newInstance(1, 1);
    org.apache.tez.dag.api.Vertex v1 = Vertex.create("vertex1",
        ProcessorDescriptor.create("Processor").setHistoryText("vertex1 Processor HistoryText"),
        dummyTaskCount, dummyTaskResource);
    v1.addDataSource("input1", DataSourceDescriptor.create(InputDescriptor.create(
        "input.class").setHistoryText("input HistoryText"), null, null));
    org.apache.tez.dag.api.Vertex v2 = Vertex.create("vertex2",
        ProcessorDescriptor.create("Processor").setHistoryText("vertex2 Processor HistoryText"),
        dummyTaskCount, dummyTaskResource);
    org.apache.tez.dag.api.Vertex v3 = Vertex.create("vertex3",
        ProcessorDescriptor.create("Processor").setHistoryText("vertex3 Processor HistoryText"),
        dummyTaskCount, dummyTaskResource);

    DAG dag = DAG.create("DAG-" + dagName);
    dag.setCallerContext(CallerContext.create("context1", "callerId1", "callerType1", "desc1"));
    dag.setDAGInfo("dagInfo");
    String groupName1 = "uv12";
    org.apache.tez.dag.api.VertexGroup uv12 = dag.createVertexGroup(groupName1, v1, v2);
    OutputDescriptor outDesc = OutputDescriptor.create("output.class")
        .setHistoryText("uvOut HistoryText");
    OutputCommitterDescriptor ocd =
        OutputCommitterDescriptor.create(OutputCommitter.class.getName());
    uv12.addDataSink("uvOut", DataSinkDescriptor.create(outDesc, ocd, null));
    v3.addDataSink("uvOut", DataSinkDescriptor.create(outDesc, ocd, null));

    GroupInputEdge e1 = GroupInputEdge.create(uv12, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class").setHistoryText("Dummy History Text"),
            InputDescriptor.create("dummy input class").setHistoryText("Dummy History Text")),
        InputDescriptor.create("merge.class").setHistoryText("Merge HistoryText"));

    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    return dag.createDag(conf, null, null, null, true);
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  @SuppressWarnings("unchecked")
  public void testConvertDAGPlanToATSMap() throws IOException, JSONException {
    DAGPlan dagPlan = createDAG("testConvertDAGPlanToATSMap");
    Map<String,TezVertexID> idNameMap = new HashMap<String, TezVertexID>();
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId1 = TezVertexID.getInstance(dagId, 1);
    TezVertexID vId2 = TezVertexID.getInstance(dagId, 2);
    TezVertexID vId3 = TezVertexID.getInstance(dagId, 3);
    idNameMap.put("vertex1", vId1);
    idNameMap.put("vertex2", vId2);
    idNameMap.put("vertex3", vId3);

    Map<String, Object> atsMap = DAGUtils.convertDAGPlanToATSMap(dagPlan);
    assertTrue(atsMap.containsKey(DAGUtils.DAG_NAME_KEY));
    assertEquals("DAG-testConvertDAGPlanToATSMap",
        atsMap.get(DAGUtils.DAG_NAME_KEY));
    assertTrue(atsMap.containsKey(DAGUtils.DAG_INFO_KEY));
    assertTrue(atsMap.containsKey(DAGUtils.DAG_CONTEXT_KEY));
    Map<String, String> contextMap = (Map<String, String>)atsMap.get(DAGUtils.DAG_CONTEXT_KEY);
    assertEquals("context1", contextMap.get(ATSConstants.CONTEXT));
    assertEquals("callerId1", contextMap.get(ATSConstants.CALLER_ID));
    assertEquals("callerType1", contextMap.get(ATSConstants.CALLER_TYPE));
    assertEquals("desc1", contextMap.get(ATSConstants.DESCRIPTION));

    assertEquals("dagInfo", atsMap.get(DAGUtils.DAG_INFO_KEY));
    assertEquals(dagPlan.getName(), atsMap.get(DAGUtils.DAG_NAME_KEY));
    assertTrue(atsMap.containsKey("version"));
    assertEquals(2, atsMap.get("version"));
    assertTrue(atsMap.containsKey(DAGUtils.VERTICES_KEY));
    assertTrue(atsMap.containsKey(DAGUtils.EDGES_KEY));
    assertTrue(atsMap.containsKey(DAGUtils.VERTEX_GROUPS_KEY));

    assertEquals(3, ((Collection<?>) atsMap.get(DAGUtils.VERTICES_KEY)).size());

    Set<String> inEdgeIds = new HashSet<String>();
    Set<String> outEdgeIds = new HashSet<String>();

    int additionalInputCount = 0;
    int additionalOutputCount = 0;

    for (Object o : ((Collection<?>) atsMap.get(DAGUtils.VERTICES_KEY))) {
      Map<String, Object> v = (Map<String, Object>) o;
      assertTrue(v.containsKey(DAGUtils.VERTEX_NAME_KEY));
      String vName = (String)v.get(DAGUtils.VERTEX_NAME_KEY);
      assertTrue(v.containsKey(DAGUtils.PROCESSOR_CLASS_KEY));
      assertTrue(v.containsKey(DAGUtils.USER_PAYLOAD_AS_TEXT));

      if (v.containsKey(DAGUtils.IN_EDGE_IDS_KEY)) {
        inEdgeIds.addAll(((Collection<String>) v.get(DAGUtils.IN_EDGE_IDS_KEY)));
      }
      if (v.containsKey(DAGUtils.OUT_EDGE_IDS_KEY)) {
        outEdgeIds.addAll(((Collection<String>) v.get(DAGUtils.OUT_EDGE_IDS_KEY)));
      }

      assertTrue(idNameMap.containsKey(vName));
      String procPayload = vName + " Processor HistoryText";
      assertEquals(procPayload, v.get(DAGUtils.USER_PAYLOAD_AS_TEXT));

      if (v.containsKey(DAGUtils.ADDITIONAL_INPUTS_KEY)) {
        additionalInputCount += ((Collection<?>) v.get(DAGUtils.ADDITIONAL_INPUTS_KEY)).size();
        for (Object input : ((Collection<?>) v.get(DAGUtils.ADDITIONAL_INPUTS_KEY))) {
          Map<String, Object> inputMap = (Map<String, Object>) input;
          assertTrue(inputMap.containsKey(DAGUtils.NAME_KEY));
          assertTrue(inputMap.containsKey(DAGUtils.CLASS_KEY));
          assertFalse(inputMap.containsKey(DAGUtils.INITIALIZER_KEY));
          assertEquals("input HistoryText", inputMap.get(DAGUtils.USER_PAYLOAD_AS_TEXT));
        }
      }

      if (v.containsKey(DAGUtils.ADDITIONAL_OUTPUTS_KEY)) {
        additionalOutputCount += ((Collection<?>) v.get(DAGUtils.ADDITIONAL_OUTPUTS_KEY)).size();
        for (Object output : ((Collection<?>) v.get(DAGUtils.ADDITIONAL_OUTPUTS_KEY))) {
          Map<String, Object> outputMap = (Map<String, Object>) output;
          assertTrue(outputMap.containsKey(DAGUtils.NAME_KEY));
          assertTrue(outputMap.containsKey(DAGUtils.CLASS_KEY));
          assertTrue(outputMap.containsKey(DAGUtils.INITIALIZER_KEY));
          assertEquals("uvOut HistoryText", outputMap.get(DAGUtils.USER_PAYLOAD_AS_TEXT));
        }
      }
    }

    // 1 input
    assertEquals(1, additionalInputCount);
    // 3 outputs due to vertex group
    assertEquals(3, additionalOutputCount);

    // 1 edge translates to 2 due to vertex group
    assertEquals(2, inEdgeIds.size());
    assertEquals(2, outEdgeIds.size());

    for (Object o : ((Collection<?>) atsMap.get(DAGUtils.EDGES_KEY))) {
      Map<String, Object> e = (Map<String, Object>) o;

      assertTrue(inEdgeIds.contains(e.get(DAGUtils.EDGE_ID_KEY)));
      assertTrue(outEdgeIds.contains(e.get(DAGUtils.EDGE_ID_KEY)));
      assertTrue(e.containsKey(DAGUtils.INPUT_VERTEX_NAME_KEY));
      assertTrue(e.containsKey(DAGUtils.OUTPUT_VERTEX_NAME_KEY));
      assertEquals(DataMovementType.SCATTER_GATHER.name(),
          e.get(DAGUtils.DATA_MOVEMENT_TYPE_KEY));
      assertEquals(DataSourceType.PERSISTED.name(), e.get(DAGUtils.DATA_SOURCE_TYPE_KEY));
      assertEquals(SchedulingType.SEQUENTIAL.name(), e.get(DAGUtils.SCHEDULING_TYPE_KEY));
      assertEquals("dummy output class", e.get(DAGUtils.EDGE_SOURCE_CLASS_KEY));
      assertEquals("dummy input class", e.get(DAGUtils.EDGE_DESTINATION_CLASS_KEY));
      assertEquals("Dummy History Text", e.get(DAGUtils.OUTPUT_USER_PAYLOAD_AS_TEXT));
      assertEquals("Dummy History Text", e.get(DAGUtils.INPUT_USER_PAYLOAD_AS_TEXT));
    }

    for (Object o : ((Collection<?>) atsMap.get(DAGUtils.VERTEX_GROUPS_KEY))) {
      Map<String, Object> e = (Map<String, Object>) o;
      assertEquals("uv12", e.get(DAGUtils.VERTEX_GROUP_NAME_KEY));
      assertTrue(e.containsKey(DAGUtils.VERTEX_GROUP_MEMBERS_KEY));
      assertTrue(e.containsKey(DAGUtils.VERTEX_GROUP_OUTPUTS_KEY));
      assertTrue(e.containsKey(DAGUtils.VERTEX_GROUP_EDGE_MERGED_INPUTS_KEY));
    }
  }

}
