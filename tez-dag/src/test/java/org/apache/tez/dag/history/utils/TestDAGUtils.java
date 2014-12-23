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

package org.apache.tez.dag.history.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
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
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestDAGUtils {

  private DAGPlan createDAG() {
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

    DAG dag = DAG.create("testDag");
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

  @Test(timeout = 5000)
  @SuppressWarnings("unchecked")
  public void testConvertDAGPlanToATSMap() throws IOException, JSONException {
    DAGPlan dagPlan = createDAG();
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
    Assert.assertTrue(atsMap.containsKey(DAGUtils.DAG_NAME_KEY));
    Assert.assertEquals("testDag", atsMap.get(DAGUtils.DAG_NAME_KEY));
    Assert.assertTrue(atsMap.containsKey(DAGUtils.DAG_INFO_KEY));
    Assert.assertEquals("dagInfo", atsMap.get(DAGUtils.DAG_INFO_KEY));
    Assert.assertEquals(dagPlan.getName(), atsMap.get(DAGUtils.DAG_NAME_KEY));
    Assert.assertTrue(atsMap.containsKey("version"));
    Assert.assertEquals(1, atsMap.get("version"));
    Assert.assertTrue(atsMap.containsKey(DAGUtils.VERTICES_KEY));
    Assert.assertTrue(atsMap.containsKey(DAGUtils.EDGES_KEY));
    Assert.assertTrue(atsMap.containsKey(DAGUtils.VERTEX_GROUPS_KEY));

    Assert.assertEquals(3, ((Collection<?>) atsMap.get(DAGUtils.VERTICES_KEY)).size());

    Set<String> inEdgeIds = new HashSet<String>();
    Set<String> outEdgeIds = new HashSet<String>();

    int additionalInputCount = 0;
    int additionalOutputCount = 0;

    for (Object o : ((Collection<?>) atsMap.get(DAGUtils.VERTICES_KEY))) {
      Map<String, Object> v = (Map<String, Object>) o;
      Assert.assertTrue(v.containsKey(DAGUtils.VERTEX_NAME_KEY));
      String vName = (String)v.get(DAGUtils.VERTEX_NAME_KEY);
      Assert.assertTrue(v.containsKey(DAGUtils.PROCESSOR_CLASS_KEY));
      Assert.assertTrue(v.containsKey(DAGUtils.USER_PAYLOAD_AS_TEXT));

      if (v.containsKey(DAGUtils.IN_EDGE_IDS_KEY)) {
        inEdgeIds.addAll(((Collection<String>) v.get(DAGUtils.IN_EDGE_IDS_KEY)));
      }
      if (v.containsKey(DAGUtils.OUT_EDGE_IDS_KEY)) {
        outEdgeIds.addAll(((Collection<String>) v.get(DAGUtils.OUT_EDGE_IDS_KEY)));
      }

      Assert.assertTrue(idNameMap.containsKey(vName));
      String procPayload = vName + " Processor HistoryText";
      Assert.assertEquals(procPayload, v.get(DAGUtils.USER_PAYLOAD_AS_TEXT));

      if (v.containsKey(DAGUtils.ADDITIONAL_INPUTS_KEY)) {
        additionalInputCount += ((Collection<?>) v.get(DAGUtils.ADDITIONAL_INPUTS_KEY)).size();
        for (Object input : ((Collection<?>) v.get(DAGUtils.ADDITIONAL_INPUTS_KEY))) {
          Map<String, Object> inputMap = (Map<String, Object>) input;
          Assert.assertTrue(inputMap.containsKey(DAGUtils.NAME_KEY));
          Assert.assertTrue(inputMap.containsKey(DAGUtils.CLASS_KEY));
          Assert.assertFalse(inputMap.containsKey(DAGUtils.INITIALIZER_KEY));
          Assert.assertEquals("input HistoryText", inputMap.get(DAGUtils.USER_PAYLOAD_AS_TEXT));
        }
      }

      if (v.containsKey(DAGUtils.ADDITIONAL_OUTPUTS_KEY)) {
        additionalOutputCount += ((Collection<?>) v.get(DAGUtils.ADDITIONAL_OUTPUTS_KEY)).size();
        for (Object output : ((Collection<?>) v.get(DAGUtils.ADDITIONAL_OUTPUTS_KEY))) {
          Map<String, Object> outputMap = (Map<String, Object>) output;
          Assert.assertTrue(outputMap.containsKey(DAGUtils.NAME_KEY));
          Assert.assertTrue(outputMap.containsKey(DAGUtils.CLASS_KEY));
          Assert.assertTrue(outputMap.containsKey(DAGUtils.INITIALIZER_KEY));
          Assert.assertEquals("uvOut HistoryText", outputMap.get(DAGUtils.USER_PAYLOAD_AS_TEXT));
        }
      }
    }

    // 1 input
    Assert.assertEquals(1, additionalInputCount);
    // 3 outputs due to vertex group
    Assert.assertEquals(3, additionalOutputCount);

    // 1 edge translates to 2 due to vertex group
    Assert.assertEquals(2, inEdgeIds.size());
    Assert.assertEquals(2, outEdgeIds.size());

    for (Object o : ((Collection<?>) atsMap.get(DAGUtils.EDGES_KEY))) {
      Map<String, Object> e = (Map<String, Object>) o;

      Assert.assertTrue(inEdgeIds.contains(e.get(DAGUtils.EDGE_ID_KEY)));
      Assert.assertTrue(outEdgeIds.contains(e.get(DAGUtils.EDGE_ID_KEY)));
      Assert.assertTrue(e.containsKey(DAGUtils.INPUT_VERTEX_NAME_KEY));
      Assert.assertTrue(e.containsKey(DAGUtils.OUTPUT_VERTEX_NAME_KEY));
      Assert.assertEquals(DataMovementType.SCATTER_GATHER.name(),
          e.get(DAGUtils.DATA_MOVEMENT_TYPE_KEY));
      Assert.assertEquals(DataSourceType.PERSISTED.name(), e.get(DAGUtils.DATA_SOURCE_TYPE_KEY));
      Assert.assertEquals(SchedulingType.SEQUENTIAL.name(), e.get(DAGUtils.SCHEDULING_TYPE_KEY));
      Assert.assertEquals("dummy output class", e.get(DAGUtils.EDGE_SOURCE_CLASS_KEY));
      Assert.assertEquals("dummy input class", e.get(DAGUtils.EDGE_DESTINATION_CLASS_KEY));
      Assert.assertEquals("Dummy History Text", e.get(DAGUtils.OUTPUT_USER_PAYLOAD_AS_TEXT));
      Assert.assertEquals("Dummy History Text", e.get(DAGUtils.INPUT_USER_PAYLOAD_AS_TEXT));
    }

    for (Object o : ((Collection<?>) atsMap.get(DAGUtils.VERTEX_GROUPS_KEY))) {
      Map<String, Object> e = (Map<String, Object>) o;
      Assert.assertEquals("uv12", e.get(DAGUtils.VERTEX_GROUP_NAME_KEY));
      Assert.assertTrue(e.containsKey(DAGUtils.VERTEX_GROUP_MEMBERS_KEY));
      Assert.assertTrue(e.containsKey(DAGUtils.VERTEX_GROUP_OUTPUTS_KEY));
      Assert.assertTrue(e.containsKey(DAGUtils.VERTEX_GROUP_EDGE_MERGED_INPUTS_KEY));
    }
  }

}
