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

import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.records.DAGProtos;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DAGUtils {

  public static JSONObject generateSimpleJSONPlan(DAGProtos.DAGPlan dagPlan) throws JSONException {

    final String DAG_NAME_KEY = "dagName";
    final String VERTICES_KEY = "vertices";
    final String EDGES_KEY = "edges";

    final String VERTEX_NAME_KEY = "vertexName";
    final String PROCESSOR_CLASS_KEY = "processorClass";
    final String IN_EDGE_IDS_KEY = "inEdgeIds";
    final String OUT_EDGE_IDS_KEY = "outEdgeIds";
    final String ADDITIONAL_INPUTS_KEY = "additionalInputs";
    final String ADDITIONAL_OUTPUTS_KEY = "additionalOutputs";
    final String VERTEX_MANAGER_PLUGIN_CLASS_KEY =
        "vertexManagerPluginClass";

    final String EDGE_ID_KEY = "edgeId";
    final String INPUT_VERTEX_NAME_KEY = "inputVertexName";
    final String OUTPUT_VERTEX_NAME_KEY = "outputVertexName";
    final String DATA_MOVEMENT_TYPE_KEY = "dataMovementType";
    final String DATA_SOURCE_TYPE_KEY = "dataSourceType";
    final String SCHEDULING_TYPE_KEY = "schedulingType";
    final String EDGE_SOURCE_CLASS_KEY = "edgeSourceClass";
    final String EDGE_DESTINATION_CLASS_KEY =
        "edgeDestinationClass";

    final String NAME_KEY = "name";
    final String CLASS_KEY = "class";
    final String INITIALIZER_KEY = "initializer";

    JSONObject dagJson = new JSONObject();
    dagJson.put(DAG_NAME_KEY, dagPlan.getName());
    for (DAGProtos.VertexPlan vertexPlan : dagPlan.getVertexList()) {
      JSONObject vertexJson = new JSONObject();
      vertexJson.put(VERTEX_NAME_KEY, vertexPlan.getName());

      if (vertexPlan.hasProcessorDescriptor()) {
        vertexJson.put(PROCESSOR_CLASS_KEY,
            vertexPlan.getProcessorDescriptor().getClassName());
      }

      for (String inEdgeId : vertexPlan.getInEdgeIdList()) {
        vertexJson.accumulate(IN_EDGE_IDS_KEY, inEdgeId);
      }
      for (String outEdgeId : vertexPlan.getOutEdgeIdList()) {
        vertexJson.accumulate(OUT_EDGE_IDS_KEY, outEdgeId);
      }

      for (DAGProtos.RootInputLeafOutputProto input :
          vertexPlan.getInputsList()) {
        JSONObject jsonInput = new JSONObject();
        jsonInput.put(NAME_KEY, input.getName());
        jsonInput.put(CLASS_KEY, input.getEntityDescriptor().getClassName());
        if (input.hasInitializerClassName()) {
          jsonInput.put(INITIALIZER_KEY, input.getInitializerClassName());
        }
        vertexJson.accumulate(ADDITIONAL_INPUTS_KEY, jsonInput);
      }

      for (DAGProtos.RootInputLeafOutputProto output :
          vertexPlan.getOutputsList()) {
        JSONObject jsonOutput = new JSONObject();
        jsonOutput.put(NAME_KEY, output.getName());
        jsonOutput.put(CLASS_KEY, output.getEntityDescriptor().getClassName());
        if (output.hasInitializerClassName()) {
          jsonOutput.put(INITIALIZER_KEY, output.getInitializerClassName());
        }
        vertexJson.accumulate(ADDITIONAL_OUTPUTS_KEY, jsonOutput);
      }

      if (vertexPlan.hasVertexManagerPlugin()) {
        vertexJson.put(VERTEX_MANAGER_PLUGIN_CLASS_KEY,
            vertexPlan.getVertexManagerPlugin().getClassName());
      }

      dagJson.accumulate(VERTICES_KEY, vertexJson);
    }

    for (DAGProtos.EdgePlan edgePlan : dagPlan.getEdgeList()) {
      JSONObject edgeJson = new JSONObject();
      edgeJson.put(EDGE_ID_KEY, edgePlan.getId());
      edgeJson.put(INPUT_VERTEX_NAME_KEY, edgePlan.getInputVertexName());
      edgeJson.put(OUTPUT_VERTEX_NAME_KEY, edgePlan.getOutputVertexName());
      edgeJson.put(DATA_MOVEMENT_TYPE_KEY,
          edgePlan.getDataMovementType().name());
      edgeJson.put(DATA_SOURCE_TYPE_KEY, edgePlan.getDataSourceType().name());
      edgeJson.put(SCHEDULING_TYPE_KEY, edgePlan.getSchedulingType().name());
      edgeJson.put(EDGE_SOURCE_CLASS_KEY,
          edgePlan.getEdgeSource().getClassName());
      edgeJson.put(EDGE_DESTINATION_CLASS_KEY,
          edgePlan.getEdgeDestination().getClassName());

      dagJson.accumulate(EDGES_KEY, edgeJson);
    }

    return dagJson;
  }

  public static JSONObject convertCountersToJSON(TezCounters counters)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    if (counters == null) {
      return jsonObject;
    }

    for (CounterGroup group : counters) {
      JSONObject jsonCGrp = new JSONObject();
      jsonCGrp.put(ATSConstants.COUNTER_GROUP_NAME, group.getName());
      jsonCGrp.put(ATSConstants.COUNTER_GROUP_DISPLAY_NAME,
          group.getDisplayName());
      for (TezCounter counter : group) {
        JSONObject counterJson = new JSONObject();
        counterJson.put(ATSConstants.COUNTER_NAME, counter.getName());
        counterJson.put(ATSConstants.COUNTER_DISPLAY_NAME,
            counter.getDisplayName());
        counterJson.put(ATSConstants.COUNTER_VALUE, counter.getValue());
        jsonCGrp.accumulate(ATSConstants.COUNTERS, counterJson);
      }
      jsonObject.accumulate(ATSConstants.COUNTER_GROUPS, jsonCGrp);
    }
    return jsonObject;
  }

}
