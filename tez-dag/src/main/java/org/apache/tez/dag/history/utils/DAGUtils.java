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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.zip.Inflater;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.common.counters.AggregateTezCounter;
import org.apache.tez.common.counters.AggregateTezCounterDelegate;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanGroupInputEdgeInfo;
import org.apache.tez.dag.app.dag.impl.ServicePluginInfo;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl.DataEventDependencyInfo;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezTaskID;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Preconditions;

public class DAGUtils {

  public static final String DAG_NAME_KEY = "dagName";
  public static final String DAG_INFO_KEY = "dagInfo";
  public static final String DAG_CONTEXT_KEY = "dagContext";
  public static final String VERTICES_KEY = "vertices";
  public static final String EDGES_KEY = "edges";
  public static final String VERTEX_GROUPS_KEY = "vertexGroups";

  public static final String VERTEX_NAME_KEY = "vertexName";
  public static final String VERTEX_ID_KEY = "vertexId";
  public static final String PROCESSOR_CLASS_KEY = "processorClass";
  public static final String IN_EDGE_IDS_KEY = "inEdgeIds";
  public static final String OUT_EDGE_IDS_KEY = "outEdgeIds";
  public static final String ADDITIONAL_INPUTS_KEY = "additionalInputs";
  public static final String ADDITIONAL_OUTPUTS_KEY = "additionalOutputs";
  public static final String VERTEX_MANAGER_PLUGIN_CLASS_KEY =
      "vertexManagerPluginClass";
  public static final String USER_PAYLOAD_AS_TEXT = "userPayloadAsText";
  public static final String OUTPUT_USER_PAYLOAD_AS_TEXT = "outputUserPayloadAsText";
  public static final String INPUT_USER_PAYLOAD_AS_TEXT = "inputUserPayloadAsText";

  public static final String EDGE_ID_KEY = "edgeId";
  public static final String INPUT_VERTEX_NAME_KEY = "inputVertexName";
  public static final String OUTPUT_VERTEX_NAME_KEY = "outputVertexName";
  public static final String DATA_MOVEMENT_TYPE_KEY = "dataMovementType";
  public static final String DATA_SOURCE_TYPE_KEY = "dataSourceType";
  public static final String SCHEDULING_TYPE_KEY = "schedulingType";
  public static final String EDGE_SOURCE_CLASS_KEY = "edgeSourceClass";
  public static final String EDGE_DESTINATION_CLASS_KEY =
      "edgeDestinationClass";
  public static final String EDGE_MANAGER_CLASS_KEY = "edgeManagerClass";

  public static final String NAME_KEY = "name";
  public static final String CLASS_KEY = "class";
  public static final String INITIALIZER_KEY = "initializer";

  public static final String VERTEX_GROUP_NAME_KEY = "groupName";
  public static final String VERTEX_GROUP_MEMBERS_KEY = "groupMembers";
  public static final String VERTEX_GROUP_OUTPUTS_KEY = "outputs";
  public static final String VERTEX_GROUP_EDGE_MERGED_INPUTS_KEY = "edgeMergedInputs";
  public static final String VERTEX_GROUP_DESTINATION_VERTEX_NAME_KEY = "destinationVertexName";



  public static JSONObject generateSimpleJSONPlan(DAGPlan dagPlan) throws JSONException {
    JSONObject dagJson;
    try {
      dagJson = new JSONObject(convertDAGPlanToATSMap(dagPlan));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    return dagJson;
  }
  
  public static JSONObject convertDataEventDependencyInfoToJSON(List<DataEventDependencyInfo> info) {
    return new JSONObject(convertDataEventDependecyInfoToATS(info));
  }
  
  public static Map<String, Object> convertDataEventDependecyInfoToATS(List<DataEventDependencyInfo> info) {
    ArrayList<Object> infoList = new ArrayList<Object>();
    for (DataEventDependencyInfo event : info) {
      Map<String, Object> eventObj = new LinkedHashMap<String, Object>();
      String id = "";
      if (event.getTaskAttemptId() != null) {
        id = event.getTaskAttemptId().toString();
      }
      eventObj.put(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), id);
      eventObj.put(ATSConstants.TIMESTAMP, event.getTimestamp());
      infoList.add(eventObj);
    }
    Map<String,Object> object = new LinkedHashMap<String, Object>();
    putInto(object, ATSConstants.LAST_DATA_EVENTS, infoList);
    return object;
  }

  public static JSONObject convertCountersToJSON(TezCounters counters)
      throws JSONException {
    JSONObject jsonObject = new JSONObject(convertCountersToATSMap(counters));
    return jsonObject;
  }

  public static Map<String,Object> convertCountersToATSMap(TezCounters counters) {
    Map<String, Object> object = new LinkedHashMap<String, Object>();
    if (counters == null) {
        return object;
      }
    ArrayList<Object> counterGroupsList = new ArrayList<Object>();
    for (CounterGroup group : counters) {
        ArrayList<Object> counterList = new ArrayList<Object>();
        for (TezCounter counter : group) {
          if (counter.getValue() != 0) {
            Map<String,Object> counterMap = new LinkedHashMap<String, Object>();
            counterMap.put(ATSConstants.COUNTER_NAME, counter.getName());
            if (!counter.getDisplayName().equals(counter.getName())) {
              counterMap.put(ATSConstants.COUNTER_DISPLAY_NAME,
                    counter.getDisplayName());
            }
            counterMap.put(ATSConstants.COUNTER_VALUE, counter.getValue());
            if (counter instanceof AggregateTezCounter) {
              counterMap.put(ATSConstants.COUNTER_INSTANCE_COUNT,
                  ((AggregateTezCounter)counter).getCount());
              counterMap.put(ATSConstants.COUNTER_MAX_VALUE,
                    ((AggregateTezCounter)counter).getMax());
              counterMap.put(ATSConstants.COUNTER_MIN_VALUE,
                  ((AggregateTezCounter)counter).getMin());

            }
            counterList.add(counterMap);
          }
        }
        if (!counterList.isEmpty()) {
          Map<String,Object> counterGroupMap = new LinkedHashMap<String, Object>();
          counterGroupMap.put(ATSConstants.COUNTER_GROUP_NAME, group.getName());
          if (!group.getDisplayName().equals(group.getName())) {
            counterGroupMap.put(ATSConstants.COUNTER_GROUP_DISPLAY_NAME,
                group.getDisplayName());
          }
          counterGroupMap.put(ATSConstants.COUNTERS, counterList);
          counterGroupsList.add(counterGroupMap);
        }
      }
    putInto(object, ATSConstants.COUNTER_GROUPS, counterGroupsList);
    return object;
  }

  static Map<String, String> createDagInfoMap(DAGPlan dagPlan) {
    Preconditions.checkArgument(dagPlan.hasCallerContext());
    Map<String, String> dagInfo = new TreeMap<String, String>();
    dagInfo.put(ATSConstants.CONTEXT, dagPlan.getCallerContext().getContext());
    if (dagPlan.getCallerContext().hasCallerId()) {
      dagInfo.put(ATSConstants.CALLER_ID, dagPlan.getCallerContext().getCallerId());
    }
    if (dagPlan.getCallerContext().hasCallerType()) {
      dagInfo.put(ATSConstants.CALLER_TYPE, dagPlan.getCallerContext().getCallerType());
    }
    if (dagPlan.getCallerContext().hasBlob()) {
      dagInfo.put(ATSConstants.DESCRIPTION, dagPlan.getCallerContext().getBlob());
    }
    return dagInfo;
  }

  public static Map<String, Object> convertDAGPlanToATSMap(final DAGPlan
      dagPlan) throws IOException {
    final Inflater inflater = TezCommonUtils.newInflater();
    try {
      return convertDAGPlanToATSMap(dagPlan, inflater);
    } finally {
      inflater.end();
    }
  }

  /**
   * Auxiliary method to convert dagPlan to ATS Map.
   * @param dagPlan dag plan.
   * @param inflater inflater. This method shouldn't end it.
   * @return ATS MAP
   */
  private static Map<String, Object> convertDAGPlanToATSMap(DAGPlan dagPlan,
      final Inflater inflater) {
    final String VERSION_KEY = "version";
    final int version = 2;
    Map<String,Object> dagMap = new LinkedHashMap<String, Object>();
    dagMap.put(DAG_NAME_KEY, dagPlan.getName());
    if (dagPlan.hasDagInfo()) {
      dagMap.put(DAG_INFO_KEY, dagPlan.getDagInfo());
    }
    if (dagPlan.hasCallerContext()) {
      dagMap.put(DAG_CONTEXT_KEY, createDagInfoMap(dagPlan));
    }
    dagMap.put(VERSION_KEY, version);
    ArrayList<Object> verticesList = new ArrayList<Object>();
    for (DAGProtos.VertexPlan vertexPlan : dagPlan.getVertexList()) {
      Map<String,Object> vertexMap = new LinkedHashMap<String, Object>();
      vertexMap.put(VERTEX_NAME_KEY, vertexPlan.getName());
      if (vertexPlan.hasProcessorDescriptor()) {
        vertexMap.put(PROCESSOR_CLASS_KEY,
            vertexPlan.getProcessorDescriptor().getClassName());
        if (vertexPlan.getProcessorDescriptor().hasHistoryText()) {
          vertexMap.put(USER_PAYLOAD_AS_TEXT,
              DagTypeConverters.getHistoryTextFromProto(
                  vertexPlan.getProcessorDescriptor(), inflater));
        }
      }

      ArrayList<Object> inEdgeIdList = new ArrayList<Object>();
      inEdgeIdList.addAll(vertexPlan.getInEdgeIdList());
      putInto(vertexMap, IN_EDGE_IDS_KEY, inEdgeIdList);

      ArrayList<Object> outEdgeIdList = new ArrayList<Object>();
      outEdgeIdList.addAll(vertexPlan.getOutEdgeIdList());
      putInto(vertexMap, OUT_EDGE_IDS_KEY, outEdgeIdList);

      ArrayList<Object> inputsList = new ArrayList<Object>();
      for (DAGProtos.RootInputLeafOutputProto input :
          vertexPlan.getInputsList()) {
        Map<String,Object> inputMap = new LinkedHashMap<String, Object>();
        inputMap.put(NAME_KEY, input.getName());
        inputMap.put(CLASS_KEY, input.getIODescriptor().getClassName());
        if (input.hasControllerDescriptor()) {
          inputMap.put(INITIALIZER_KEY, input.getControllerDescriptor().getClassName());
        }
        if (input.getIODescriptor().hasHistoryText()) {
          inputMap.put(USER_PAYLOAD_AS_TEXT,
              DagTypeConverters.getHistoryTextFromProto(
                  input.getIODescriptor(), inflater));
        }
        inputsList.add(inputMap);
      }
      putInto(vertexMap, ADDITIONAL_INPUTS_KEY, inputsList);

      ArrayList<Object> outputsList = new ArrayList<Object>();
      for (DAGProtos.RootInputLeafOutputProto output :
          vertexPlan.getOutputsList()) {
        Map<String,Object> outputMap = new LinkedHashMap<String, Object>();
        outputMap.put(NAME_KEY, output.getName());
        outputMap.put(CLASS_KEY, output.getIODescriptor().getClassName());
        if (output.hasControllerDescriptor()) {
          outputMap.put(INITIALIZER_KEY, output.getControllerDescriptor().getClassName());
        }
        if (output.getIODescriptor().hasHistoryText()) {
          outputMap.put(USER_PAYLOAD_AS_TEXT,
              DagTypeConverters.getHistoryTextFromProto(
                  output.getIODescriptor(), inflater));
        }
        outputsList.add(outputMap);
      }
      putInto(vertexMap, ADDITIONAL_OUTPUTS_KEY, outputsList);

      if (vertexPlan.hasVertexManagerPlugin()) {
        vertexMap.put(VERTEX_MANAGER_PLUGIN_CLASS_KEY,
            vertexPlan.getVertexManagerPlugin().getClassName());
      }

      verticesList.add(vertexMap);
    }
    putInto(dagMap, VERTICES_KEY, verticesList);

    ArrayList<Object> edgesList = new ArrayList<Object>();
    for (DAGProtos.EdgePlan edgePlan : dagPlan.getEdgeList()) {
      Map<String,Object> edgeMap = new LinkedHashMap<String, Object>();
      edgeMap.put(EDGE_ID_KEY, edgePlan.getId());
      edgeMap.put(INPUT_VERTEX_NAME_KEY, edgePlan.getInputVertexName());
      edgeMap.put(OUTPUT_VERTEX_NAME_KEY, edgePlan.getOutputVertexName());
      edgeMap.put(DATA_MOVEMENT_TYPE_KEY,
          edgePlan.getDataMovementType().name());
      edgeMap.put(DATA_SOURCE_TYPE_KEY, edgePlan.getDataSourceType().name());
      edgeMap.put(SCHEDULING_TYPE_KEY, edgePlan.getSchedulingType().name());
      edgeMap.put(EDGE_SOURCE_CLASS_KEY,
          edgePlan.getEdgeSource().getClassName());
      edgeMap.put(EDGE_DESTINATION_CLASS_KEY,
          edgePlan.getEdgeDestination().getClassName());
      if (edgePlan.getEdgeSource().hasHistoryText()) {
        edgeMap.put(OUTPUT_USER_PAYLOAD_AS_TEXT,
            DagTypeConverters.getHistoryTextFromProto(
                edgePlan.getEdgeSource(), inflater));
      }
      if (edgePlan.getEdgeDestination().hasHistoryText()) {
        edgeMap.put(INPUT_USER_PAYLOAD_AS_TEXT,
            DagTypeConverters.getHistoryTextFromProto(
                edgePlan.getEdgeDestination(), inflater));
      } // TEZ-2286 this is missing edgemanager descriptor for custom edge
      edgesList.add(edgeMap);
    }
    putInto(dagMap, EDGES_KEY, edgesList);

    ArrayList<Object> vertexGroupsList = new ArrayList<Object>();
    for (DAGProtos.PlanVertexGroupInfo vertexGroupInfo :
        dagPlan.getVertexGroupsList()) {
      Map<String,Object> groupMap = new LinkedHashMap<String, Object>();
      groupMap.put(VERTEX_GROUP_NAME_KEY, vertexGroupInfo.getGroupName());
      if (vertexGroupInfo.getGroupMembersCount() > 0 ) {
        groupMap.put(VERTEX_GROUP_MEMBERS_KEY, vertexGroupInfo.getGroupMembersList());
      }
      if (vertexGroupInfo.getOutputsCount() > 0) {
        groupMap.put(VERTEX_GROUP_OUTPUTS_KEY, vertexGroupInfo.getOutputsList());
      }

      if (vertexGroupInfo.getEdgeMergedInputsCount() > 0) {
        ArrayList<Object> edgeMergedInputs = new ArrayList<Object>();
        for (PlanGroupInputEdgeInfo edgeMergedInputInfo :
            vertexGroupInfo.getEdgeMergedInputsList()) {
          Map<String,Object> edgeMergedInput = new LinkedHashMap<String, Object>();
          edgeMergedInput.put(VERTEX_GROUP_DESTINATION_VERTEX_NAME_KEY,
              edgeMergedInputInfo.getDestVertexName());
          if (edgeMergedInputInfo.hasMergedInput()
            && edgeMergedInputInfo.getMergedInput().hasClassName()) {
            edgeMergedInput.put(PROCESSOR_CLASS_KEY,
                edgeMergedInputInfo.getMergedInput().getClassName());
            if (edgeMergedInputInfo.getMergedInput().hasHistoryText()) {
              edgeMergedInput.put(USER_PAYLOAD_AS_TEXT,
                  DagTypeConverters.getHistoryTextFromProto(
                      edgeMergedInputInfo.getMergedInput(), inflater));
            }
          }
          edgeMergedInputs.add(edgeMergedInput);
        }
        groupMap.put(VERTEX_GROUP_EDGE_MERGED_INPUTS_KEY, edgeMergedInputs);
      }
      vertexGroupsList.add(groupMap);
    }
    putInto(dagMap, VERTEX_GROUPS_KEY, vertexGroupsList);

    return dagMap;
  }

  private static void putInto(Map<String, Object> map, String key,
      ArrayList<Object> list) {
    if (list.isEmpty()) {
      return;
    }
    map.put(key, list);
  }

  private static ArrayList<String> convertToStringArrayList(
      Collection<TezTaskID> collection) {
    ArrayList<String> list = new ArrayList<String>(collection.size());
    for (TezTaskID t : collection) {
      list.add(t.toString());
    }
    return list;
  }

  public static JSONObject convertVertexStatsToJSON(VertexStats vertexStats)
      throws JSONException {
    JSONObject jsonObject = new JSONObject(convertVertexStatsToATSMap(vertexStats));
    return jsonObject;
  }

  public static Map<String,Object> convertVertexStatsToATSMap(
      VertexStats vertexStats) {
    Map<String,Object> vertexStatsMap = new LinkedHashMap<String, Object>();
    if (vertexStats == null) {
      return vertexStatsMap;
    }

    final String FIRST_TASK_START_TIME_KEY = "firstTaskStartTime";
    final String FIRST_TASKS_TO_START_KEY = "firstTasksToStart";
    final String LAST_TASK_FINISH_TIME_KEY = "lastTaskFinishTime";
    final String LAST_TASKS_TO_FINISH_KEY = "lastTasksToFinish";

    final String MIN_TASK_DURATION = "minTaskDuration";
    final String MAX_TASK_DURATION = "maxTaskDuration";
    final String AVG_TASK_DURATION = "avgTaskDuration";

    final String SHORTEST_DURATION_TASKS = "shortestDurationTasks";
    final String LONGEST_DURATION_TASKS = "longestDurationTasks";

    vertexStatsMap.put(FIRST_TASK_START_TIME_KEY, vertexStats.getFirstTaskStartTime());
    if (vertexStats.getFirstTasksToStart() != null
        && !vertexStats.getFirstTasksToStart().isEmpty()) {
      vertexStatsMap.put(FIRST_TASKS_TO_START_KEY,
          convertToStringArrayList(vertexStats.getFirstTasksToStart()));
    }
    vertexStatsMap.put(LAST_TASK_FINISH_TIME_KEY, vertexStats.getLastTaskFinishTime());
    if (vertexStats.getLastTasksToFinish() != null
        && !vertexStats.getLastTasksToFinish().isEmpty()) {
      vertexStatsMap.put(LAST_TASKS_TO_FINISH_KEY,
          convertToStringArrayList(vertexStats.getLastTasksToFinish()));
    }

    vertexStatsMap.put(MIN_TASK_DURATION, vertexStats.getMinTaskDuration());
    vertexStatsMap.put(MAX_TASK_DURATION, vertexStats.getMaxTaskDuration());
    vertexStatsMap.put(AVG_TASK_DURATION, vertexStats.getAvgTaskDuration());

    if (vertexStats.getShortestDurationTasks() != null
        && !vertexStats.getShortestDurationTasks().isEmpty()) {
      vertexStatsMap.put(SHORTEST_DURATION_TASKS,
          convertToStringArrayList(vertexStats.getShortestDurationTasks()));
    }
    if (vertexStats.getLongestDurationTasks() != null
        && !vertexStats.getLongestDurationTasks().isEmpty()) {
      vertexStatsMap.put(LONGEST_DURATION_TASKS,
          convertToStringArrayList(vertexStats.getLongestDurationTasks()));
    }

    return vertexStatsMap;
  }

  public static JSONObject convertServicePluginToJSON(
      ServicePluginInfo servicePluginInfo) {
    JSONObject jsonObject = new JSONObject(convertServicePluginToATSMap(servicePluginInfo));
    return jsonObject;
  }

  public static Map<String,Object> convertServicePluginToATSMap(
      ServicePluginInfo servicePluginInfo) {
    Map<String, Object> servicePluginMap = new LinkedHashMap<String, Object>();
    if (servicePluginInfo == null) {
      return servicePluginMap;
    }
    servicePluginMap.put(ATSConstants.TASK_SCHEDULER_NAME,
        servicePluginInfo.getTaskSchedulerName());
    servicePluginMap.put(ATSConstants.TASK_SCHEDULER_CLASS_NAME,
        servicePluginInfo.getTaskSchedulerClassName());
    servicePluginMap.put(ATSConstants.TASK_COMMUNICATOR_NAME,
        servicePluginInfo.getTaskCommunicatorName());
    servicePluginMap.put(ATSConstants.TASK_COMMUNICATOR_CLASS_NAME,
        servicePluginInfo.getTaskCommunicatorClassName());
    servicePluginMap.put(ATSConstants.CONTAINER_LAUNCHER_NAME,
        servicePluginInfo.getContainerLauncherName());
    servicePluginMap.put(ATSConstants.CONTAINER_LAUNCHER_CLASS_NAME,
        servicePluginInfo.getContainerLauncherClassName());
    return servicePluginMap;
  }


  public static Map<String,Object> convertEdgeProperty(
      EdgeProperty edge) {
    Map<String, Object> jsonDescriptor = new HashMap<String, Object>();
    
    jsonDescriptor.put(DATA_MOVEMENT_TYPE_KEY,
        edge.getDataMovementType().name());
    jsonDescriptor.put(DATA_SOURCE_TYPE_KEY, edge.getDataSourceType().name());
    jsonDescriptor.put(SCHEDULING_TYPE_KEY, edge.getSchedulingType().name());
    jsonDescriptor.put(EDGE_SOURCE_CLASS_KEY,
        edge.getEdgeSource().getClassName());
    jsonDescriptor.put(EDGE_DESTINATION_CLASS_KEY,
        edge.getEdgeDestination().getClassName());
    String history = edge.getEdgeSource().getHistoryText();
    if (history != null) {
      jsonDescriptor.put(OUTPUT_USER_PAYLOAD_AS_TEXT, history);
    }
    history = edge.getEdgeDestination().getHistoryText();
    if (history != null) {
      jsonDescriptor.put(INPUT_USER_PAYLOAD_AS_TEXT, history);
    }
    EdgeManagerPluginDescriptor descriptor = edge.getEdgeManagerDescriptor();
    if (descriptor != null) {
      jsonDescriptor.put(EDGE_MANAGER_CLASS_KEY, descriptor.getClassName());
      if (descriptor.getHistoryText() != null && !descriptor.getHistoryText().isEmpty()) {
        jsonDescriptor.put(USER_PAYLOAD_AS_TEXT, descriptor.getHistoryText());
      }
    }
    return jsonDescriptor;
  }
  
  public static Map<String,Object> convertEdgeManagerPluginDescriptor(
      EdgeManagerPluginDescriptor descriptor) {
    Map<String, Object> jsonDescriptor = new HashMap<String, Object>();
    jsonDescriptor.put(EDGE_MANAGER_CLASS_KEY, descriptor.getClassName());
    if (descriptor.getHistoryText() != null && !descriptor.getHistoryText().isEmpty()) {
      jsonDescriptor.put(USER_PAYLOAD_AS_TEXT, descriptor.getHistoryText());
    }
    return jsonDescriptor;
  }

  public static Map<String, String> convertConfigurationToATSMap(Configuration conf) {
    // Copy configuration to avoid CME since iterator is not thread safe until HADOOP-13500
    Iterator<Entry<String, String>> iter = new Configuration(conf).iterator();
    Map<String, String> atsConf = new TreeMap<String, String>();
    while (iter.hasNext()) {
      Entry<String, String> entry = iter.next();
      atsConf.put(entry.getKey(), entry.getValue());
    }
    return atsConf;
  }

  public static Map<String, Object> convertTezVersionToATSMap(VersionInfo versionInfo) {
    Map<String, Object> atsInfo = new TreeMap<String, Object>();
    atsInfo.put(ATSConstants.VERSION, versionInfo.getVersion());
    atsInfo.put(ATSConstants.BUILD_TIME, versionInfo.getBuildTime());
    atsInfo.put(ATSConstants.REVISION, versionInfo.getRevision());
    return atsInfo;
  }


}
