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

package org.apache.tez.history.parser.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringInterner;
import org.apache.tez.client.CallerContext;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.history.HistoryEventType;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.classification.InterfaceAudience.Public;
import static org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
public class DagInfo extends BaseInfo {

  private static final Log LOG = LogFactory.getLog(DagInfo.class);

  //Fields populated via JSON
  private final String name;
  private final long startTime;
  private final long endTime;
  private final long submitTime;
  private final int failedTasks;
  private final String dagId;
  private final int numVertices;
  private final String status;
  private final String diagnostics;
  private VersionInfo versionInfo;
  private CallerContext callerContext;

  //VertexID --> VertexName & vice versa
  private final BidiMap vertexNameIDMapping;

  //edgeId to EdgeInfo mapping
  private final Map<Integer, EdgeInfo> edgeInfoMap;

  //Only for internal parsing (vertexname mapping)
  private Map<String, BasicVertexInfo> basicVertexInfoMap;

  //VertexName --> VertexInfo
  private Map<String, VertexInfo> vertexNameMap;

  private Multimap<Container, TaskAttemptInfo> containerMapping;

  DagInfo(JSONObject jsonObject) throws JSONException {
    super(jsonObject);

    vertexNameMap = Maps.newHashMap();
    vertexNameIDMapping = new DualHashBidiMap();
    edgeInfoMap = Maps.newHashMap();
    basicVertexInfoMap = Maps.newHashMap();
    containerMapping = LinkedHashMultimap.create();

    Preconditions.checkArgument(jsonObject.getString(Constants.ENTITY_TYPE).equalsIgnoreCase
        (Constants.TEZ_DAG_ID));

    dagId = StringInterner.weakIntern(jsonObject.getString(Constants.ENTITY));

    //Parse additional Info
    JSONObject otherInfoNode = jsonObject.getJSONObject(Constants.OTHER_INFO);

    long sTime = otherInfoNode.optLong(Constants.START_TIME);
    long eTime= otherInfoNode.optLong(Constants.FINISH_TIME);
    if (eTime < sTime) {
      LOG.warn("DAG has got wrong start/end values. "
          + "startTime=" + sTime + ", endTime=" + eTime + ". Will check "
          + "timestamps in DAG started/finished events");

      // Check if events DAG_STARTED, DAG_FINISHED can be made use of
      for(Event event : eventList) {
        switch (HistoryEventType.valueOf(event.getType())) {
        case DAG_STARTED:
          sTime = event.getAbsoluteTime();
          break;
        case DAG_FINISHED:
          eTime = event.getAbsoluteTime();
          break;
        default:
          break;
        }
      }

      if (eTime < sTime) {
        LOG.warn("DAG has got wrong start/end values in events as well. "
            + "startTime=" + sTime + ", endTime=" + eTime);
      }
    }
    startTime = sTime;
    endTime = eTime;

    //TODO: Not getting populated correctly for lots of jobs.  Verify
    submitTime = otherInfoNode.optLong(Constants.START_REQUESTED_TIME);
    diagnostics = otherInfoNode.optString(Constants.DIAGNOSTICS);
    failedTasks = otherInfoNode.optInt(Constants.NUM_FAILED_TASKS);
    JSONObject dagPlan = otherInfoNode.optJSONObject(Constants.DAG_PLAN);
    name = StringInterner.weakIntern((dagPlan != null) ? (dagPlan.optString(Constants.DAG_NAME)) : null);
    if (dagPlan != null) {
      JSONArray vertices = dagPlan.optJSONArray(Constants.VERTICES);
      if (vertices != null) {
        numVertices = vertices.length();
      } else {
        numVertices = 0;
      }
      parseDAGPlan(dagPlan);
    } else {
      numVertices = 0;
    }
    status = StringInterner.weakIntern(otherInfoNode.optString(Constants.STATUS));

    //parse name id mapping
    JSONObject vertexIDMappingJson = otherInfoNode.optJSONObject(Constants.VERTEX_NAME_ID_MAPPING);
    if (vertexIDMappingJson != null) {
      //get vertex name
      for (Map.Entry<String, BasicVertexInfo> entry : basicVertexInfoMap.entrySet()) {
        String vertexId = vertexIDMappingJson.optString(entry.getKey());
        //vertexName --> vertexId
        vertexNameIDMapping.put(entry.getKey(), vertexId);
      }
    }
  }

  public static DagInfo create(JSONObject jsonObject) throws JSONException {
    DagInfo dagInfo = new DagInfo(jsonObject);
    return dagInfo;
  }

  private void parseDAGPlan(JSONObject dagPlan) throws JSONException {
    int version = dagPlan.optInt(Constants.VERSION, 1);
    parseEdges(dagPlan.optJSONArray(Constants.EDGES));

    JSONArray verticesInfo = dagPlan.optJSONArray(Constants.VERTICES);
    parseBasicVertexInfo(verticesInfo);

    if (version > 1) {
      parseDAGContext(dagPlan.optJSONObject(Constants.DAG_CONTEXT));
    }
  }

  private void parseDAGContext(JSONObject callerContextInfo) {
    if (callerContextInfo == null) {
      LOG.info("No DAG Caller Context available");
      return;
    }
    String context = callerContextInfo.optString(Constants.CONTEXT);
    String callerId = callerContextInfo.optString(Constants.CALLER_ID);
    String callerType = callerContextInfo.optString(Constants.CALLER_TYPE);
    String description = callerContextInfo.optString(Constants.DESCRIPTION);

    this.callerContext = CallerContext.create(context, description);
    if (callerId != null && !callerId.isEmpty() && callerType != null && !callerType.isEmpty()) {
      this.callerContext.setCallerIdAndType(callerId, callerType);
    } else {
      LOG.info("No DAG Caller Context Id and Type available");
    }

  }

  private void parseBasicVertexInfo(JSONArray verticesInfo) throws JSONException {
    if (verticesInfo == null) {
      LOG.info("No vertices available.");
      return;
    }

    //Parse basic information available in DAG for vertex and edges
    for (int i = 0; i < verticesInfo.length(); i++) {
      BasicVertexInfo basicVertexInfo = new BasicVertexInfo();

      JSONObject vJson = verticesInfo.getJSONObject(i);
      basicVertexInfo.vertexName =
          vJson.optString(Constants.VERTEX_NAME);
      JSONArray inEdges = vJson.optJSONArray(Constants.IN_EDGE_IDS);
      if (inEdges != null) {
        String[] inEdgeIds = new String[inEdges.length()];
        for (int j = 0; j < inEdges.length(); j++) {
          inEdgeIds[j] = inEdges.get(j).toString();
        }
        basicVertexInfo.inEdgeIds = inEdgeIds;
      }

      JSONArray outEdges = vJson.optJSONArray(Constants.OUT_EDGE_IDS);
      if (outEdges != null) {
        String[] outEdgeIds = new String[outEdges.length()];
        for (int j = 0; j < outEdges.length(); j++) {
          outEdgeIds[j] = outEdges.get(j).toString();
        }
        basicVertexInfo.outEdgeIds = outEdgeIds;
      }

      JSONArray addInputsJson =
          vJson.optJSONArray(Constants.ADDITIONAL_INPUTS);
      basicVertexInfo.additionalInputs = parseAdditionalDetailsForVertex(addInputsJson);

      JSONArray addOutputsJson =
          vJson.optJSONArray(Constants.ADDITIONAL_OUTPUTS);
      basicVertexInfo.additionalOutputs = parseAdditionalDetailsForVertex(addOutputsJson);

      basicVertexInfoMap.put(basicVertexInfo.vertexName, basicVertexInfo);
    }
  }

  /**
   * get additional details available for every vertex in the dag
   *
   * @param jsonArray
   * @return AdditionalInputOutputDetails[]
   * @throws JSONException
   */
  private AdditionalInputOutputDetails[] parseAdditionalDetailsForVertex(JSONArray jsonArray) throws
      JSONException {
    if (jsonArray != null) {
      AdditionalInputOutputDetails[]
          additionalInputOutputDetails = new AdditionalInputOutputDetails[jsonArray.length()];
      for (int j = 0; j < jsonArray.length(); j++) {
        String name = jsonArray.getJSONObject(j).optString(
            Constants.NAME);
        String clazz = jsonArray.getJSONObject(j).optString(
            Constants.CLASS);
        String initializer =
            jsonArray.getJSONObject(j).optString(Constants.INITIALIZER);
        String userPayloadText = jsonArray.getJSONObject(j).optString(
            Constants.USER_PAYLOAD_TEXT);

        additionalInputOutputDetails[j] =
            new AdditionalInputOutputDetails(name, clazz, initializer, userPayloadText);

      }
      return additionalInputOutputDetails;
    }
    return null;
  }

  /**
   * Parse edge details in the DAG
   *
   * @param edgesArray
   *
   * @throws JSONException
   */
  private void parseEdges(JSONArray edgesArray) throws JSONException {
    if (edgesArray == null) {
      return;
    }
    for (int i = 0; i < edgesArray.length(); i++) {
      JSONObject edge = edgesArray.getJSONObject(i);
      Integer edgeId = edge.optInt(Constants.EDGE_ID);
      String inputVertexName =
          edge.optString(Constants.INPUT_VERTEX_NAME);
      String outputVertexName =
          edge.optString(Constants.OUTPUT_VERTEX_NAME);
      String dataMovementType =
          edge.optString(Constants.DATA_MOVEMENT_TYPE);
      String edgeSourceClass =
          edge.optString(Constants.EDGE_SOURCE_CLASS);
      String edgeDestinationClass =
          edge.optString(Constants.EDGE_DESTINATION_CLASS);
      String inputUserPayloadAsText =
          edge.optString(Constants.INPUT_PAYLOAD_TEXT);
      String outputUserPayloadAsText =
          edge.optString(Constants.OUTPUT_PAYLOAD_TEXT);
      EdgeInfo edgeInfo = new EdgeInfo(inputVertexName, outputVertexName,
          dataMovementType, edgeSourceClass, edgeDestinationClass, inputUserPayloadAsText,
          outputUserPayloadAsText);
      edgeInfoMap.put(edgeId, edgeInfo);
    }
  }

  static class BasicVertexInfo {
    String vertexName;
    String[] inEdgeIds;
    String[] outEdgeIds;
    AdditionalInputOutputDetails[] additionalInputs;
    AdditionalInputOutputDetails[] additionalOutputs;
  }

  void addVertexInfo(VertexInfo vertexInfo) {
    BasicVertexInfo basicVertexInfo = basicVertexInfoMap.get(vertexInfo.getVertexName());

    Preconditions.checkArgument(basicVertexInfo != null,
        "VerteName " + vertexInfo.getVertexName()
            + " not present in DAG's vertices " + basicVertexInfoMap.entrySet());

    //populate additional information in VertexInfo
    if (basicVertexInfo.additionalInputs != null) {
      vertexInfo.setAdditionalInputInfoList(Arrays.asList(basicVertexInfo.additionalInputs));
    }
    if (basicVertexInfo.additionalOutputs != null) {
      vertexInfo.setAdditionalOutputInfoList(Arrays.asList(basicVertexInfo.additionalOutputs));
    }

    //Populate edge information in vertex
    if (basicVertexInfo.inEdgeIds != null) {
      for (String edge : basicVertexInfo.inEdgeIds) {
        EdgeInfo edgeInfo = edgeInfoMap.get(Integer.parseInt(edge));
        Preconditions.checkState(edgeInfo != null, "EdgeId " + edge + " not present in DAG");
        vertexInfo.addInEdge(edgeInfo);
      }
    }

    if (basicVertexInfo.outEdgeIds != null) {
      for (String edge : basicVertexInfo.outEdgeIds) {
        EdgeInfo edgeInfo = edgeInfoMap.get(Integer.parseInt(edge));
        Preconditions.checkState(edgeInfo != null, "EdgeId " + edge + " not present in DAG");
        vertexInfo.addOutEdge(edgeInfo);
      }
    }

    vertexNameMap.put(vertexInfo.getVertexName(), vertexInfo);
  }

  void setVersionInfo(VersionInfo versionInfo) {
    this.versionInfo = versionInfo;
  }

  void addContainerMapping(Container container, TaskAttemptInfo taskAttemptInfo) {
    this.containerMapping.put(container, taskAttemptInfo);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("dagID=").append(getDagId()).append(", ");
    sb.append("dagName=").append(getName()).append(", ");
    sb.append("status=").append(getStatus()).append(", ");
    sb.append("startTime=").append(getStartTimeInterval()).append(", ");
    sb.append("submitTime=").append(getSubmitTime()).append(", ");
    sb.append("endTime=").append(getFinishTimeInterval()).append(", ");
    sb.append("timeTaken=").append(getTimeTaken()).append(", ");
    sb.append("diagnostics=").append(getDiagnostics()).append(", ");
    sb.append("vertexNameIDMapping=").append(getVertexNameIDMapping()).append(", ");
    sb.append("failedTasks=").append(getFailedTaskCount()).append(", ");
    sb.append("events=").append(getEvents()).append(", ");
    sb.append("status=").append(getStatus());
    sb.append("]");
    return sb.toString();
  }

  public Multimap<Container, TaskAttemptInfo> getContainerMapping() {
    return Multimaps.unmodifiableMultimap(containerMapping);
  }

  public final VersionInfo getVersionInfo() {
    return versionInfo;
  }

  public final CallerContext getCallerContext() {
    return callerContext;
  }

  public final String getName() {
    return name;
  }

  public final Collection<EdgeInfo> getEdges() {
    return Collections.unmodifiableCollection(edgeInfoMap.values());
  }

  public final long getSubmitTime() {
    return submitTime;
  }

  public final long getStartTime() {
    return startTime;
  }

  public final long getFinishTime() {
    return endTime;
  }

  /**
   * Reference start time for the DAG. Vertex, Task, TaskAttempt would map on to this.
   * If absolute start time is needed, call getAbsStartTime().
   *
   * @return starting time w.r.t to dag
   */
  public final long getStartTimeInterval() {
    return 0;
  }

  @Override
  public final long getFinishTimeInterval() {
    long dagEndTime = (endTime - startTime);
    if (dagEndTime < 0) {
      //probably dag is not complete or failed in middle. get the last task attempt time
      for (VertexInfo vertexInfo : getVertices()) {
        dagEndTime = (vertexInfo.getFinishTimeInterval() > dagEndTime) ? vertexInfo.getFinishTimeInterval() : dagEndTime;
      }
    }
    return dagEndTime;
  }

  public final long getTimeTaken() {
    return getFinishTimeInterval();
  }

  public final String getStatus() {
    return status;
  }

  /**
   * Get vertexInfo for a given vertexid
   *
   * @param vertexId
   * @return VertexInfo
   */
  public VertexInfo getVertexFromId(String vertexId) {
    return vertexNameMap.get(vertexNameIDMapping.getKey(vertexId));
  }

  /**
   * Get vertexInfo for a given vertex name
   *
   * @param vertexName
   * @return VertexInfo
   */
  public final VertexInfo getVertex(String vertexName) {
    return vertexNameMap.get(vertexName);
  }

  public final String getDiagnostics() {
    return diagnostics;
  }

  /**
   * Get all vertices
   *
   * @return List<VertexInfo>
   */
  public final List<VertexInfo> getVertices() {
    List<VertexInfo> vertices = Lists.newLinkedList(vertexNameMap.values());
    Collections.sort(vertices, new Comparator<VertexInfo>() {

      @Override public int compare(VertexInfo o1, VertexInfo o2) {
        return (o1.getStartTimeInterval() < o2.getStartTimeInterval()) ? -1 :
            ((o1.getStartTimeInterval() == o2.getStartTimeInterval()) ?
                0 : 1);
      }
    });
    return Collections.unmodifiableList(vertices);
  }

  /**
   * Get list of failed vertices
   *
   * @return List<VertexInfo>
   */
  public final List<VertexInfo> getFailedVertices() {
    return getVertices(VertexState.FAILED);
  }

  /**
   * Get list of killed vertices
   *
   * @return List<VertexInfo>
   */
  public final List<VertexInfo> getKilledVertices() {
    return getVertices(VertexState.KILLED);
  }

  /**
   * Get list of failed vertices
   *
   * @return List<VertexInfo>
   */
  public final List<VertexInfo> getSuccessfullVertices() {
    return getVertices(VertexState.SUCCEEDED);
  }

  /**
   * Get list of vertices belonging to a specific state
   *
   * @param state
   * @return Collection<VertexInfo>
   */
  public final List<VertexInfo> getVertices(final VertexState state) {
    return Collections.unmodifiableList(Lists.newLinkedList(Iterables.filter(Lists.newLinkedList
                    (vertexNameMap.values()), new Predicate<VertexInfo>() {
                  @Override public boolean apply(VertexInfo input) {
                    return input.getStatus() != null && input.getStatus().equals(state.toString());
                  }
                }
            )
        )
    );
  }

  public final Map<String, VertexInfo> getVertexMapping() {
    return Collections.unmodifiableMap(vertexNameMap);
  }

  private Ordering<VertexInfo> getVertexOrdering() {
    return Ordering.from(new Comparator<VertexInfo>() {
      @Override public int compare(VertexInfo o1, VertexInfo o2) {
        return (o1.getTimeTaken() < o2.getTimeTaken()) ? -1 :
            ((o1.getTimeTaken() == o2.getTimeTaken()) ?
                0 : 1);
      }
    });
  }

  /**
   * Get the slowest vertex in the DAG
   *
   * @return VertexInfo
   */
  public final VertexInfo getSlowestVertex() {
    List<VertexInfo> vertexInfoList = getVertices();
    if (vertexInfoList.size() == 0) {
      return null;
    }
    return getVertexOrdering().max(vertexInfoList);
  }

  /**
   * Get the slowest vertex in the DAG
   *
   * @return VertexInfo
   */
  public final VertexInfo getFastestVertex() {
    List<VertexInfo> vertexInfoList = getVertices();
    if (vertexInfoList.size() == 0) {
      return null;
    }
    return getVertexOrdering().min(vertexInfoList);
  }

  /**
   * Get node details for this DAG. Would be useful for analyzing node to tasks.
   *
   * @return Multimap<String, TaskAttemptInfo> taskAttempt details at every node
   */
  public final Multimap<String, TaskAttemptInfo> getNodeDetails() {
    Multimap<String, TaskAttemptInfo> nodeDetails = LinkedListMultimap.create();
    for (VertexInfo vertexInfo : getVertices()) {
      Multimap<Container, TaskAttemptInfo> containerMapping = vertexInfo.getContainersMapping();
      for (Map.Entry<Container, TaskAttemptInfo> entry : containerMapping.entries()) {
        nodeDetails.put(entry.getKey().getHost(), entry.getValue());
      }
    }
    return nodeDetails;
  }

  /**
   * Get containers used for this DAG
   *
   * @return Multimap<Container, TaskAttemptInfo> task attempt details at every container
   */
  public final Multimap<Container, TaskAttemptInfo> getContainersToTaskAttemptMapping() {
    List<VertexInfo> VertexInfoList = getVertices();
    Multimap<Container, TaskAttemptInfo> containerMapping = LinkedHashMultimap.create();

    for (VertexInfo vertexInfo : VertexInfoList) {
      containerMapping.putAll(vertexInfo.getContainersMapping());
    }
    return Multimaps.unmodifiableMultimap(containerMapping);
  }

  public final Map getVertexNameIDMapping() {
    return vertexNameIDMapping;
  }

  public final int getNumVertices() {
    return numVertices;
  }

  public final String getDagId() {
    return dagId;
  }

  public final int getFailedTaskCount() {
    return failedTasks;
  }

}
