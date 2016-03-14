/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.ats;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineEntityGroupPlugin;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

import com.google.common.collect.Sets;

public class TimelineCachePluginImpl extends TimelineEntityGroupPlugin {

  private static Set<String> summaryEntityTypes;
  private static Set<String> knownEntityTypes;

  static {
    knownEntityTypes = Sets.newHashSet(
        EntityTypes.TEZ_DAG_ID.name(),
        EntityTypes.TEZ_VERTEX_ID.name(),
        EntityTypes.TEZ_TASK_ID.name(),
        EntityTypes.TEZ_TASK_ATTEMPT_ID.name(),
        EntityTypes.TEZ_CONTAINER_ID.name());
    summaryEntityTypes = Sets.newHashSet(
        EntityTypes.TEZ_DAG_ID.name(),
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        EntityTypes.TEZ_APPLICATION.name());
  }

  // Empty public constructor
  public TimelineCachePluginImpl() {
  }

  private TimelineEntityGroupId convertToTimelineEntityGroupId(String entityType, String entityId) {
    if (entityType == null || entityType.isEmpty()
        || entityId == null || entityId.isEmpty()) {
      return null;
    }
    if (entityType.equals(EntityTypes.TEZ_DAG_ID.name())) {
      TezDAGID dagId = TezDAGID.fromString(entityId);
      if (dagId != null) {
        return TimelineEntityGroupId.newInstance(dagId.getApplicationId(), dagId.toString());
      }
    } else if (entityType.equals(EntityTypes.TEZ_VERTEX_ID.name())) {
      TezVertexID vertexID = TezVertexID.fromString(entityId);
      if (vertexID != null) {
        return TimelineEntityGroupId.newInstance(vertexID.getDAGId().getApplicationId(),
            vertexID.getDAGId().toString());
      }

    } else if (entityType.equals(EntityTypes.TEZ_TASK_ID.name())) {
      TezTaskID taskID = TezTaskID.fromString(entityId);
      if (taskID != null) {
        return TimelineEntityGroupId.newInstance(taskID.getVertexID().getDAGId().getApplicationId(),
            taskID.getVertexID().getDAGId().toString());
      }
    } else if (entityType.equals(EntityTypes.TEZ_TASK_ATTEMPT_ID.name())) {
      TezTaskAttemptID taskAttemptID = TezTaskAttemptID.fromString(entityId);
      if (taskAttemptID != null) {
        return TimelineEntityGroupId.newInstance(
            taskAttemptID.getTaskID().getVertexID().getDAGId().getApplicationId(),
            taskAttemptID.getTaskID().getVertexID().getDAGId().toString());
      }
    } else if (entityType.equals(EntityTypes.TEZ_CONTAINER_ID.name())) {
      String cId = entityId;
      if (cId.startsWith("tez_")) {
        cId = cId.substring(4);
      }
      ContainerId containerId = ContainerId.fromString(cId);
      if (containerId != null) {
        return TimelineEntityGroupId.newInstance(
            containerId.getApplicationAttemptId().getApplicationId(),
            containerId.getApplicationAttemptId().getApplicationId().toString());
      }
    }
    return null;
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilters) {
    if (!knownEntityTypes.contains(entityType)
        || primaryFilter == null
        || !knownEntityTypes.contains(primaryFilter.getName())
        || summaryEntityTypes.contains(entityType)) {
      return null;
    }
    TimelineEntityGroupId groupId = convertToTimelineEntityGroupId(primaryFilter.getName(),
        primaryFilter.getValue().toString());
    if (groupId != null) {
      TimelineEntityGroupId appGroupId =
          TimelineEntityGroupId.newInstance(groupId.getApplicationId(),
              groupId.getApplicationId().toString());
      return Sets.newHashSet(groupId, appGroupId);
    }
    return null;
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityId, String entityType) {
    if (!knownEntityTypes.contains(entityType) || summaryEntityTypes.contains(entityType)) {
      return null;
    }
    TimelineEntityGroupId groupId = convertToTimelineEntityGroupId(entityType, entityId);
    if (groupId != null) {
      TimelineEntityGroupId appGroupId =
          TimelineEntityGroupId.newInstance(groupId.getApplicationId(),
              groupId.getApplicationId().toString());
      return Sets.newHashSet(groupId, appGroupId);
    }
    return null;
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      SortedSet<String> entityIds, Set<String> eventTypes) {
    if (!knownEntityTypes.contains(entityType)
        || summaryEntityTypes.contains(entityType)
        || entityIds == null || entityIds.isEmpty()) {
      return null;
    }
    Set<TimelineEntityGroupId> groupIds = new HashSet<TimelineEntityGroupId>();
    Set<ApplicationId> appIdSet = new HashSet<ApplicationId>();

    for (String entityId : entityIds) {
      TimelineEntityGroupId groupId = convertToTimelineEntityGroupId(entityType, entityId);
      if (groupId != null) {
        groupIds.add(groupId);
        appIdSet.add(groupId.getApplicationId());
      }
    }
    for (ApplicationId appId : appIdSet) {
      groupIds.add(TimelineEntityGroupId.newInstance(appId, appId.toString()));
    }
    return groupIds;
  }

}
