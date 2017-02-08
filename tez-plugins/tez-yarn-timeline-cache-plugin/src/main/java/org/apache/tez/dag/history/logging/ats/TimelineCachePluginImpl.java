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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineEntityGroupPlugin;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class TimelineCachePluginImpl extends TimelineEntityGroupPlugin implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(TimelineCachePluginImpl.class);

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

  private Configuration conf;

  private Set<Integer> allNumGroupsPerDag;

  // Empty public constructor
  public TimelineCachePluginImpl() {
    setConf(new TezConfiguration());
  }

  private Set<TimelineEntityGroupId> createTimelineEntityGroupIds(TezDAGID dagId) {
    ApplicationId appId = dagId.getApplicationId();
    HashSet<TimelineEntityGroupId> groupIds = Sets.newHashSet(
        TimelineEntityGroupId.newInstance(appId, dagId.toString()));
    for (int numGroupsPerDag : allNumGroupsPerDag) {
      groupIds.add(TimelineEntityGroupId.newInstance(appId, dagId.getGroupId(numGroupsPerDag)));
    }
    return groupIds;
  }

  private Set<TimelineEntityGroupId> convertToTimelineEntityGroupIds(String entityType, String entityId) {
    if (entityType == null || entityType.isEmpty()
        || entityId == null || entityId.isEmpty()) {
      return null;
    }
    if (entityType.equals(EntityTypes.TEZ_DAG_ID.name())) {
      TezDAGID dagId = TezDAGID.fromString(entityId);
      if (dagId != null) {
        return createTimelineEntityGroupIds(dagId);
      }
    } else if (entityType.equals(EntityTypes.TEZ_VERTEX_ID.name())) {
      TezVertexID vertexID = TezVertexID.fromString(entityId);
      if (vertexID != null) {
        return createTimelineEntityGroupIds(vertexID.getDAGId());
      }

    } else if (entityType.equals(EntityTypes.TEZ_TASK_ID.name())) {
      TezTaskID taskID = TezTaskID.fromString(entityId);
      if (taskID != null) {
        return createTimelineEntityGroupIds(taskID.getVertexID().getDAGId());
      }
    } else if (entityType.equals(EntityTypes.TEZ_TASK_ATTEMPT_ID.name())) {
      TezTaskAttemptID taskAttemptID = TezTaskAttemptID.fromString(entityId);
      if (taskAttemptID != null) {
        return createTimelineEntityGroupIds(taskAttemptID.getTaskID().getVertexID().getDAGId());
      }
    } else if (entityType.equals(EntityTypes.TEZ_CONTAINER_ID.name())) {
      String cId = entityId;
      if (cId.startsWith("tez_")) {
        cId = cId.substring(4);
      }
      ContainerId containerId = ContainerId.fromString(cId);
      if (containerId != null) {
        return Sets.newHashSet(TimelineEntityGroupId.newInstance(
            containerId.getApplicationAttemptId().getApplicationId(),
            containerId.getApplicationAttemptId().getApplicationId().toString()));
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
    return convertToTimelineEntityGroupIds(primaryFilter.getName(), primaryFilter.getValue().toString());
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityId, String entityType) {
    if (!knownEntityTypes.contains(entityType) || summaryEntityTypes.contains(entityType)) {
      return null;
    }
    return convertToTimelineEntityGroupIds(entityType, entityId);
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
    for (String entityId : entityIds) {
      Set<TimelineEntityGroupId> groupId = convertToTimelineEntityGroupIds(entityType, entityId);
      if (groupId != null) {
        groupIds.addAll(groupId);
      }
    }
    return groupIds;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf instanceof TezConfiguration ? conf : new TezConfiguration(conf);

    this.allNumGroupsPerDag = loadAllNumDagsPerGroup();
  }

  private Set<Integer> loadAllNumDagsPerGroup() {
    Set<Integer> allNumDagsPerGroup = new HashSet<Integer>();

    int numDagsPerGroup = conf.getInt(TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_NUM_DAGS_PER_GROUP,
        TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_NUM_DAGS_PER_GROUP_DEFAULT);
    if (numDagsPerGroup > 1) {
      // Add current numDagsPerGroup from config.
      allNumDagsPerGroup.add(numDagsPerGroup);
    }

    // Add the older values from config.
    int [] usedNumGroups = conf.getInts(TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_CACHE_PLUGIN_OLD_NUM_DAGS_PER_GROUP);
    if (usedNumGroups != null) {
      for (int i = 0; i < usedNumGroups.length; ++i) {
        allNumDagsPerGroup.add(usedNumGroups[i]);
      }
    }

    // Warn for performance impact
    if (allNumDagsPerGroup.size() > 3) {
      LOG.warn("Too many entries in " + TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_CACHE_PLUGIN_OLD_NUM_DAGS_PER_GROUP +
          ", this can result in slower lookup from Yarn Timeline server or slower load times in TezUI.");
    }
    return allNumDagsPerGroup;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
