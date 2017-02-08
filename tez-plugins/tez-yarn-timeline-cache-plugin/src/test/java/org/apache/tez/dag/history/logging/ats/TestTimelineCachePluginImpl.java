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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;


public class TestTimelineCachePluginImpl {

  static ApplicationId appId1;
  static ApplicationAttemptId appAttemptId1;
  static ApplicationId appId2;
  static TezDAGID dagID1;
  static TezVertexID vertexID1;
  static TezTaskID taskID1;
  static TezTaskAttemptID attemptID1;
  static TezDAGID dagID2;
  static TezVertexID vertexID2;
  static TezTaskID taskID2;
  static TezTaskAttemptID attemptID2;
  static ContainerId cId1;
  static ContainerId cId2;
  static Map<String, String> typeIdMap1;
  static Map<String, String> typeIdMap2;

  private static TimelineCachePluginImpl createPlugin(int numDagsPerGroup, String usedNumDagsPerGroup) {
    Configuration conf = new Configuration(false);
    if (numDagsPerGroup > 0) {
      conf.setInt(TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_NUM_DAGS_PER_GROUP, numDagsPerGroup);
    }
    if (usedNumDagsPerGroup != null) {
      conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_CACHE_PLUGIN_OLD_NUM_DAGS_PER_GROUP, usedNumDagsPerGroup);
    }
    if (numDagsPerGroup > 0 || usedNumDagsPerGroup != null) {
      return ReflectionUtils.newInstance(TimelineCachePluginImpl.class, conf);
    } else {
      return new TimelineCachePluginImpl();
    }
  }

  @BeforeClass
  public static void beforeClass() {
    appId1 = ApplicationId.newInstance(1000l, 111);
    appId2 = ApplicationId.newInstance(1001l, 121);
    appAttemptId1 = ApplicationAttemptId.newInstance(appId1, 11);

    dagID1 = TezDAGID.getInstance(appId1, 1);
    vertexID1 = TezVertexID.getInstance(dagID1, 12);
    taskID1 = TezTaskID.getInstance(vertexID1, 11144);
    attemptID1 = TezTaskAttemptID.getInstance(taskID1, 4);

    dagID2 = TezDAGID.getInstance(appId2, 111);
    vertexID2 = TezVertexID.getInstance(dagID2, 121);
    taskID2 = TezTaskID.getInstance(vertexID2, 113344);
    attemptID2 = TezTaskAttemptID.getInstance(taskID2, 14);

    cId1 = ContainerId.newContainerId(appAttemptId1, 1);
    cId2 = ContainerId.newContainerId(ApplicationAttemptId.newInstance(appId2, 1), 22);

    typeIdMap1 = new HashMap<String, String>();
    typeIdMap1.put(EntityTypes.TEZ_DAG_ID.name(), dagID1.toString());
    typeIdMap1.put(EntityTypes.TEZ_VERTEX_ID.name(), vertexID1.toString());
    typeIdMap1.put(EntityTypes.TEZ_TASK_ID.name(), taskID1.toString());
    typeIdMap1.put(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), attemptID1.toString());

    typeIdMap2 = new HashMap<String, String>();
    typeIdMap2.put(EntityTypes.TEZ_DAG_ID.name(), dagID2.toString());
    typeIdMap2.put(EntityTypes.TEZ_VERTEX_ID.name(), vertexID2.toString());
    typeIdMap2.put(EntityTypes.TEZ_TASK_ID.name(), taskID2.toString());
    typeIdMap2.put(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), attemptID2.toString());
  }

  @Test
  public void testGetTimelineEntityGroupIdByPrimaryFilter() {
    TimelineCachePluginImpl plugin = createPlugin(100, null);
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      NameValuePair primaryFilter = new NameValuePair(entry.getKey(), entry.getValue());
      Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_APPLICATION.name(),
          primaryFilter, null));
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getKey(), primaryFilter, null);
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(2, groupIds.size());
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        Assert.assertEquals(appId1, groupId.getApplicationId());
        Assert.assertTrue(getGroupIds(dagID1, 100).contains(groupId.getTimelineEntityGroupId()));
      }
    }
  }

  @Test
  public void testGetTimelineEntityGroupIdByIdDefaultConfig() {
    TimelineCachePluginImpl plugin = createPlugin(-1, null);
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getValue(), entry.getKey());
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(1, groupIds.size());
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        Assert.assertEquals(appId1, groupId.getApplicationId());
        Assert.assertTrue(getGroupIds(dagID1).contains(groupId.getTimelineEntityGroupId()));
      }
    }
  }

  @Test
  public void testGetTimelineEntityGroupIdByIdNoGroupingConf() {
    TimelineCachePluginImpl plugin = createPlugin(1, null);
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getValue(), entry.getKey());
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(1, groupIds.size());
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        Assert.assertEquals(appId1, groupId.getApplicationId());
        Assert.assertTrue(getGroupIds(dagID1).contains(groupId.getTimelineEntityGroupId()));
      }
    }
  }

  @Test
  public void testGetTimelineEntityGroupIdById() {
    TimelineCachePluginImpl plugin = createPlugin(100, null);
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getValue(), entry.getKey());
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(2, groupIds.size());
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        Assert.assertEquals(appId1, groupId.getApplicationId());
        Assert.assertTrue(getGroupIds(dagID1, 100).contains(groupId.getTimelineEntityGroupId()));
      }
    }
  }

  @Test
  public void testGetTimelineEntityGroupIdByIdWithOldGroupIdsSingle() {
    TimelineCachePluginImpl plugin = createPlugin(100, "50");
    for (Entry<String, String> entry : typeIdMap2.entrySet()) {
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getValue(), entry.getKey());
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(3, groupIds.size());
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        Assert.assertEquals(appId2, groupId.getApplicationId());
        Assert.assertTrue(getGroupIds(dagID2, 100, 50).contains(groupId.getTimelineEntityGroupId()));
      }
    }
  }

  @Test
  public void testGetTimelineEntityGroupIdByIdWithOldGroupIdsMultiple() {
    TimelineCachePluginImpl plugin = createPlugin(100, "25, 50");
    for (Entry<String, String> entry : typeIdMap2.entrySet()) {
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getValue(), entry.getKey());
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(4, groupIds.size());
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        Assert.assertEquals(appId2, groupId.getApplicationId());
        Assert.assertTrue(
            getGroupIds(dagID2, 100, 25, 50).contains(groupId.getTimelineEntityGroupId()));
      }
    }
  }

  @Test
  public void testGetTimelineEntityGroupIdByIdWithOldGroupIdsEmpty() {
    TimelineCachePluginImpl plugin = createPlugin(100, "");
    for (Entry<String, String> entry : typeIdMap2.entrySet()) {
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getValue(), entry.getKey());
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(2, groupIds.size());
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        Assert.assertEquals(appId2, groupId.getApplicationId());
        Assert.assertTrue(getGroupIds(dagID2, 100).contains(groupId.getTimelineEntityGroupId()));
      }
    }
  }

  @Test
  public void testGetTimelineEntityGroupIdByIds() {
    TimelineCachePluginImpl plugin = createPlugin(100, null);
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      SortedSet<String> entityIds = new TreeSet<String>();
      entityIds.add(entry.getValue());
      entityIds.add(typeIdMap2.get(entry.getKey()));
      Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entry.getKey(),
          entityIds, null);
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(groupIds);
        continue;
      }
      Assert.assertEquals(4, groupIds.size());
      int found = 0;
      Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
      while (iter.hasNext()) {
        TimelineEntityGroupId groupId = iter.next();
        if (groupId.getApplicationId().equals(appId1)) {
          String entityGroupId = groupId.getTimelineEntityGroupId();
          if (getGroupIds(dagID1, 100).contains(entityGroupId)) {
            ++found;
          } else {
            Assert.fail("Unexpected group id: " + entityGroupId);
          }
        } else if (groupId.getApplicationId().equals(appId2)) {
          String entityGroupId = groupId.getTimelineEntityGroupId();
          if (getGroupIds(dagID2, 100).contains(entityGroupId)) {
            ++found;
          } else {
            Assert.fail("Unexpected group id: " + entityGroupId);
          }
        } else {
          Assert.fail("Unexpected appId: " + groupId.getApplicationId());
        }
      }
      Assert.assertEquals("All groupIds not returned", 4, found);
    }
  }

  @Test
  public void testInvalidIds() {
    TimelineCachePluginImpl plugin = createPlugin(-1, null);
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_DAG_ID.name(),
        vertexID1.toString()));
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_VERTEX_ID.name(),
        taskID1.toString()));
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_TASK_ID.name(),
        attemptID1.toString()));
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(),
        dagID1.toString()));
    Assert.assertNull(plugin.getTimelineEntityGroupId("", ""));
    Assert.assertNull(plugin.getTimelineEntityGroupId(null, null));
    Assert.assertNull(plugin.getTimelineEntityGroupId("adadasd", EntityTypes.TEZ_DAG_ID.name()));
  }

  @Test
  public void testInvalidTypeRequests() {
    TimelineCachePluginImpl plugin = createPlugin(-1, null);
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_APPLICATION.name(),
        appId1.toString()));
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        appAttemptId1.toString()));
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_CONTAINER_ID.name(),
        appId1.toString()));

    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_TASK_ID.name(), null,
        new HashSet<String>()));
    Assert.assertNull(plugin.getTimelineEntityGroupId(EntityTypes.TEZ_TASK_ID.name(), null,
        new HashSet<NameValuePair>()));

  }

  @Test
  public void testContainerIdConversion() {
    TimelineCachePluginImpl plugin = createPlugin(-1, null);
    String entityType = EntityTypes.TEZ_CONTAINER_ID.name();
    SortedSet<String> entityIds = new TreeSet<String>();
    entityIds.add("tez_" + cId1.toString());
    entityIds.add("tez_" + cId2.toString());
    Set<TimelineEntityGroupId> groupIds = plugin.getTimelineEntityGroupId(entityType,
        entityIds, null);
    Assert.assertEquals(2, groupIds.size());
    int found = 0;
    Iterator<TimelineEntityGroupId> iter = groupIds.iterator();
    while (iter.hasNext()) {
      TimelineEntityGroupId groupId = iter.next();
      if (groupId.getApplicationId().equals(appId1)
          && groupId.getTimelineEntityGroupId().equals(appId1.toString())) {
        ++found;
      } else if (groupId.getApplicationId().equals(appId2)
          && groupId.getTimelineEntityGroupId().equals(appId2.toString())) {
        ++found;
      }
    }
    Assert.assertEquals("All groupIds not returned", 2, found);

    groupIds.clear();
    groupIds = plugin.getTimelineEntityGroupId(cId1.toString(), entityType);
    Assert.assertEquals(1, groupIds.size());
    found = 0;
    iter = groupIds.iterator();
    while (iter.hasNext()) {
      TimelineEntityGroupId groupId = iter.next();
      if (groupId.getApplicationId().equals(appId1)
          && groupId.getTimelineEntityGroupId().equals(appId1.toString())) {
        ++found;
      }
    }
    Assert.assertEquals("All groupIds not returned", 1, found);

    groupIds.clear();
    groupIds = plugin.getTimelineEntityGroupId("tez_" + cId2.toString(), entityType);
    Assert.assertEquals(1, groupIds.size());
    found = 0;
    iter = groupIds.iterator();
    while (iter.hasNext()) {
      TimelineEntityGroupId groupId = iter.next();
      if (groupId.getApplicationId().equals(appId2)
          && groupId.getTimelineEntityGroupId().equals(appId2.toString())) {
        ++found;
      }
    }
    Assert.assertEquals("All groupIds not returned", 1, found);
  }

  private Set<String> getGroupIds(TezDAGID dagId, int ... allNumDagsPerGroup) {
    HashSet<String> groupIds = Sets.newHashSet(dagId.toString());
    for (int numDagsPerGroup : allNumDagsPerGroup) {
      groupIds.add(dagId.getGroupId(numDagsPerGroup));
    }
    return groupIds;
  }
}
