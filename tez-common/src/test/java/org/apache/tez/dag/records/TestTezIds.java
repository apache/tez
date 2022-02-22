/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.records;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.junit.Test;

public class TestTezIds {

  private static final Logger LOG = LoggerFactory.getLogger(TestTezIds.class);

  private void verifyDagInfo(String[] splits, TezDAGID dagId) {
    Assert.assertEquals(dagId.getApplicationId().getClusterTimestamp(),
        Long.valueOf(splits[1]).longValue());
    Assert.assertEquals(dagId.getApplicationId().getId(),
        Integer.valueOf(splits[2]).intValue());
    Assert.assertEquals(dagId.getId(),
        Integer.valueOf(splits[3]).intValue());
  }

  private void verifyVertexInfo(String[] splits, TezVertexID vId) {
    verifyDagInfo(splits, vId.getDAGID());
    Assert.assertEquals(vId.getId(),
        Integer.valueOf(splits[4]).intValue());
  }

  private void verifyTaskInfo(String[] splits, TezTaskID tId) {
    verifyVertexInfo(splits, tId.getVertexID());
    Assert.assertEquals(tId.getId(),
        Integer.valueOf(splits[5]).intValue());
  }

  private void verifyAttemptInfo(String[] splits, TezTaskAttemptID taId) {
    verifyTaskInfo(splits, taId.getTaskID());
    Assert.assertEquals(taId.getId(),
        Integer.valueOf(splits[6]).intValue());
  }

  private void verifyDagId(String dagIdStr, TezDAGID dagId) {
    String[] splits = dagIdStr.split("_");
    Assert.assertEquals(4, splits.length);
    verifyDagInfo(splits, dagId);
  }

  private void verifyVertexId(String vIdStr, TezVertexID vId) {
    String[] splits = vIdStr.split("_");
    Assert.assertEquals(5, splits.length);
    verifyVertexInfo(splits, vId);
  }

  private void verifyTaskId(String tIdStr, TezTaskID tId) {
    String[] splits = tIdStr.split("_");
    Assert.assertEquals(6, splits.length);
    verifyTaskInfo(splits, tId);
  }

  private void verifyAttemptId(String taIdStr, TezTaskAttemptID taId) {
    String[] splits = taIdStr.split("_");
    Assert.assertEquals(7, splits.length);
    verifyAttemptInfo(splits, taId);
  }

  @Test(timeout = 5000)
  public void testIdStringify() {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(tId, 2);

    String dagIdStr = dagId.toString();
    String vIdStr = vId.toString();
    String tIdStr = tId.toString();
    String taIdStr = taId.toString();

    LOG.info("DAG ID:" + dagIdStr);
    LOG.info("Vertex ID:" + vIdStr);
    LOG.info("Task ID:" + tIdStr);
    LOG.info("Attempt ID:" + taIdStr);

    Assert.assertTrue(dagIdStr.startsWith("dag"));
    Assert.assertTrue(vIdStr.startsWith("vertex"));
    Assert.assertTrue(tIdStr.startsWith("task"));
    Assert.assertTrue(taIdStr.startsWith("attempt"));

    verifyDagId(dagIdStr, dagId);
    verifyVertexId(vIdStr, vId);
    verifyTaskId(tIdStr, tId);
    verifyAttemptId(taIdStr, taId);
  }

  @Test(timeout=5000)
  public void testInvalidDagIds() {
    String dagIdStr = "aaa_111_1_1";
    TezDAGID dagId;
    try {
      dagId = TezDAGID.fromString(dagIdStr);
      Assert.fail("Expected failure for invalid dagId=" + dagIdStr);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid DAG Id format"));
    }

    dagIdStr = "dag_111_11";
    try {
      dagId = TezDAGID.fromString(dagIdStr);
      Assert.fail("Expected failure for invalid dagId=" + dagIdStr);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid DAG Id format"));
    }

    dagIdStr = "dag_111_11_aa";
    try {
      dagId = TezDAGID.fromString(dagIdStr);
      Assert.fail("Expected failure for invalid dagId=" + dagIdStr);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Error while parsing"));
    }

    dagIdStr = "dag_111_aa_1";
    try {
      dagId = TezDAGID.fromString(dagIdStr);
      Assert.fail("Expected failure for invalid dagId=" + dagIdStr);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Error while parsing"));
    }

  }


  public void testGetGroupIds() {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    int numDagsPerGroup = 5;
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);

    // All dags within one group should have same id.
    String groupId1 = dagId.getGroupId(numDagsPerGroup);
    for (int i = 0; i < numDagsPerGroup; ++i) {
      Assert.assertEquals(TezDAGID.getInstance(appId, i + 1).getGroupId(numDagsPerGroup), groupId1);
    }

    // Assert different id across groups.
    Assert.assertNotEquals(
        TezDAGID.getInstance(appId, numDagsPerGroup + 1).getGroupId(numDagsPerGroup), groupId1);

    // Invalid values -1, 0, 1 should throw IllegalArgumentException.
    for (int i = -1; i < 2; ++i) {
      try {
        dagId.getGroupId(i);
        Assert.fail("Expected IllegalArgumentException for numDagsPerGroup: " + i);
      } catch (IllegalArgumentException e) {
      }
    }
  }

}
