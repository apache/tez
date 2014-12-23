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

package org.apache.tez.dag.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.app.RecoveryParser.DAGSummaryData;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestRecoveryParser {

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestRecoveryParser.class.getName() + "-tmpDir";

  private RecoveryParser parser;

  @Before
  public void setUp() throws IllegalArgumentException, IOException {
    DAGAppMaster mockAppMaster = mock(DAGAppMaster.class);
    when(mockAppMaster.getConfig()).thenReturn(new Configuration());
    parser =
        new RecoveryParser(mockAppMaster, mock(FileSystem.class), new Path(
            TEST_ROOT_DIR), 3);
  }

  private DAGSummaryData createDAGSummaryData(TezDAGID dagId, boolean completed) {
    DAGSummaryData data = new DAGSummaryData(dagId);
    data.completed = completed;
    return data;
  }

  @Test(timeout = 5000)
  public void testGetLastCompletedDAG() {
    Map<TezDAGID, DAGSummaryData> summaryDataMap =
        new HashMap<TezDAGID, DAGSummaryData>();
    int lastCompletedDAGId = new Random().nextInt(20) + 1;
    for (int i = 1; i <= lastCompletedDAGId; ++i) {
      ApplicationId appId = ApplicationId.newInstance(1, 1);
      TezDAGID dagId = TezDAGID.getInstance(appId, i);
      summaryDataMap.put(dagId, createDAGSummaryData(dagId, true));
    }

    DAGSummaryData lastCompletedDAG =
        parser.getLastCompletedOrInProgressDAG(summaryDataMap);
    assertEquals(lastCompletedDAGId, lastCompletedDAG.dagId.getId());
  }

  @Test(timeout = 5000)
  public void testGetLastInProgressDAG() {
    Map<TezDAGID, DAGSummaryData> summaryDataMap =
        new HashMap<TezDAGID, DAGSummaryData>();
    int dagNum = 20;
    int lastInProgressDAGId = new Random().nextInt(dagNum) + 1;
    for (int i = 1; i <= dagNum; ++i) {
      ApplicationId appId = ApplicationId.newInstance(1, 1);
      TezDAGID dagId = TezDAGID.getInstance(appId, i);
      if (i == lastInProgressDAGId) {
        summaryDataMap.put(dagId, createDAGSummaryData(dagId, false));
      } else {
        summaryDataMap.put(dagId, createDAGSummaryData(dagId, true));
      }
    }

    DAGSummaryData lastInProgressDAG =
        parser.getLastCompletedOrInProgressDAG(summaryDataMap);
    assertEquals(lastInProgressDAGId, lastInProgressDAG.dagId.getId());
  }
}
