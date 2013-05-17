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

package org.apache.tez.dag.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// based on TestDAGLocationHint
public class TestDAGPlan {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(); //TODO: doesn't seem to be deleting this folder automatically as expected.

  @Test
  public void testBasicJobPlanSerde() throws IOException {

    DAGPlan job = DAGPlan.newBuilder()
       .setName("test")
       .addVertex(
           VertexPlan.newBuilder()
             .setName("vertex1")
             .setType(PlanVertexType.NORMAL)
             .addTaskLocationHint(PlanTaskLocationHint.newBuilder().addHost("machineName").addRack("rack1").build())
             .setTaskConfig(
                 PlanTaskConfiguration.newBuilder()
                   .setNumTasks(2)
                   .setVirtualCores(4)
                   .setMemoryMb(1024)
                   .setJavaOpts("")
                   .setTaskModule("x.y")
                   .build())
             .build())
        .build();
   File file = tempFolder.newFile("jobPlan");
   FileOutputStream outStream = null;
   try {
     outStream = new FileOutputStream(file);
     job.writeTo(outStream); 
   }
   finally {
     if(outStream != null){
       outStream.close();  
     }
   }

   DAGPlan inJob;
   FileInputStream inputStream;
   try {
     inputStream = new FileInputStream(file);
     inJob = DAGPlan.newBuilder().mergeFrom(inputStream).build();
   }
   finally {
     outStream.close();  
   }

   Assert.assertEquals(job, inJob);
  }
}
