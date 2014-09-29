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

package org.apache.tez.mapreduce.examples;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.util.ProgramDriver;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;

/**
 * A description of an example program based on its class and a
 * human-readable description.
 */
public class ExampleDriver {

  private static final DecimalFormat formatter = new DecimalFormat("###.##%");

  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("rpcloadgen", RPCLoadGen.class, "Run a DAG to generate load for the task to AM RPC");
      pgd.addClass("wordcount", MapredWordCount.class,
          "A map/reduce program that counts the words in the input files.");
      pgd.addClass("mapredwordcount", MapredWordCount.class,
          "A map/reduce program that counts the words in the input files"
         + " using the mapred apis.");
      pgd.addClass("randomwriter", RandomWriter.class,
          "A map/reduce program that writes 10GB of random data per node.");
      pgd.addClass("randomtextwriter", RandomTextWriter.class,
      "A map/reduce program that writes 10GB of random textual data per node.");
      pgd.addClass("sort", Sort.class,
          "A map/reduce program that sorts the data written by the random"
          + " writer.");
      pgd.addClass("secondarysort", SecondarySort.class,
          "An example defining a secondary sort to the reduce.");
      pgd.addClass("join", Join.class,
          "A job that effects a join over sorted, equally partitioned"
          + " datasets");
      pgd.addClass("groupbyorderbymrrtest", GroupByOrderByMRRTest.class,
          "A map-reduce-reduce program that does groupby-order by. Takes input"
          + " containing employee_name department name per line of input"
          + " and generates count of employees per department and"
          + " sorted on employee count");
      pgd.addClass("mrrsleep", MRRSleepJob.class,
          "MRR Sleep Job");
      pgd.addClass("testorderedwordcount", TestOrderedWordCount.class,
          "Word Count with words sorted on frequency");
      pgd.addClass("unionexample", UnionExample.class,
          "Union example");
      pgd.addClass("broadcastAndOneToOneExample", BroadcastAndOneToOneExample.class,
          "BroadcastAndOneToOneExample example");
      pgd.addClass("filterLinesByWord", FilterLinesByWord.class,
          "Filters lines by the specified word using broadcast edge");
      pgd.addClass("filterLinesByWordOneToOne", FilterLinesByWordOneToOne.class,
          "Filters lines by the specified word using OneToOne edge");
      exitCode = pgd.run(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }

    System.exit(exitCode);
  }

  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames)
      throws IOException, TezException {
    printDAGStatus(dagClient, vertexNames, false, false);
  }

  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames,
      boolean displayDAGCounters, boolean displayVertexCounters)
      throws IOException, TezException {
    Set<StatusGetOpts> opts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
    DAGStatus dagStatus = dagClient.getDAGStatus(
      (displayDAGCounters ? opts : null));
    Progress progress = dagStatus.getDAGProgress();
    double vProgressFloat = 0.0f;
    if (progress != null) {
      System.out.println("");
      System.out.println("DAG: State: "
          + dagStatus.getState()
          + " Progress: "
          + (progress.getTotalTaskCount() < 0 ? formatter.format(0.0f) :
            formatter.format((double)(progress.getSucceededTaskCount())
              /progress.getTotalTaskCount())));
      for (String vertexName : vertexNames) {
        VertexStatus vStatus = dagClient.getVertexStatus(vertexName,
          (displayVertexCounters ? opts : null));
        if (vStatus == null) {
          System.out.println("Could not retrieve status for vertex: "
            + vertexName);
          continue;
        }
        Progress vProgress = vStatus.getProgress();
        if (vProgress != null) {
          vProgressFloat = 0.0f;
          if (vProgress.getTotalTaskCount() == 0) {
            vProgressFloat = 1.0f;
          } else if (vProgress.getTotalTaskCount() > 0) {
            vProgressFloat = (double)vProgress.getSucceededTaskCount()
              /vProgress.getTotalTaskCount();
          }
          System.out.println("VertexStatus:"
              + " VertexName: "
              + (vertexName.equals("ivertex1") ? "intermediate-reducer"
                  : vertexName)
              + " Progress: " + formatter.format(vProgressFloat));
        }
        if (displayVertexCounters) {
          TezCounters counters = vStatus.getVertexCounters();
          if (counters != null) {
            System.out.println("Vertex Counters for " + vertexName + ": "
              + counters);
          }
        }
      }
    }
    if (displayDAGCounters) {
      TezCounters counters = dagStatus.getDAGCounters();
      if (counters != null) {
        System.out.println("DAG Counters: " + counters);
      }
    }
  }

}
	
