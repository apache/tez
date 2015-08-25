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

package org.apache.tez.analyzer.plugins;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.plugins.CriticalPathAnalyzer.CriticalPathStep.EntityType;
import org.apache.tez.analyzer.utils.SVGUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.history.parser.datamodel.Container;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CriticalPathAnalyzer extends TezAnalyzerBase implements Analyzer {

  String succeededState = StringInterner.weakIntern(TaskAttemptState.SUCCEEDED.name());
  String failedState = StringInterner.weakIntern(TaskAttemptState.FAILED.name());

  private final static String DATA_DEPENDENCY = "Data-Dependency";
  private final static String INIT_DEPENDENCY = "Init-Dependency";
  private final static String COMMIT_DEPENDENCY = "Commit-Dependency";
  private final static String NON_DATA_DEPENDENCY = "Non-Data-Dependency";
  private final static String OUTPUT_LOST = "Previous version outputs lost";

  public static class CriticalPathStep {
    public enum EntityType {
      ATTEMPT,
      VERTEX_INIT,
      DAG_COMMIT
    }

    EntityType type;
    TaskAttemptInfo attempt;
    String reason; // reason linking this to the previous step on the critical path
    long startCriticalPathTime; // time at which attempt is on critical path
    long stopCriticalPathTime; // time at which attempt is off critical path
    List<String> notes = Lists.newLinkedList();
    
    public CriticalPathStep(TaskAttemptInfo attempt, EntityType type) {
      this.type = type;
      this.attempt = attempt;
    }
    public EntityType getType() {
      return type;
    }
    public TaskAttemptInfo getAttempt() {
      return attempt;
    }
    public long getStartCriticalTime() {
      return startCriticalPathTime;
    }
    public long getStopCriticalTime() {
      return stopCriticalPathTime;
    }
    public String getReason() {
      return reason;
    }
    public List<String> getNotes() {
      return notes;
    }
  }
  
  List<CriticalPathStep> criticalPath = Lists.newLinkedList();
  
  Map<String, TaskAttemptInfo> attempts = Maps.newHashMap();

  public CriticalPathAnalyzer() {
  }

  @Override 
  public void analyze(DagInfo dagInfo) throws TezException {
    // get all attempts in the dag and find the last failed/succeeded attempt.
    // ignore killed attempt to handle kills that happen upon dag completion
    TaskAttemptInfo lastAttempt = null;
    long lastAttemptFinishTime = 0;
    for (VertexInfo vertex : dagInfo.getVertices()) {
      for (TaskAttemptInfo attempt : vertex.getTaskAttempts()) {
        attempts.put(attempt.getTaskAttemptId(), attempt);
        if (attempt.getStatus().equals(succeededState) ||
            attempt.getStatus().equals(failedState)) {
          if (lastAttemptFinishTime < attempt.getFinishTime()) {
            lastAttempt = attempt;
            lastAttemptFinishTime = attempt.getFinishTime();
          }
        }
      }
    }
    
    if (lastAttempt == null) {
      System.out.println("Cannot find last attempt to finish in DAG " + dagInfo.getDagId());
      return;
    }
    
    createCriticalPath(dagInfo, lastAttempt, lastAttemptFinishTime, attempts);
    
    analyzeCriticalPath(dagInfo);

    saveCriticalPathAsSVG(dagInfo);
  }
  
  private void saveCriticalPathAsSVG(DagInfo dagInfo) {
    SVGUtils svg = new SVGUtils();
    String outputFileName = getOutputDir() + File.separator + dagInfo.getDagId() + ".svg";
    System.out.println("Writing output to: " + outputFileName);
    svg.saveCriticalPathAsSVG(dagInfo, outputFileName, criticalPath);
  }
  
  private void analyzeCriticalPath(DagInfo dag) {
    if (!criticalPath.isEmpty()) {
      System.out.println("Walking critical path for dag " + dag.getDagId());
      long dagStartTime = dag.getStartTime();
      long dagTime = dag.getFinishTime() - dagStartTime;
      long totalAttemptCriticalTime = 0;
      for (int i = 0; i < criticalPath.size(); ++i) {
        CriticalPathStep step = criticalPath.get(i);
        totalAttemptCriticalTime += (step.stopCriticalPathTime - step.startCriticalPathTime);
        TaskAttemptInfo attempt = step.attempt;
        if (step.getType() == EntityType.ATTEMPT) {
          // analyze execution overhead
          long avgExecutionTime = attempt.getTaskInfo().getVertexInfo()
              .getAvgExecutionTimeInterval();
          if (avgExecutionTime * 1.25 < attempt.getExecutionTimeInterval()) {
            step.notes
                .add("Potential straggler. Execution time " + attempt.getExecutionTimeInterval()
                    + " compared to vertex average of " + avgExecutionTime);
          }
          
          if (attempt.getStartTime() > step.startCriticalPathTime) {
            // the attempt is critical before launching. So allocation overhead needs analysis
            // analyzer allocation overhead
            Container container = attempt.getContainer();
            if (container != null) {
              Collection<TaskAttemptInfo> attempts = dag.getContainerMapping().get(container);
              if (attempts != null && !attempts.isEmpty()) {
                // arrange attempts by allocation time
                List<TaskAttemptInfo> attemptsList = Lists.newArrayList(attempts);
                Collections.sort(attemptsList, TaskAttemptInfo.orderingOnAllocationTime());
                // walk the list to record allocation time before the current attempt
                long containerPreviousAllocatedTime = 0;
                for (TaskAttemptInfo containerAttempt : attemptsList) {
                  if (containerAttempt.getTaskAttemptId().equals(attempt.getTaskAttemptId())) {
                    break;
                  }
                  System.out.println("Container: " + container.getId() + " running att: " + 
                  containerAttempt.getTaskAttemptId() + " wait att: " + attempt.getTaskAttemptId());
                  containerPreviousAllocatedTime += containerAttempt.getAllocationToEndTimeInterval();
                }
                if (containerPreviousAllocatedTime == 0) {
                  step.notes.add("Container " + container.getId() + " newly allocated.");
                } else {
                  if (containerPreviousAllocatedTime >= attempt.getCreationToAllocationTimeInterval()) {
                    step.notes.add("Container " + container.getId() + " was fully allocated");
                  } else {
                    step.notes.add("Container " + container.getId() + " allocated for " + 
                    SVGUtils.getTimeStr(containerPreviousAllocatedTime) + " out of " +
                        SVGUtils.getTimeStr(attempt.getCreationToAllocationTimeInterval()) + 
                        " of allocation wait time");
                  }
                }
              }
            }
          }
        }
      }
      System.out
          .println("DAG time taken: " + dagTime + " TotalAttemptTime: " + totalAttemptCriticalTime
              + " DAG finish time: " + dag.getFinishTime() + " DAG start time: " + dagStartTime);
    }
  }
  
  private void createCriticalPath(DagInfo dagInfo, TaskAttemptInfo lastAttempt,
      long lastAttemptFinishTime, Map<String, TaskAttemptInfo> attempts) {
    List<CriticalPathStep> tempCP = Lists.newLinkedList();
    if (lastAttempt != null) {
      TaskAttemptInfo currentAttempt = lastAttempt;
      CriticalPathStep currentStep = new CriticalPathStep(currentAttempt, EntityType.DAG_COMMIT);
      long currentAttemptStopCriticalPathTime = lastAttemptFinishTime;

      // add the commit step
      currentStep.stopCriticalPathTime = dagInfo.getFinishTime();
      currentStep.startCriticalPathTime = currentAttemptStopCriticalPathTime;
      currentStep.reason = COMMIT_DEPENDENCY;
      tempCP.add(currentStep);

      while (true) {
        Preconditions.checkState(currentAttempt != null);
        Preconditions.checkState(currentAttemptStopCriticalPathTime > 0);
        System.out.println(
            "Step: " + tempCP.size() + " Attempt: " + currentAttempt.getTaskAttemptId());
        currentStep = new CriticalPathStep(currentAttempt, EntityType.ATTEMPT);
        currentStep.stopCriticalPathTime = currentAttemptStopCriticalPathTime;
        tempCP.add(currentStep);
  
        // find the next attempt on the critical path
        boolean dataDependency = false;
        // find out predecessor dependency
        if (currentAttempt.getLastDataEventTime() > currentAttempt.getCreationTime()) {
          dataDependency = true;
        }
  
        long startCriticalPathTime = 0;
        String nextAttemptId = null;
        String reason = null;
        if (dataDependency) {
          // last data event was produced after the attempt was scheduled. use
          // data dependency
          // typically case when scheduling ahead of time
          System.out.println("Has data dependency");
          if (!Strings.isNullOrEmpty(currentAttempt.getLastDataEventSourceTA())) {
            // there is a valid data causal TA. Use it.
            nextAttemptId = currentAttempt.getLastDataEventSourceTA();
            reason = DATA_DEPENDENCY;
            startCriticalPathTime = currentAttempt.getLastDataEventTime();
            System.out.println("Using data dependency " + nextAttemptId);
          } else {
            // there is no valid data causal TA. This means data event came from the same vertex
            VertexInfo vertex = currentAttempt.getTaskInfo().getVertexInfo();
            Preconditions.checkState(!vertex.getAdditionalInputInfoList().isEmpty(),
                "Vertex: " + vertex.getVertexId() + " has no external inputs but the last data event "
                    + "TA is null for " + currentAttempt.getTaskAttemptId());
            nextAttemptId = null;
            reason = INIT_DEPENDENCY;
            System.out.println("Using init dependency");
          }
        } else {
          // attempt was scheduled after last data event. use scheduling dependency
          // typically happens for retries
          System.out.println("Has scheduling dependency");
          if (!Strings.isNullOrEmpty(currentAttempt.getCreationCausalTA())) {
            // there is a scheduling causal TA. Use it.
            nextAttemptId = currentAttempt.getCreationCausalTA();
            reason = NON_DATA_DEPENDENCY;
            TaskAttemptInfo nextAttempt = attempts.get(nextAttemptId);
            if (nextAttempt != null) {
              VertexInfo currentVertex = currentAttempt.getTaskInfo().getVertexInfo();
              VertexInfo nextVertex = nextAttempt.getTaskInfo().getVertexInfo();
              if (!nextVertex.getVertexName().equals(currentVertex.getVertexName())){
                // cause from different vertex. Might be rerun to re-generate outputs
                for (VertexInfo outVertex : currentVertex.getOutputVertices()) {
                  if (nextVertex.getVertexName().equals(outVertex.getVertexName())) {
                    // next vertex is an output vertex
                    reason = OUTPUT_LOST;
                    break;
                  }
                }
              }
            }
            startCriticalPathTime = currentAttempt.getCreationTime();
            System.out.println("Using scheduling dependency " + nextAttemptId);
          } else {
            // there is no scheduling causal TA.
            if (!Strings.isNullOrEmpty(currentAttempt.getLastDataEventSourceTA())) {
              // there is a data event going to the vertex. Count the time between data event and
              // scheduling time as Initializer/Manager overhead and follow data dependency
              nextAttemptId = currentAttempt.getLastDataEventSourceTA();
              reason = DATA_DEPENDENCY;
              startCriticalPathTime = currentAttempt.getLastDataEventTime();
              long overhead = currentAttempt.getCreationTime()
                  - currentAttempt.getLastDataEventTime();
              currentStep.notes
                  .add("Initializer/VertexManager scheduling overhead " + overhead + " ms");
              System.out.println("Using data dependency " + nextAttemptId);
            } else {
              // there is no scheduling causal TA and no data event casual TA.
              // the vertex has external input that sent the last data events
              // or the vertex has external input but does not use events
              // or the vertex has no external inputs or edges
              nextAttemptId = null;
              reason = INIT_DEPENDENCY;
              System.out.println("Using init dependency");
            }
          }
        }
  
        currentStep.startCriticalPathTime = startCriticalPathTime;
        currentStep.reason = reason;
  
        if (Strings.isNullOrEmpty(nextAttemptId)) {
          Preconditions.checkState(reason.equals(INIT_DEPENDENCY));
          Preconditions.checkState(startCriticalPathTime == 0);
          // no predecessor attempt found. this is the last step in the critical path
          // assume attempts start critical path time is when its scheduled. before that is 
          // vertex initialization time
          currentStep.startCriticalPathTime = currentStep.attempt.getCreationTime();
          
          // add vertex init step
          long initStepStopCriticalTime = currentStep.startCriticalPathTime;
          currentStep = new CriticalPathStep(currentAttempt, EntityType.VERTEX_INIT);
          currentStep.stopCriticalPathTime = initStepStopCriticalTime;
          currentStep.startCriticalPathTime = dagInfo.getStartTime();
          currentStep.reason = INIT_DEPENDENCY;
          tempCP.add(currentStep);
          
          if (!tempCP.isEmpty()) {
            for (int i=tempCP.size() - 1; i>=0; --i) {
              criticalPath.add(tempCP.get(i));
            }
          }
          return;
        }
  
        currentAttempt = attempts.get(nextAttemptId);
        currentAttemptStopCriticalPathTime = startCriticalPathTime;
      }
    }
  }
  
  @Override
  public CSVResult getResult() throws TezException {
    String[] headers = { "Entity", "PathReason", "Status", "CriticalStartTime", 
        "CriticalStopTime", "Notes" };

    CSVResult csvResult = new CSVResult(headers);
    for (CriticalPathStep step : criticalPath) {
      String entity = (step.getType() == EntityType.ATTEMPT ? step.getAttempt().getTaskAttemptId()
          : (step.getType() == EntityType.VERTEX_INIT
              ? step.attempt.getTaskInfo().getVertexInfo().getVertexName() : "DAG COMMIT"));
      String [] record = {entity, step.getReason(), 
          step.getAttempt().getDetailedStatus(), String.valueOf(step.getStartCriticalTime()), 
          String.valueOf(step.getStopCriticalTime()),
          Joiner.on(";").join(step.getNotes())};
      csvResult.addRecord(record);
    }
    return csvResult;
  }

  @Override
  public String getName() {
    return "CriticalPathAnalyzer";
  }

  @Override
  public String getDescription() {
    return "Analyze critical path of the DAG";
  }

  @Override
  public Configuration getConfiguration() {
    return getConf();
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CriticalPathAnalyzer(), args);
    System.exit(res);
  }

}
