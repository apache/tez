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
import java.util.ArrayList;
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
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.history.parser.datamodel.Container;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo.DataDependencyEvent;
import org.apache.tez.history.parser.datamodel.TaskInfo;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CriticalPathAnalyzer extends TezAnalyzerBase implements Analyzer {

  private static final Logger LOG = LoggerFactory.getLogger(CriticalPathAnalyzer.class);

  String succeededState = StringInterner.weakIntern(TaskAttemptState.SUCCEEDED.name());
  String failedState = StringInterner.weakIntern(TaskAttemptState.FAILED.name());

  public enum CriticalPathDependency {
    DATA_DEPENDENCY,
    INIT_DEPENDENCY,
    COMMIT_DEPENDENCY,
    RETRY_DEPENDENCY,
    OUTPUT_RECREATE_DEPENDENCY
  }

  public static final String DRAW_SVG = "tez.critical-path-analyzer.draw-svg";
  public static final String OUTPUT_DIR = "output-dir";

  public static class CriticalPathStep {
    public enum EntityType {
      ATTEMPT,
      VERTEX_INIT,
      DAG_COMMIT
    }

    EntityType type;
    TaskAttemptInfo attempt;
    CriticalPathDependency reason; // reason linking this to the previous step on the critical path
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
    public CriticalPathDependency getReason() {
      return reason;
    }
    public List<String> getNotes() {
      return notes;
    }
  }
  
  List<CriticalPathStep> criticalPath = Lists.newLinkedList();
  
  Map<String, TaskAttemptInfo> attempts = Maps.newHashMap();

  int maxConcurrency = 0;
  ArrayList<TimeInfo> concurrencyByTime = Lists.newArrayList();

  public CriticalPathAnalyzer() {
  }

  public CriticalPathAnalyzer(Configuration conf) {
    setConf(conf);
  }

  @Override 
  public void analyze(DagInfo dagInfo) throws TezException {
    // get all attempts in the dag and find the last failed/succeeded attempt.
    // ignore killed attempt to handle kills that happen upon dag completion
    TaskAttemptInfo lastAttempt = null;
    long lastAttemptFinishTime = 0;
    for (VertexInfo vertex : dagInfo.getVertices()) {
      for (TaskInfo task : vertex.getTasks()) {
        for (TaskAttemptInfo attempt : task.getTaskAttempts()) { 
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
    }
    
    if (lastAttempt == null) {
      LOG.info("Cannot find last attempt to finish in DAG " + dagInfo.getDagId());
      return;
    }
    
    createCriticalPath(dagInfo, lastAttempt, lastAttemptFinishTime, attempts);
    
    analyzeCriticalPath(dagInfo);

    if (getConf().getBoolean(DRAW_SVG, true)) {
      saveCriticalPathAsSVG(dagInfo);
    }
  }
  
  public List<CriticalPathStep> getCriticalPath() {
    return criticalPath;
  }
  
  private void saveCriticalPathAsSVG(DagInfo dagInfo) {
    SVGUtils svg = new SVGUtils();
    String outputDir = getOutputDir();
    if (outputDir == null) {
      outputDir = getConf().get(OUTPUT_DIR);
    }
    String outputFileName = outputDir + File.separator + dagInfo.getDagId() + ".svg";
    LOG.info("Writing output to: " + outputFileName);
    svg.saveCriticalPathAsSVG(dagInfo, outputFileName, criticalPath);
  }
  
  static class TimeInfo implements Comparable<TimeInfo> {
    long timestamp;
    int count;
    boolean start;
    TimeInfo(long timestamp, boolean start) {
      this.timestamp = timestamp;
      this.start = start;
    }
    
    @Override
    public int compareTo(TimeInfo o) {
      return Long.compare(this.timestamp, o.timestamp);
    }
    
    @Override
    public int hashCode() {
      return (int)((timestamp >> 32) ^ timestamp);
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if(o == null) {
        return false;
      }
      if (o.getClass() == this.getClass()) {
        TimeInfo other = (TimeInfo) o;
        return (this.compareTo(other) == 0);
      }
      else {
        return false;
      }
    }
  }
  
  private void determineConcurrency(DagInfo dag) {
    ArrayList<TimeInfo> timeInfo = Lists.newArrayList();
    for (VertexInfo v : dag.getVertices()) {
      for (TaskInfo t : v.getTasks()) {
        for (TaskAttemptInfo a : t.getTaskAttempts()) {
          if (a.getStartTime() > 0) {
            timeInfo.add(new TimeInfo(a.getStartTime(), true));
            timeInfo.add(new TimeInfo(a.getFinishTime(), false));
          }
        }
      }
    }
    Collections.sort(timeInfo);
    
    int concurrency = 0;
    TimeInfo lastTimeInfo = null;
    for (TimeInfo t : timeInfo) {
      concurrency += (t.start) ? 1 : -1;
      maxConcurrency = (concurrency > maxConcurrency) ? concurrency : maxConcurrency;
      if (lastTimeInfo == null || lastTimeInfo.timestamp < t.timestamp) {
        lastTimeInfo = t;
        lastTimeInfo.count = concurrency;
        concurrencyByTime.add(lastTimeInfo);        
      } else {
        // lastTimeInfo.timestamp == t.timestamp
        lastTimeInfo.count = concurrency;
      }
    }
//    for (TimeInfo t : concurrencyByTime) {
//      System.out.println(t.timestamp + " " + t.count);
//    }
  }
  
  private int getIntervalMaxConcurrency(long begin, long end) {
    int concurrency = 0;
    for (TimeInfo timeInfo : concurrencyByTime) {
      if (timeInfo.timestamp < begin) {
        continue;
      }
      if (timeInfo.timestamp > end) {
        break;
      }
      if (timeInfo.count > concurrency) {
        concurrency = timeInfo.count;
      }
    }
    return concurrency;
  }
  
  private void analyzeAllocationOverhead(DagInfo dag) {
    List<TaskAttemptInfo> preemptedAttempts = Lists.newArrayList();
    for (VertexInfo v : dag.getVertices()) {
      for (TaskInfo t : v.getTasks()) {
        for (TaskAttemptInfo a : t.getTaskAttempts()) {
          if (a.getTerminationCause().equals(
              TaskAttemptTerminationCause.INTERNAL_PREEMPTION.name())) {
            LOG.debug("Found preempted attempt " + a.getTaskAttemptId());
            preemptedAttempts.add(a);
          }
        }
      }
    }
    for (int i = 0; i < criticalPath.size(); ++i) {
      CriticalPathStep step = criticalPath.get(i);
      TaskAttemptInfo attempt = step.attempt;
      if (step.getType() != EntityType.ATTEMPT) {
        continue;
      }
      
      long creationTime = attempt.getCreationTime();
      long allocationTime = attempt.getAllocationTime();
      long finishTime = attempt.getFinishTime();
      if (allocationTime < step.startCriticalPathTime) {
        // allocated before it became critical
        continue;
      }

      // the attempt is critical before allocation. So allocation overhead needs analysis
      Container container = attempt.getContainer();
      if (container != null) {
        Collection<TaskAttemptInfo> attempts = dag.getContainerMapping().get(container);
        if (attempts != null && !attempts.isEmpty()) {
          // arrange attempts by allocation time
          List<TaskAttemptInfo> attemptsList = Lists.newArrayList(attempts);
          Collections.sort(attemptsList, TaskAttemptInfo.orderingOnAllocationTime());
          // walk the list to record allocation time before the current attempt
          long containerPreviousAllocatedTime = 0;
          int reUsesForVertex = 1;
          for (TaskAttemptInfo containerAttempt : attemptsList) {
            if (containerAttempt.getTaskAttemptId().equals(attempt.getTaskAttemptId())) {
              break;
            }
            if (containerAttempt.getTaskInfo().getVertexInfo().getVertexId().equals(
                attempt.getTaskInfo().getVertexInfo().getVertexId())) {
              // another task from the same vertex ran in this container. So there are multiple 
              // waves for this vertex on this container.
              reUsesForVertex++;
            }
            long cAllocTime = containerAttempt.getAllocationTime();
            long cFinishTime = containerAttempt.getFinishTime();
            if (cFinishTime > creationTime) {
              // for containerAttempts that used the container while this attempt was waiting
              // add up time container was allocated to containerAttempt. Account for allocations
              // that started before this attempt was created.
              containerPreviousAllocatedTime += 
                  (cFinishTime - (cAllocTime > creationTime ? cAllocTime : creationTime));
            }
          }
          int numVertexTasks = attempt.getTaskInfo().getVertexInfo().getNumTasks();
          int intervalMaxConcurrency = getIntervalMaxConcurrency(creationTime, finishTime);
          double numWaves = getWaves(numVertexTasks, intervalMaxConcurrency);
          
          if (reUsesForVertex > 1) {
            step.notes.add("Container ran multiple tasks for this vertex. ");
            if (numWaves < 1) {
              // less than 1 wave total but still ran more than 1 on this container
              step.notes.add("Vertex potentially seeing contention from other branches in the DAG. ");
            }
          }
          if (containerPreviousAllocatedTime == 0) {
            step.notes.add("Container newly allocated.");
          } else {
            if (containerPreviousAllocatedTime >= attempt.getCreationToAllocationTimeInterval()) {
              step.notes.add("Container was fully allocated");
            } else {
              step.notes.add("Container in use for " + 
              SVGUtils.getTimeStr(containerPreviousAllocatedTime) + " out of " +
                  SVGUtils.getTimeStr(attempt.getCreationToAllocationTimeInterval()) + 
                  " of allocation wait time");
            }
          }
        }
        // look for internal preemptions while attempt was waiting for allocation
        for (TaskAttemptInfo a : preemptedAttempts) {
          if (a.getTaskInfo().getVertexInfo().getVertexId()
              .equals(attempt.getTaskInfo().getVertexInfo().getVertexId())) {
            // dont preempt same vertex task. ideally this should look at priority but we dont have it
            continue;
          }
          if (a.getFinishTime() > creationTime && a.getFinishTime() < allocationTime) {
            // found an attempt that was preempted within this time interval
            step.notes.add("Potentially waited for preemption of " + a.getShortName());
          }
        }
      }
    }
  }
  
  private double getWaves(int numTasks, int concurrency) {
    double numWaves = (numTasks*1.0) / concurrency;
    numWaves = (double)Math.round(numWaves * 10d) / 10d; // convert to 1 decimal place
    return numWaves;
  }
  
  private void analyzeWaves(DagInfo dag) {
    for (int i = 0; i < criticalPath.size(); ++i) {
      CriticalPathStep step = criticalPath.get(i);
      TaskAttemptInfo attempt = step.attempt;
      if (step.getType() != EntityType.ATTEMPT) {
        continue;
      }
      long creationTime = attempt.getCreationTime();
      long finishTime = attempt.getFinishTime();

      int numVertexTasks = attempt.getTaskInfo().getVertexInfo().getNumTasks();
      if (numVertexTasks <= 1) {
        continue;
      }
      int intervalMaxConcurrency = getIntervalMaxConcurrency(creationTime, finishTime);
      double numWaves = getWaves(numVertexTasks, intervalMaxConcurrency);

      step.notes.add("Vertex ran " + numVertexTasks
          + " tasks in " + numWaves
          + " waves with available concurrency of " + intervalMaxConcurrency);
      if (numWaves > 1) {
        if (numWaves%1 < 0.5) {
          // more than 1 wave needed and last wave is small
          step.notes.add("Last partial wave did not use full concurrency. ");
        }
      }
    }
  }
  
  private void analyzeStragglers(DagInfo dag) {
    long dagStartTime = dag.getStartTime();
    long dagTime = dag.getFinishTime() - dagStartTime;
    long totalAttemptCriticalTime = 0;
    for (int i = 0; i < criticalPath.size(); ++i) {
      CriticalPathStep step = criticalPath.get(i);
      totalAttemptCriticalTime += (step.stopCriticalPathTime - step.startCriticalPathTime);
      TaskAttemptInfo attempt = step.attempt;
      if (step.getType() == EntityType.ATTEMPT) {
        // analyze execution overhead
        if (attempt.getLastDataEvents().size() > 1) {
          // there were read errors. that could have delayed the attempt. ignore this
          continue;
        }
        long avgPostDataExecutionTime = attempt.getTaskInfo().getVertexInfo()
            .getAvgPostDataExecutionTimeInterval();
        if (avgPostDataExecutionTime <= 0) {
          continue;
        }
        long attemptExecTime = attempt.getPostDataExecutionTimeInterval();
        if (avgPostDataExecutionTime * 1.25 < attemptExecTime) {
          step.notes
              .add("Potential straggler. Post Data Execution time " + 
                  SVGUtils.getTimeStr(attemptExecTime)
                  + " compared to vertex average of " + 
                  SVGUtils.getTimeStr(avgPostDataExecutionTime));
        }
      }
    }
    LOG.debug("DAG time taken: " + dagTime + " TotalAttemptTime: " + totalAttemptCriticalTime
            + " DAG finish time: " + dag.getFinishTime() + " DAG start time: " + dagStartTime);
  }
  
  private void analyzeCriticalPath(DagInfo dag) {
    if (!criticalPath.isEmpty()) {
      determineConcurrency(dag);
      analyzeStragglers(dag);
      analyzeWaves(dag);
      analyzeAllocationOverhead(dag);
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
      if (dagInfo.getFinishTime() > 0) {
        currentStep.stopCriticalPathTime = dagInfo.getFinishTime();
      } else {
        // AM crashed and no dag finished written
        currentStep.stopCriticalPathTime = currentAttemptStopCriticalPathTime;
      }
      currentStep.startCriticalPathTime = currentAttemptStopCriticalPathTime;
      currentStep.reason = CriticalPathDependency.COMMIT_DEPENDENCY;
      tempCP.add(currentStep);

      while (true) {
        Preconditions.checkState(currentAttempt != null);
        Preconditions.checkState(currentAttemptStopCriticalPathTime > 0);
        LOG.debug("Step: " + tempCP.size() + " Attempt: " + currentAttempt.getTaskAttemptId());
        
        currentStep = new CriticalPathStep(currentAttempt, EntityType.ATTEMPT);
        currentStep.stopCriticalPathTime = currentAttemptStopCriticalPathTime;

        // consider the last data event seen immediately preceding the current critical path 
        // stop time for this attempt
        long currentStepLastDataEventTime = 0;
        String currentStepLastDataTA = null;
        DataDependencyEvent item = currentAttempt.getLastDataEventInfo(currentStep.stopCriticalPathTime);
        if (item!=null) {
          currentStepLastDataEventTime = item.getTimestamp();
          currentStepLastDataTA = item.getTaskAttemptId();
        }

        // sanity check
        for (CriticalPathStep previousStep : tempCP) {
          if (previousStep.type == EntityType.ATTEMPT) {
            if (previousStep.attempt.getTaskAttemptId().equals(currentAttempt.getTaskAttemptId())) {
              // found loop.
              // this should only happen for read errors in currentAttempt
              List<DataDependencyEvent> dataEvents = currentAttempt.getLastDataEvents();
              Preconditions.checkState(dataEvents.size() > 1); // received
                                                               // original and
                                                               // retry data events
              Preconditions.checkState(currentStepLastDataEventTime < dataEvents
                  .get(dataEvents.size() - 1).getTimestamp()); // new event is
                                                               // earlier than
                                                               // last
            }
          }
        }

        tempCP.add(currentStep);
  
        // find the next attempt on the critical path
        boolean dataDependency = false;
        // find out predecessor dependency
        if (currentStepLastDataEventTime > currentAttempt.getCreationTime()) {
          dataDependency = true;
        }
  
        long startCriticalPathTime = 0;
        String nextAttemptId = null;
        CriticalPathDependency reason = null;
        if (dataDependency) {
          // last data event was produced after the attempt was scheduled. use
          // data dependency
          // typically the case when scheduling ahead of time
          LOG.debug("Has data dependency");
          if (!Strings.isNullOrEmpty(currentStepLastDataTA)) {
            // there is a valid data causal TA. Use it.
            nextAttemptId = currentStepLastDataTA;
            reason = CriticalPathDependency.DATA_DEPENDENCY;
            startCriticalPathTime = currentStepLastDataEventTime;
            LOG.debug("Using data dependency " + nextAttemptId);
          } else {
            // there is no valid data causal TA. This means data event came from the same vertex
            VertexInfo vertex = currentAttempt.getTaskInfo().getVertexInfo();
            Preconditions.checkState(!vertex.getAdditionalInputInfoList().isEmpty(),
                "Vertex: " + vertex.getVertexId() + " has no external inputs but the last data event "
                    + "TA is null for " + currentAttempt.getTaskAttemptId());
            nextAttemptId = null;
            reason = CriticalPathDependency.INIT_DEPENDENCY;
            LOG.debug("Using init dependency");
          }
        } else {
          // attempt was scheduled after last data event. use scheduling dependency
          // typically happens for retries
          LOG.debug("Has scheduling dependency");
          if (!Strings.isNullOrEmpty(currentAttempt.getCreationCausalTA())) {
            // there is a scheduling causal TA. Use it.
            nextAttemptId = currentAttempt.getCreationCausalTA();
            reason = CriticalPathDependency.RETRY_DEPENDENCY;
            TaskAttemptInfo nextAttempt = attempts.get(nextAttemptId);
            if (nextAttemptId != null) {
              VertexInfo currentVertex = currentAttempt.getTaskInfo().getVertexInfo();
              VertexInfo nextVertex = nextAttempt.getTaskInfo().getVertexInfo();
              if (!nextVertex.getVertexName().equals(currentVertex.getVertexName())){
                // cause from different vertex. Might be rerun to re-generate outputs
                for (VertexInfo outVertex : currentVertex.getOutputVertices()) {
                  if (nextVertex.getVertexName().equals(outVertex.getVertexName())) {
                    // next vertex is an output vertex
                    reason = CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY;
                    break;
                  }
                }
              }
            }
            if (reason == CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY) {
              // rescheduled due to read error. start critical at read error report time.
              // for now proxy own creation time for read error report time
              startCriticalPathTime = currentAttempt.getCreationTime();
            } else {
              // rescheduled due to own previous attempt failure
              // we are critical when the previous attempt fails
              Preconditions.checkState(nextAttempt != null);
              Preconditions.checkState(nextAttempt.getTaskInfo().getTaskId().equals(
                  currentAttempt.getTaskInfo().getTaskId()));
              startCriticalPathTime = nextAttempt.getFinishTime();
            }
            LOG.debug("Using scheduling dependency " + nextAttemptId);
          } else {
            // there is no scheduling causal TA.
            if (!Strings.isNullOrEmpty(currentStepLastDataTA)) {
              // there is a data event going to the vertex. Count the time between data event and
              // creation time as Initializer/Manager overhead and follow data dependency
              nextAttemptId = currentStepLastDataTA;
              reason = CriticalPathDependency.DATA_DEPENDENCY;
              startCriticalPathTime = currentStepLastDataEventTime;
              long overhead = currentAttempt.getCreationTime() - currentStepLastDataEventTime;
              currentStep.notes
                  .add("Initializer/VertexManager scheduling overhead " + SVGUtils.getTimeStr(overhead));
              LOG.debug("Using data dependency " + nextAttemptId);
            } else {
              // there is no scheduling causal TA and no data event casual TA.
              // the vertex has external input that sent the last data events
              // or the vertex has external input but does not use events
              // or the vertex has no external inputs or edges
              nextAttemptId = null;
              reason = CriticalPathDependency.INIT_DEPENDENCY;
              LOG.debug("Using init dependency");
            }
          }
        }

        currentStep.startCriticalPathTime = startCriticalPathTime;
        currentStep.reason = reason;
        
        Preconditions.checkState(currentStep.stopCriticalPathTime >= currentStep.startCriticalPathTime);
  
        if (Strings.isNullOrEmpty(nextAttemptId)) {
          Preconditions.checkState(reason.equals(CriticalPathDependency.INIT_DEPENDENCY));
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
          currentStep.reason = CriticalPathDependency.INIT_DEPENDENCY;
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
      String [] record = {entity, step.getReason().name(), 
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
