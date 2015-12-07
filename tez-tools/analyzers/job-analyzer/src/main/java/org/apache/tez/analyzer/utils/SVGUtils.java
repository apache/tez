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
package org.apache.tez.analyzer.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.tez.analyzer.plugins.CriticalPathAnalyzer.CriticalPathStep;
import org.apache.tez.analyzer.plugins.CriticalPathAnalyzer.CriticalPathStep.EntityType;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;

import com.google.common.base.Joiner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SVGUtils {

  private static int MAX_DAG_RUNTIME = 0;
  private static final int SCREEN_WIDTH = 1800;

  public SVGUtils() {    
  }

  private int Y_MAX;
  private int X_MAX;
  private static final DecimalFormat secondFormat = new DecimalFormat("#.##");
  private static final int X_BASE = 100;
  private static final int Y_BASE = 100;
  private static final int TICK = 1;
  private static final int STEP_GAP = 50;
  private static final int TEXT_SIZE = 20;
  private static final String RUNTIME_COLOR = "LightGreen";
  private static final String ALLOCATION_OVERHEAD_COLOR = "GoldenRod";
  private static final String LAUNCH_OVERHEAD_COLOR = "DarkSalmon";
  private static final String BORDER_COLOR = "Sienna";
  private static final String VERTEX_INIT_COMMIT_COLOR = "LightSalmon";
  private static final String CRITICAL_COLOR = "IndianRed";
  private static final float RECT_OPACITY = 1.0f;
  private static final String TITLE_BR = "&#13;";

  public static String getTimeStr(final long millis) {
    long minutes = TimeUnit.MILLISECONDS.toMinutes(millis)
            - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis));
    long hours = TimeUnit.MILLISECONDS.toHours(millis);
    StringBuilder b = new StringBuilder();
    b.append(hours == 0 ? "" : String.valueOf(hours) + "h");
    b.append(minutes == 0 ? "" : String.valueOf(minutes) + "m");
    long seconds = millis - TimeUnit.MINUTES.toMillis(
        TimeUnit.MILLISECONDS.toMinutes(millis));
    b.append(secondFormat.format(seconds/1000.0) + "s");
    
    return b.toString(); 
  }
  
  List<String> svgLines = new LinkedList<String>();
  
  private final int addOffsetX(int x) {
    int xOff = x + X_BASE;
    X_MAX = Math.max(X_MAX, xOff);
    return xOff;
  }
  
  private final int addOffsetY(int y) {
    int yOff = y + Y_BASE;
    Y_MAX = Math.max(Y_MAX, yOff);
    return yOff;
  }
  
  private int scaleDown(int len) {
    return Math.round((len * 1.0f / MAX_DAG_RUNTIME) * SCREEN_WIDTH);
  }
  
  private void addRectStr(int x, int width, int y, int height, 
      String fillColor, String borderColor, float opacity, String title) {
    String rectStyle = "stroke: " + borderColor + "; fill: " + fillColor + "; opacity: " + opacity;
    String rectStr = "<rect x=\"" + addOffsetX(scaleDown(x)) + "\""
               + " y=\"" + addOffsetY(y) + "\""
               + " width=\"" + scaleDown(width) + "\""
               + " height=\"" + height + "\""
               + " style=\"" + rectStyle + "\""
               + " >"
               + " <title>" + title +"</title>"
               + " </rect>";
    svgLines.add(rectStr);    
  }
  
  private void addTextStr(int x, int y, String text, String anchor, int size, String title, boolean italic) {
    String textStyle = "text-anchor: " + anchor + "; font-style: " + (italic?"italic":"normal") +
        "; font-size: " + size + "px;";
    String textStr = "<text x=\"" + addOffsetX(scaleDown(x)) + "\" "
        + "y=\"" + addOffsetY(y) + "\" "
        + "style=\"" + textStyle + "\" transform=\"\">"
        + text
        + " <title>" + title +"</title>"
        + "</text>";
    svgLines.add(textStr);
  }
  
  private void addLineStr(int x1, int y1, int x2, int y2, String color, String title, int width) {
    String style = "stroke: " + color + "; stroke-width:" + width;
    String str = "<line x1=\"" + addOffsetX(scaleDown(x1)) + "\""
               + " y1=\"" + addOffsetY(y1) + "\""
               + " x2=\"" + addOffsetX(scaleDown(x2)) + "\""
               + " y2=\"" + addOffsetY(y2) + "\""
               + " style=\"" + style + "\""
               + " >"
               + " <title>" + title +"</title>"
               + " </line>";
    svgLines.add(str);
  }
  
  public void drawStep(CriticalPathStep step, long dagStartTime, int yOffset) {
    if (step.getType() != EntityType.ATTEMPT) {
      // draw initial vertex or final commit overhead
      StringBuilder title = new StringBuilder();
      String text = null;
      if (step.getType() == EntityType.VERTEX_INIT) {
        String vertex = step.getAttempt().getTaskInfo().getVertexInfo().getVertexName(); 
        text = vertex + " : Init";
        title.append(text).append(TITLE_BR);
      } else {
        text = "Output Commit";
        title.append(text).append(TITLE_BR);
      }
      title.append("Critical Path Dependency: " + step.getReason()).append(TITLE_BR);
      title.append(
          "Critical Time: " + getTimeStr(step.getStopCriticalTime() - step.getStartCriticalTime()))
          .append("");
      title.append(Joiner.on(TITLE_BR).join(step.getNotes()));
      String titleStr = title.toString();
      int stopTimeInterval = (int) (step.getStopCriticalTime() - dagStartTime);
      int startTimeInterval = (int) (step.getStartCriticalTime() - dagStartTime);
      addRectStr(startTimeInterval,
          (stopTimeInterval - startTimeInterval), yOffset * STEP_GAP, STEP_GAP,
          VERTEX_INIT_COMMIT_COLOR, BORDER_COLOR, RECT_OPACITY, titleStr);
      addTextStr((stopTimeInterval + startTimeInterval) / 2,
          (yOffset * STEP_GAP + STEP_GAP / 2),
          text, "middle",
          TEXT_SIZE, titleStr, false);
    } else {
      TaskAttemptInfo attempt = step.getAttempt();
      int startCriticalTimeInterval = (int) (step.getStartCriticalTime() - dagStartTime);
      int stopCriticalTimeInterval = (int) (step.getStopCriticalTime() - dagStartTime);
      int creationTimeInterval = (int) (attempt.getCreationTime() - dagStartTime);
      int allocationTimeInterval = attempt.getAllocationTime() > 0 ? 
          (int) (attempt.getAllocationTime() - dagStartTime) : 0;
      int launchTimeInterval = attempt.getStartTime() > 0 ? 
          (int) (attempt.getStartTime() - dagStartTime) : 0;
      int finishTimeInterval = (int) (attempt.getFinishTime() - dagStartTime);
      System.out.println(attempt.getTaskAttemptId() + " " + creationTimeInterval + " "
          + allocationTimeInterval + " " + launchTimeInterval + " " + finishTimeInterval);

      StringBuilder title = new StringBuilder();
      title.append("Attempt: " + attempt.getTaskAttemptId()).append(TITLE_BR);
      title.append("Critical Path Dependency: " + step.getReason()).append(TITLE_BR);
      title.append("Completion Status: " + attempt.getDetailedStatus()).append(TITLE_BR);
      title.append(
          "Critical Time Contribution: " + 
              getTimeStr(step.getStopCriticalTime() - step.getStartCriticalTime())).append(TITLE_BR);
      title.append("Critical start at: " + getTimeStr(startCriticalTimeInterval)).append(TITLE_BR);
      title.append("Critical stop at: " + getTimeStr(stopCriticalTimeInterval)).append(TITLE_BR);
      title.append("Created at: " + getTimeStr(creationTimeInterval)).append(TITLE_BR);
      if (allocationTimeInterval > 0) {
        title.append("Allocated at: " + getTimeStr(allocationTimeInterval)).append(TITLE_BR);
      }
      if (launchTimeInterval > 0) {
        title.append("Launched at: " + getTimeStr(launchTimeInterval)).append(TITLE_BR);
      }
      title.append("Finished at: " + getTimeStr(finishTimeInterval)).append(TITLE_BR);
      title.append(Joiner.on(TITLE_BR).join(step.getNotes()));
      String titleStr = title.toString();

      // handle cases when attempt fails before allocation or launch
      if (allocationTimeInterval > 0) {
        addRectStr(creationTimeInterval, allocationTimeInterval - creationTimeInterval,
            yOffset * STEP_GAP, STEP_GAP, ALLOCATION_OVERHEAD_COLOR, BORDER_COLOR, RECT_OPACITY,
            titleStr);
        if (launchTimeInterval > 0) {
          addRectStr(allocationTimeInterval, launchTimeInterval - allocationTimeInterval,
              yOffset * STEP_GAP, STEP_GAP, LAUNCH_OVERHEAD_COLOR, BORDER_COLOR, RECT_OPACITY,
              titleStr);          
          addRectStr(launchTimeInterval, finishTimeInterval - launchTimeInterval, yOffset * STEP_GAP,
              STEP_GAP, RUNTIME_COLOR, BORDER_COLOR, RECT_OPACITY, titleStr);
        } else {
          // no launch - so allocate to finish drawn - ended while launching
          addRectStr(allocationTimeInterval, finishTimeInterval - allocationTimeInterval, yOffset * STEP_GAP,
              STEP_GAP, LAUNCH_OVERHEAD_COLOR, BORDER_COLOR, RECT_OPACITY, titleStr);        
        }
      } else {
        // no allocation - so create to finish drawn - ended while allocating
        addRectStr(creationTimeInterval, finishTimeInterval - creationTimeInterval, yOffset * STEP_GAP,
            STEP_GAP, ALLOCATION_OVERHEAD_COLOR, BORDER_COLOR, RECT_OPACITY, titleStr);        
      }

      addTextStr((finishTimeInterval + creationTimeInterval) / 2,
          (yOffset * STEP_GAP + STEP_GAP / 2),   attempt.getShortName(), "middle", TEXT_SIZE, 
          titleStr, !attempt.isSucceeded());
    }
  }

  private void drawCritical(DagInfo dagInfo, List<CriticalPathStep> criticalPath) {
    long dagStartTime = dagInfo.getStartTime();
    int dagStartTimeInterval = 0; // this is 0 since we are offseting from the dag start time
    int dagFinishTimeInterval = (int) (dagInfo.getFinishTime() - dagStartTime);
    if (dagInfo.getFinishTime() <= 0) {
      // AM crashed. no dag finish time written
      dagFinishTimeInterval =(int) (criticalPath.get(criticalPath.size()-1).getStopCriticalTime()
          - dagStartTime);
    }
    MAX_DAG_RUNTIME = dagFinishTimeInterval;
    
    // draw grid
    addLineStr(dagStartTimeInterval, 0, dagFinishTimeInterval, 0, BORDER_COLOR, "", TICK);
    int yGrid = (criticalPath.size() + 2)*STEP_GAP;
    for (int i=0; i<11; ++i) {
      int x = Math.round(((dagFinishTimeInterval - dagStartTimeInterval)/10.0f)*i);
      addLineStr(x, 0, x, yGrid, BORDER_COLOR, "", TICK);  
      addTextStr(x, 0, getTimeStr(x), "left", TEXT_SIZE, "", false);
    }
    addLineStr(dagStartTimeInterval, yGrid, dagFinishTimeInterval, yGrid, BORDER_COLOR, "", TICK);
    addTextStr((dagFinishTimeInterval + dagStartTimeInterval) / 2, yGrid + STEP_GAP,
        "Critical Path for " + dagInfo.getName() + " (" + dagInfo.getDagId() + ")", "middle",
        TEXT_SIZE, "", false);

    // draw steps
    for (int i=1; i<=criticalPath.size(); ++i) {
      CriticalPathStep step = criticalPath.get(i-1); 
      drawStep(step, dagStartTime, i);      
    }
    
    // draw critical path on top
    for (int i=1; i<=criticalPath.size(); ++i) {
      CriticalPathStep step = criticalPath.get(i-1); 
      boolean isLast = i == criticalPath.size(); 
      
      // draw critical path for step
      int startCriticalTimeInterval = (int) (step.getStartCriticalTime() - dagStartTime);
      int stopCriticalTimeInterval = (int) (step.getStopCriticalTime() - dagStartTime);
      addLineStr(startCriticalTimeInterval, (i + 1) * STEP_GAP, stopCriticalTimeInterval,
          (i + 1) * STEP_GAP, CRITICAL_COLOR, "Critical Time " + step.getAttempt().getShortName(), TICK*5);
      
      if (isLast) {
        // last step. add commit overhead
        int stepStopCriticalTimeInterval = (int) (step.getStopCriticalTime() - dagStartTime);
        addLineStr(stepStopCriticalTimeInterval, (i + 1) * STEP_GAP, dagFinishTimeInterval,
            (i + 1) * STEP_GAP, CRITICAL_COLOR,
            "Critical Time " + step.getAttempt().getTaskInfo().getVertexInfo().getVertexName(), TICK*5);
      } else {
        // connect to next step in critical path
        addLineStr(stopCriticalTimeInterval, (i + 1) * STEP_GAP, stopCriticalTimeInterval,
            (i + 2) * STEP_GAP, CRITICAL_COLOR, "Critical Time " + step.getAttempt().getShortName(), TICK*5);
      }
    }
    
    // draw legend
    int legendX = 0;
    int legendY = (criticalPath.size() + 2) * STEP_GAP;
    int legendWidth = dagFinishTimeInterval/5;
    
    addRectStr(legendX, legendWidth, legendY, STEP_GAP/2, VERTEX_INIT_COMMIT_COLOR, BORDER_COLOR, RECT_OPACITY, "");
    addTextStr(legendX, legendY + STEP_GAP/3, "Vertex Init/Commit Overhead", "left", TEXT_SIZE, "", false);
    legendY += STEP_GAP/2;
    addRectStr(legendX, legendWidth, legendY, STEP_GAP/2, ALLOCATION_OVERHEAD_COLOR, BORDER_COLOR, RECT_OPACITY, "");
    addTextStr(legendX, legendY + STEP_GAP/3, "Task Allocation Overhead", "left", TEXT_SIZE, "", false);
    legendY += STEP_GAP/2;
    addRectStr(legendX, legendWidth, legendY, STEP_GAP/2, LAUNCH_OVERHEAD_COLOR, BORDER_COLOR, RECT_OPACITY, "");
    addTextStr(legendX, legendY + STEP_GAP/3, "Task Launch Overhead", "left", TEXT_SIZE, "", false);
    legendY += STEP_GAP/2;
    addRectStr(legendX, legendWidth, legendY, STEP_GAP/2, RUNTIME_COLOR, BORDER_COLOR, RECT_OPACITY, "");
    addTextStr(legendX, legendY + STEP_GAP/3, "Task Execution Time", "left", TEXT_SIZE, "", false);
    
    Y_MAX += Y_BASE*2;
    X_MAX += X_BASE*2;
  }
  
  public void saveCriticalPathAsSVG(DagInfo dagInfo, 
      String fileName, List<CriticalPathStep> criticalPath) {
    drawCritical(dagInfo, criticalPath);
    saveFileStr(fileName);
  }
  
  private void saveFileStr(String fileName) {
    String header = "<?xml version=\"1.0\" standalone=\"no\"?> "
        + "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" "
        + "\"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">"
        + "<svg xmlns=\"http://www.w3.org/2000/svg\" version=\"1.1\" "
        + "xmlns:xlink=\"http://www.w3.org/1999/xlink\" "
        + "height=\"" + Y_MAX + "\" "
        + "width=\""  + X_MAX + "\"> "
        + "<script type=\"text/ecmascript\" "
        + "xlink:href=\"http://code.jquery.com/jquery-2.1.1.min.js\" />";
    String footer = "</svg>";
    String newline = System.getProperty("line.separator");
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriterWithEncoding(fileName, "UTF-8"));
      writer.write(header);
      writer.write(newline);
      for (String str : svgLines) {
        writer.write(str);
        writer.write(newline);
      }
      writer.write(footer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null) {
        IOUtils.closeQuietly(writer);
      }
    }

  }

}
