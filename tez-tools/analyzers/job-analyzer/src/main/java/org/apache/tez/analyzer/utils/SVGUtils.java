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
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.plutext.jaxb.svg11.Line;
import org.plutext.jaxb.svg11.ObjectFactory;
import org.plutext.jaxb.svg11.Svg;
import org.plutext.jaxb.svg11.Title;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

public class SVGUtils {

  private static final String UTF8 = "UTF-8";

  private static final Logger LOG = LoggerFactory.getLogger(SVGUtils.class);


  private final ObjectFactory objectFactory;
  private final Svg svg;
  private final QName titleName = new QName("title");

  private static int MAX_DAG_RUNTIME = 0;
  private static final int SCREEN_WIDTH = 1800;

  private final DagInfo dagInfo;

  //Gap between various components
  private static final int DAG_GAP = 70;
  private static final int VERTEX_GAP = 50;
  private static final int TASK_GAP = 5;
  private static final int STROKE_WIDTH = 5;

  //To compute the size of the graph.
  private long MIN_X = Long.MAX_VALUE;
  private long MAX_X = Long.MIN_VALUE;

  private int x1 = 0;
  private int y1 = 0;
  private int y2 = 0;

  public SVGUtils(DagInfo dagInfo) {
    this.dagInfo = dagInfo;
    this.objectFactory = new ObjectFactory();
    this.svg = objectFactory.createSvg();
  }

  private Line createLine(int x1, int y1, int x2, int y2) {
    Line line = objectFactory.createLine();
    line.setX1(scaleDown(x1) + "");
    line.setY1(y1 + "");
    line.setX2(scaleDown(x2) + "");
    line.setY2(y2 + "");
    return line;
  }

  private Title createTitle(String msg) {
    Title t = objectFactory.createTitle();
    t.setContent(msg);
    return t;
  }

  private Title createTitleForVertex(VertexInfo vertex) {
    String titleStr = vertex.getVertexName() + ":"
        + (vertex.getFinishTimeInterval())
        + " ms, RelativeTimeToDAG:"
        + (vertex.getInitTime() - this.dagInfo.getStartTime())
        + " ms, counters:" + vertex.getTezCounters();
    Title title = createTitle(titleStr);
    return title;
  }

  private Title createTitleForTaskAttempt(TaskAttemptInfo taskAttemptInfo) {
    String titleStr = "RelativeTimeToVertex:"
        + (taskAttemptInfo.getStartTime() -
        taskAttemptInfo.getTaskInfo().getVertexInfo().getInitTime()) +
        " ms, " + taskAttemptInfo.toString() + ", counters:" + taskAttemptInfo.getTezCounters();
    Title title = createTitle(titleStr);
    return title;
  }

  /**
   * Draw DAG from dagInfo
   *
   * @param dagInfo
   */
  private void drawDAG(DagInfo dagInfo) {
    Title title = createTitle(dagInfo.getDagId() + " : " + dagInfo.getTimeTaken() + " ms");
    int duration = (int) dagInfo.getFinishTimeInterval();
    MAX_DAG_RUNTIME = duration;
    MIN_X = Math.min(dagInfo.getStartTimeInterval(), MIN_X);
    MAX_X = Math.max(dagInfo.getFinishTimeInterval(), MAX_X);
    Line line = createLine(x1, y1, x1 + duration, y2);
    line.getSVGDescriptionClass().add(new JAXBElement<Title>(titleName, Title.class, title));
    line.setStyle("stroke: black; stroke-width:20");
    line.setOpacity("0.3");
    svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass().add(line);
    drawVertex();
  }

  private Collection<VertexInfo> getSortedVertices() {
    Collection<VertexInfo> vertices = this.dagInfo.getVertices();
    // Add corresponding vertex details
    TreeSet<VertexInfo> vertexSet = new TreeSet<VertexInfo>(
        new Comparator<VertexInfo>() {
          @Override
          public int compare(VertexInfo o1, VertexInfo o2) {
            return (int) (o1.getFirstTaskStartTimeInterval() - o2.getFirstTaskStartTimeInterval());
          }
        });
    vertexSet.addAll(vertices);
    return  vertexSet;
  }

  private Collection<TaskInfo> getSortedTasks(VertexInfo vertexInfo) {
    Collection<TaskInfo> tasks = vertexInfo.getTasks();
    // Add corresponding task details
    TreeSet<TaskInfo> taskSet = new TreeSet<TaskInfo>(new Comparator<TaskInfo>() {
      @Override
      public int compare(TaskInfo o1, TaskInfo o2) {
        return (int) (o1.getSuccessfulTaskAttempt().getStartTimeInterval()
            - o2.getSuccessfulTaskAttempt().getStartTimeInterval());
      }
    });
    taskSet.addAll(tasks);
    return taskSet;
  }

  /**
   * Draw the vertices
   *
   */
  public void drawVertex() {
    Collection<VertexInfo> vertices = getSortedVertices();
    for (VertexInfo vertex : vertices) {
      //Set vertex start time as the one when its first task attempt started executing
      x1 = (int) vertex.getStartTimeInterval();
      y1 += VERTEX_GAP;
      int duration = ((int) (vertex.getTimeTaken()));
      Line line = createLine(x1, y1, x1 + duration, y1);
      line.setStyle("stroke: red; stroke-width:" + STROKE_WIDTH);
      line.setOpacity("0.3");

      Title vertexTitle = createTitleForVertex(vertex);
      line.getSVGDescriptionClass().add(
          new JAXBElement<Title>(titleName, Title.class, vertexTitle));
      svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass().add(line);
      // For each vertex, draw the tasks
      drawTask(vertex);
    }
    x1 = x1 + (int) dagInfo.getFinishTimeInterval();
    y1 = y1 + DAG_GAP;
    y2 = y1;
  }

  /**
   * Draw tasks
   *
   * @param vertex
   */
  public void drawTask(VertexInfo vertex) {
    Collection<TaskInfo> tasks = getSortedTasks(vertex);
    for (TaskInfo task : tasks) {
      for (TaskAttemptInfo taskAttemptInfo : task.getTaskAttempts()) {
        x1 = (int) taskAttemptInfo.getStartTimeInterval();
        y1 += TASK_GAP;
        int duration = (int) taskAttemptInfo.getTimeTaken();
        Line line = createLine(x1, y1, x1 + duration, y1);
        String color =
            taskAttemptInfo.getStatus().equalsIgnoreCase(TaskAttemptState.SUCCEEDED.name())
                ? "green" : "red";
        line.setStyle("stroke: " + color + "; stroke-width:" + STROKE_WIDTH);
        Title title = createTitleForTaskAttempt(taskAttemptInfo);
        line.getSVGDescriptionClass().add(
            new JAXBElement<Title>(titleName, Title.class, title));
        svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass()
            .add(line);
      }
    }
  }

  /**
   * Convert DAG to graph
   *
   * @throws java.io.IOException
   * @throws javax.xml.bind.JAXBException
   */
  public void saveAsSVG(String fileName) throws IOException, JAXBException {
    drawDAG(dagInfo);
    svg.setHeight("" + y2);
    svg.setWidth("" + (MAX_X - MIN_X));
    String tempFileName = System.nanoTime() + ".svg";
    File file = new File(tempFileName);
    JAXBContext jaxbContext = JAXBContext.newInstance(Svg.class);
    Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
    jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    jaxbMarshaller.marshal(svg, file);
    //TODO: dirty workaround to get rid of XMLRootException issue
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), UTF8));
    BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(fileName), UTF8));
    try {
      while (reader.ready()) {
        String line = reader.readLine();
        if (line != null) {
          line = line.replaceAll(
              " xmlns:ns3=\"http://www.w3.org/2000/svg\" xmlns=\"\"", "");
          writer.write(line);
          writer.newLine();
        }
      }
    } finally {
      IOUtils.closeQuietly(reader);
      IOUtils.closeQuietly(writer);
      if (file.exists()) {
        boolean deleted = file.delete();
        LOG.debug("Deleted {}" + file.getAbsolutePath());
      }
    }
  }

  private float scaleDown(int len) {
    return (len * 1.0f / MAX_DAG_RUNTIME) * SCREEN_WIDTH;
  }
}