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

import com.sun.istack.Nullable;
import org.apache.tez.dag.utils.Graph;
import org.apache.tez.history.parser.datamodel.AdditionalInputOutputDetails;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.EdgeInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

  private static Pattern sanitizeLabelPattern = Pattern.compile("[:\\-\\W]+");

  public static String getShortClassName(String className) {
    int pos = className.lastIndexOf(".");
    if (pos != -1 && pos < className.length() - 1) {
      return className.substring(pos + 1);
    }
    return className;
  }

  public static String sanitizeLabelForViz(String label) {
    Matcher m = sanitizeLabelPattern.matcher(label);
    return m.replaceAll("_");
  }

  public static void generateDAGVizFile(DagInfo dagInfo, String fileName,
      @Nullable List<String> criticalVertices) throws IOException {
    Graph graph = new Graph(sanitizeLabelForViz(dagInfo.getName()));

    for (VertexInfo v : dagInfo.getVertices()) {
      String nodeLabel = sanitizeLabelForViz(v.getVertexName())
          + "[" + getShortClassName(v.getProcessorClassName()
          + ", tasks=" + v.getTasks().size() + ", time=" + v.getTimeTaken() +" ms]");
      Graph.Node n = graph.newNode(sanitizeLabelForViz(v.getVertexName()), nodeLabel);

      boolean criticalVertex = (criticalVertices != null) ? criticalVertices.contains(v
          .getVertexName()) : false;
      if (criticalVertex) {
        n.setColor("red");
      }


      for (AdditionalInputOutputDetails input : v.getAdditionalInputInfoList()) {
        Graph.Node inputNode = graph.getNode(sanitizeLabelForViz(v.getVertexName())
            + "_" + sanitizeLabelForViz(input.getName()));
        inputNode.setLabel(sanitizeLabelForViz(v.getVertexName())
            + "[" + sanitizeLabelForViz(input.getName()) + "]");
        inputNode.setShape("box");
        inputNode.addEdge(n, "Input name=" + input.getName()
            + " [inputClass=" + getShortClassName(input.getClazz())
            + ", initializer=" + getShortClassName(input.getInitializer()) + "]");
      }
      for (AdditionalInputOutputDetails output : v.getAdditionalOutputInfoList()) {
        Graph.Node outputNode = graph.getNode(sanitizeLabelForViz(v.getVertexName())
            + "_" + sanitizeLabelForViz(output.getName()));
        outputNode.setLabel(sanitizeLabelForViz(v.getVertexName())
            + "[" + sanitizeLabelForViz(output.getName()) + "]");
        outputNode.setShape("box");
        n.addEdge(outputNode, "Output name=" + output.getName()
            + " [outputClass=" + getShortClassName(output.getClazz())
            + ", committer=" + getShortClassName(output.getInitializer()) + "]");
      }

    }

    for (EdgeInfo e : dagInfo.getEdges()) {
      Graph.Node n = graph.getNode(sanitizeLabelForViz(e.getInputVertexName()));
      n.addEdge(graph.getNode(sanitizeLabelForViz(e.getOutputVertexName())),
          "[input=" + getShortClassName(e.getEdgeSourceClass())
              + ", output=" + getShortClassName(e.getEdgeDestinationClass())
              + ", dataMovement=" + e.getDataMovementType().trim() + "]");
    }

    graph.save(fileName);
  }
}
