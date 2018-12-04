/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez;

import javax.annotation.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.event.Event;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventTerminateDag;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.utils.Graph;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.ServicePluginError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
/**
 * Utility class within the tez-dag module
 */
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Pattern to clean the labels in the .dot generation.
   */
  private static Pattern sanitizeLabelPattern = Pattern.compile("[:\\-\\W]+");

  public static String getContainerLauncherIdentifierString(int launcherIndex, AppContext appContext) {
    String name;
    try {
      name = appContext.getContainerLauncherName(launcherIndex);
    } catch (Exception e) {
      LOG.error("Unable to get launcher name for index: " + launcherIndex +
          ", falling back to reporting the index");
      return "[" + String.valueOf(launcherIndex) + "]";
    }
    return "[" + launcherIndex + ":" + name + "]";
  }

  public static String getTaskCommIdentifierString(int taskCommIndex, AppContext appContext) {
    String name;
    try {
      name = appContext.getTaskCommunicatorName(taskCommIndex);
    } catch (Exception e) {
      LOG.error("Unable to get taskcomm name for index: " + taskCommIndex +
          ", falling back to reporting the index");
      return "[" + String.valueOf(taskCommIndex) + "]";
    }
    return "[" + taskCommIndex + ":" + name + "]";
  }

  public static String getTaskSchedulerIdentifierString(int schedulerIndex, AppContext appContext) {
    String name;
    try {
      name = appContext.getTaskSchedulerName(schedulerIndex);
    } catch (Exception e) {
      LOG.error("Unable to get scheduler name for index: " + schedulerIndex +
          ", falling back to reporting the index");
      return "[" + String.valueOf(schedulerIndex) + "]";
    }
    return "[" + schedulerIndex + ":" + name + "]";
  }

  public static void processNonFatalServiceErrorReport(String entityString,
                                                       ServicePluginError servicePluginError,
                                                       String diagnostics,
                                                       DagInfo dagInfo, AppContext appContext,
                                                       String componentName) {
    String message = "Error reported by " + componentName + " [" +
        entityString + "][" +
        servicePluginError +
        "] " + (diagnostics == null ? "" : diagnostics);
    if (dagInfo != null) {
      DAG dag = appContext.getCurrentDAG();
      if (dag != null && dag.getID().getId() == dagInfo.getIndex()) {
        TezDAGID dagId = dag.getID();
        // Send a kill message only if it is the same dag.
        LOG.warn(message + ", Failing dag: [" + dagInfo.getName() + ", " + dagId + "]");
        sendEvent(appContext, new DAGEventTerminateDag(dagId, DAGTerminationCause.SERVICE_PLUGIN_ERROR, message));
      }
    } else {
      LOG.warn("No current dag name provided. Not acting on " + message);
    }
  }

  /**
   * Generate a visualization file.
   * @param dag DAG.
   * @param dagPB DAG plan.
   * @param scheduler scheduler that provide the priorities of the vertexes.
   */
  public static void generateDAGVizFile(final DAG dag,
      final DAGProtos.DAGPlan dagPB, @Nullable final DAGScheduler scheduler) {
    generateDAGVizFile(dag, dagPB, TezCommonUtils.getTrimmedStrings(
        System.getenv(ApplicationConstants.Environment.LOG_DIRS.name())),
        scheduler);
  }

  /**
   * Generate a visualization file.
   * @param dag DAG.
   * @param dagPB DAG plan.
   * @param logDirs directories where the file will be written.
   * @param scheduler scheduler that will provide the priorities
   *                  of the vertexes.
   */
  public static void generateDAGVizFile(final DAG dag,
      final DAGProtos.DAGPlan dagPB,
      final String[] logDirs, final @Nullable DAGScheduler scheduler) {
    TezDAGID dagId = dag.getID();

    HashMap<String, Vertex> nameToVertex = null;
    if (scheduler != null) {
      nameToVertex = new HashMap<>(dag.getVertices().size());
      for (Vertex v: dag.getVertices().values()) {
        nameToVertex.put(v.getName(), v);
      }
    }

    Graph graph = new Graph(sanitizeLabelForViz(dagPB.getName()));
    for (DAGProtos.VertexPlan vertexPlan : dagPB.getVertexList()) {
      StringBuilder nodeLabel = new StringBuilder(
          sanitizeLabelForViz(vertexPlan.getName())
          + "[" + getShortClassName(
              vertexPlan.getProcessorDescriptor().getClassName()));

      if (scheduler != null) {
        Vertex vertex = nameToVertex.get(vertexPlan.getName());
        if (vertex != null) {
          try {
            int priority = (scheduler.getPriorityLowLimit(dag, vertex)
                + scheduler.getPriorityHighLimit(dag,vertex)) / 2;
            nodeLabel.append(", priority=").append(priority).append("]");
          } catch (UnsupportedOperationException e) {
            LOG.info("The DAG graphviz file with priorities will not"
                + " be generate since the scheduler "
                + scheduler.getClass().getSimpleName() + " doesn't"
                + " override the methods to get the priorities");
            return;
          }
        }
      }
      Graph.Node n = graph.newNode(sanitizeLabelForViz(vertexPlan.getName()),
          nodeLabel.toString());
      for (DAGProtos.RootInputLeafOutputProto input
          : vertexPlan.getInputsList()) {
        Graph.Node inputNode = graph.getNode(
            sanitizeLabelForViz(vertexPlan.getName())
            + "_" + sanitizeLabelForViz(input.getName()));
        inputNode.setLabel(sanitizeLabelForViz(vertexPlan.getName())
            + "[" + sanitizeLabelForViz(input.getName()) + "]");
        inputNode.setShape("box");
        inputNode.addEdge(n, "Input"
            + " [inputClass=" + getShortClassName(
                  input.getIODescriptor().getClassName())
            + ", initializer=" + getShortClassName(
                  input.getControllerDescriptor().getClassName()) + "]");
      }
      for (DAGProtos.RootInputLeafOutputProto output
          : vertexPlan.getOutputsList()) {
        Graph.Node outputNode = graph.getNode(sanitizeLabelForViz(
                vertexPlan.getName())
            + "_" + sanitizeLabelForViz(output.getName()));
        outputNode.setLabel(sanitizeLabelForViz(vertexPlan.getName())
            + "[" + sanitizeLabelForViz(output.getName()) + "]");
        outputNode.setShape("box");
        n.addEdge(outputNode, "Output"
            + " [outputClass=" + getShortClassName(
                  output.getIODescriptor().getClassName())
            + ", committer=" + getShortClassName(
                  output.getControllerDescriptor().getClassName()) + "]");
      }
    }

    for (DAGProtos.EdgePlan e : dagPB.getEdgeList()) {

      Graph.Node n = graph.getNode(sanitizeLabelForViz(
          e.getInputVertexName()));
      n.addEdge(graph.getNode(sanitizeLabelForViz(
          e.getOutputVertexName())),
          "["
              + "input=" + getShortClassName(e.getEdgeSource().getClassName())
              + ", output=" + getShortClassName(
                    e.getEdgeDestination().getClassName())
              + ", dataMovement=" + e.getDataMovementType().name().trim()
              + ", schedulingType="
              + e.getSchedulingType().name().trim() + "]");
    }

    String outputFile = "";
    if (logDirs != null && logDirs.length != 0) {
      outputFile += logDirs[0];
      outputFile += File.separator;
    }
    outputFile += dagId.toString();
    // Means we have set the priorities
    if (scheduler != null) {
      outputFile += "_priority";
    }
    outputFile += ".dot";

    try {
      LOG.info("Generating DAG graphviz file"
           + ", dagId=" + dagId.toString()
           + ", filePath=" + outputFile);
      graph.save(outputFile);
    } catch (Exception e) {
      LOG.warn("Error occurred when trying to save graph structure"
          + " for dag " + dagId.toString(), e);
    }
  }

  /**
   * Get the short name of the class.
   * @param className long name
   * @return short name
   */
  private static String getShortClassName(final String className) {
    int pos = className.lastIndexOf(".");
    if (pos != -1 && pos < className.length() - 1) {
      return className.substring(pos + 1);
    }
    return className;
  }

  /**
   * Replace some characters with underscores.
   * @param label label to sanitize
   * @return the label with the replaced characters
   */
  private static String sanitizeLabelForViz(final String label) {
    Matcher m = sanitizeLabelPattern.matcher(label);
    return m.replaceAll("_");
  }

  @SuppressWarnings("unchecked")
  private static void sendEvent(AppContext appContext, Event<?> event) {
    appContext.getEventHandler().handle(event);
  }
}
