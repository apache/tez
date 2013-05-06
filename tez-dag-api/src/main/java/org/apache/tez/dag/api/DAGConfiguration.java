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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

// FIXME dag conf should not be public API???
public class DAGConfiguration extends Configuration {

  private static final Log LOG = LogFactory.getLog(DAGConfiguration.class);

  public DAGConfiguration(Configuration conf) {
    super(conf);
    if (! (conf instanceof DAGConfiguration)) {
      this.reloadConfiguration();
    }
  }

  public DAGConfiguration() {
    super();
  }

  public final static String DAG = "tez.dag.";
  
  public final static String DAG_AM = DAG + "am.";

  public final static String VERTEX = DAG + "vertex.";

  public final static String TASK = DAG + "task.";

  public final static String TASK_ATTEMPT = DAG + "attempt.";

  public final static String TEZ_DAG_VERTICES = DAG + "vertices";

  public final static String TEZ_DAG_EDGES = DAG + "edges";

  public final static String EDGE = DAG + "edge.";

  private final static String SEPARATOR = "|";
  
  public final static String TEZ_DAG_CONFIG_KEYS = DAG + "keys";
  public final static String TEZ_DAG_CONFIG_VALUES = DAG + "values";

  @Private
  public void setConfig(Map<String, String> config) {
    if(!config.isEmpty()) {
      String[] key = new String[config.size()];
      String[] value = new String[config.size()];
      int i=0;
      for(Entry<String, String> entry : config.entrySet()) {
        key[i] = entry.getKey();
        value[i] = entry.getValue();
        i++;
      }
      setStrings(TEZ_DAG_CONFIG_KEYS, key);
      setStrings(TEZ_DAG_CONFIG_VALUES, value);
    }
  }
  
  @Private
  public Map<String, String> getConfig() {
    HashMap<String, String> config = new HashMap<String, String>();
    String[] key = getStrings(TEZ_DAG_CONFIG_KEYS);
    if(key != null) {
      String[] value = getStrings(TEZ_DAG_CONFIG_VALUES);
      assert value.length == key.length;
      for(int i=0; i<key.length; ++i) {
        config.put(key[i], value[i]);
      }
    }
    return config;
  }
  
  @Private
  public void setEdgeProperties(List<Edge> edges) {
    String[] edgeIds = new String[edges.size()];
    for(int i=0; i<edges.size(); ++i) {
      edgeIds[i] = edges.get(i).getId();
    }
    setStrings(TEZ_DAG_EDGES, edgeIds);
    for (Edge edge : edges) {
      setEdgeProperty(edge.getEdgeProperty(), edge.getId());
    }
  }

  public Map<String, EdgeProperty> getEdgeProperties() {
    String[] edgeIds = getStrings(TEZ_DAG_EDGES);
    if (edgeIds == null) {
      return new TreeMap<String, EdgeProperty>();
    }
    Map<String, EdgeProperty> edgeProperties =
                          new HashMap<String, EdgeProperty>(edgeIds.length);
    for(int i=0; i<edgeIds.length; ++i) {
      edgeProperties.put(edgeIds[i], getEdgeProperty(edgeIds[i]));
    }
    return edgeProperties;
  }

  private void setEdgeProperty(EdgeProperty edgeProperty, String edgeId) {
    String[] edgeStrs = new String[4];
    edgeStrs[0] = edgeProperty.getConnectionPattern().name();
    edgeStrs[1] = edgeProperty.getSourceType().name();
    edgeStrs[2] = edgeProperty.inputClass;
    edgeStrs[3] = edgeProperty.outputClass;

    setStrings(EDGE + edgeId, edgeStrs);
  }

  private EdgeProperty getEdgeProperty(String edgeId) {
    String[] edgeStr = getStrings(EDGE + edgeId);
    assert edgeStr.length == 4;
    return new EdgeProperty(EdgeProperty.ConnectionPattern.valueOf(edgeStr[0]),
                             EdgeProperty.SourceType.valueOf(edgeStr[1]),
                             edgeStr[2],
                             edgeStr[3]);
  }

  public final static String TEZ_DAG_VERTEX_TASKS = VERTEX + "num-tasks";
  public final static int DEFAULT_TEZ_DAG_VERTEX_TASKS  = 0;

  public int getNumVertexTasks(String vertexName) {
    return getInt(
        TEZ_DAG_VERTEX_TASKS + "." + vertexName,
        DEFAULT_TEZ_DAG_VERTEX_TASKS);
  }

  public void setNumVertexTasks(String vertexName, int numTasks) {
    setInt(
        TEZ_DAG_VERTEX_TASKS + "." + vertexName,
        numTasks);
  }

  public final String TEZ_DAG_VERTEX_TASK_MEMORY = TASK + "memory-mb";

  public final int DEFAULT_TEZ_DAG_VERTEX_TASK_MEMORY = 1024;

  public int getVertexTaskMemory(String vertexName) {
    return getInt(
        TEZ_DAG_VERTEX_TASK_MEMORY + "." + vertexName,
        DEFAULT_TEZ_DAG_VERTEX_TASK_MEMORY);
  }

  public void setVertexTaskMemory(String vertexName, int memory) {
    setInt(
        TEZ_DAG_VERTEX_TASK_MEMORY + "." + vertexName,
        memory);
  }

  public final String TEZ_DAG_VERTEX_TASK_CPU = TASK + "cpu-vcores";

  public final int DEFAULT_TEZ_DAG_VERTEX_TASK_CORES = 1;

  public int getVertexTaskCores(String vertexName) {
    return getInt(
        TEZ_DAG_VERTEX_TASK_CPU + "." + vertexName,
        DEFAULT_TEZ_DAG_VERTEX_TASK_CORES);
  }
  public void setVertexTaskCores(String vertexName, int cores) {
    setInt(
        TEZ_DAG_VERTEX_TASK_CPU + "." + vertexName,
        cores);
  }

  private final String[] EMPTY = new String[0];

  public String[] getVertices() {
    String[] vertices = getStrings(TEZ_DAG_VERTICES, EMPTY);
    return vertices == null? EMPTY : vertices;
  }

  void setVertexResource(Vertex vertex) {
    Resource resource = vertex.getTaskResource();
    if (resource == null) {
      return;
    }
    setVertexTaskCores(vertex.getVertexName(), resource.getVirtualCores());
    setVertexTaskMemory(vertex.getVertexName(), resource.getMemory());
  }

  public Resource getVertexResource(String vertexName) {
    int memory = getVertexTaskMemory(vertexName);
    int vCores = getVertexTaskCores(vertexName);
    return BuilderUtils.newResource(memory, vCores);
  }

  // FIXME we are serializing YarnURL which is not same as serializing a URL
  public final String TEZ_DAG_VERTEX_TASK_LOCAL_RESOURCE = TASK + "local-resource.";
  void setVertexLocalResource(Vertex vertex) {
    Map<String,LocalResource> lrs = vertex.getTaskLocalResources();
    if (lrs == null) {
      return;
    }
    String[] lrStrs = new String[lrs.size()];
    int i=0;
    for(Map.Entry<String,LocalResource> entry : lrs.entrySet()) {
      LocalResource lr = entry.getValue();
      try {
        String lrStr = StringUtils.escapeString(entry.getKey(),
            StringUtils.ESCAPE_CHAR,
            SEPARATOR.charAt(0))
            + SEPARATOR
            + StringUtils.escapeString(
                ConverterUtils.getPathFromYarnURL(lr.getResource()).toString(),
                StringUtils.ESCAPE_CHAR,
                SEPARATOR.charAt(0))
            + SEPARATOR
            + StringUtils.escapeString(String.valueOf(lr.getSize()),
                StringUtils.ESCAPE_CHAR, SEPARATOR.charAt(0))
            + SEPARATOR
            + StringUtils.escapeString(String.valueOf(lr.getTimestamp()),
                StringUtils.ESCAPE_CHAR, SEPARATOR.charAt(0))
            + SEPARATOR
            + StringUtils.escapeString(lr.getType().name(),
                StringUtils.ESCAPE_CHAR, SEPARATOR.charAt(0))
            + SEPARATOR
            + StringUtils.escapeString(lr.getVisibility().name(),
                StringUtils.ESCAPE_CHAR, SEPARATOR.charAt(0))
            + SEPARATOR
            + StringUtils.escapeString(
                (lr.getPattern() == null ? "" : lr.getPattern()),
                StringUtils.ESCAPE_CHAR, SEPARATOR.charAt(0));
        lrStrs[i++] = StringUtils.escapeString(lrStr);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
    setStrings(TEZ_DAG_VERTEX_TASK_LOCAL_RESOURCE + vertex.getVertexName(),
        lrStrs);
  }

  public Map<String, LocalResource> getVertexLocalResources(String vertexName) {
    String[] lrStrs = StringUtils.split(get(
        TEZ_DAG_VERTEX_TASK_LOCAL_RESOURCE + vertexName, ""));
    Map<String, LocalResource> localResources =
        new TreeMap<String, LocalResource>();
    if (lrStrs == null) {
      return localResources;
    }
    LOG.info("XXXX Found " + lrStrs.length + " local resources");
    for (String lrStr : lrStrs) {
      String[] tokens =
          StringUtils.split(
              lrStr, StringUtils.ESCAPE_CHAR, SEPARATOR.charAt(0));
      if (tokens.length != 6
          && tokens.length != 7) {
        LOG.warn("Invalid token count in serialized LocalResource"
            + ", serializedString=" + lrStr
            + ", tokenCount=" + tokens.length);
      }
      String resourceName = tokens[0];
      LocalResource lRsrc = Records.newRecord(LocalResource.class);
      lRsrc.setResource(ConverterUtils.getYarnUrlFromPath(
          new Path(tokens[1])));
      lRsrc.setSize(Long.valueOf(tokens[2]));
      lRsrc.setTimestamp(Long.valueOf(tokens[3]));
      lRsrc.setType(LocalResourceType.valueOf(tokens[4]));
      lRsrc.setVisibility(LocalResourceVisibility.valueOf(
          tokens[5]));
      if (tokens.length == 7) {
        lRsrc.setPattern(tokens[6]);
      }
      try {
        LOG.info("XXXX Adding local resource"
            + ", vertexName=" + vertexName
            + ", resourceName=" + resourceName
            + ", resourceUrl"
            + ConverterUtils.getPathFromYarnURL(
                lRsrc.getResource()).toString());
      } catch (URISyntaxException e) {
        // Ignore
        // FIXME
      }
      localResources.put(resourceName, lRsrc);
    }
    return localResources;
  }

  public final String TEZ_DAG_VERTEX_TASK_ENV = TASK + "env.";
  void setVertexEnv(Vertex vertex) {
    Map<String, String> env = vertex.getTaskEnvironment();
    if (env == null) {
      return;
    }
    String[] envStrs = new String[env.size()];
    int i=0;
    for(Map.Entry<String,String> entry : env.entrySet()) {
      String envStr = entry.getKey() + SEPARATOR + entry.getValue();
      envStrs[i++] = StringUtils.escapeString(envStr);
    }
    set(TEZ_DAG_VERTEX_TASK_ENV + vertex.getVertexName(),
        StringUtils.join(",", envStrs));
  }

  public Map<String,String> getVertexEnv(String vertexName) {
    String[] envStrs = StringUtils.split(
        get(TEZ_DAG_VERTEX_TASK_ENV + vertexName, ""));
    Map<String,String> env = new HashMap<String,String>();
    if(envStrs == null) {
      return env;
    }

    LOG.info("XXXX Found " + envStrs.length + " environment");
    for(String envStr : envStrs ) {
      LOG.info("XXXX Parsing env from " + envStr);
      StringTokenizer tokenizer = new StringTokenizer (envStr, SEPARATOR);
      String envName = tokenizer.nextToken();
      String envValue = tokenizer.nextToken();
      env.put(StringUtils.unEscapeString(envName),
          StringUtils.unEscapeString(envValue));
    }

    return env;
  }
  
  public final String TEZ_DAG_NAME = DAG + "name";
  @Private
  public void setName(String name) {
    setStrings(name, name);
  }
  
  @Public
  @Stable
  public String getName() {
    String[] name = getStrings(TEZ_DAG_NAME);
    assert name != null && name.length == 1;
    return name[0];
  }

  @Private
  public void setVertices(List<Vertex> vertices) {
    setVertices(TEZ_DAG_VERTICES, vertices);
    for(Vertex vertex : vertices) {
      // set num tasks
      setNumVertexTasks(vertex.getVertexName(), vertex.getParallelism());
      // set resource
      setVertexResource(vertex);
      // set localResource
      setVertexLocalResource(vertex);
      // set environment
      setVertexEnv(vertex);
      // set processor name
      setVertexTaskModuleClassName(vertex);
      //set javaOpts
      setVertexJavaOpts(vertex.getVertexName(), vertex.getJavaOpts());
    }
  }

  public final String TEZ_DAG_VERTEX_INPUT_VERTICES = VERTEX + "input-vertices";
  @Public
  @Stable
  public String[] getInputVertices(String vertexName) {
    String[] vertices =
        getStrings(TEZ_DAG_VERTEX_INPUT_VERTICES + "." + vertexName, EMPTY);
    return vertices == null ? EMPTY : vertices;
  }
  @Private
  public void setInputVertices(String vertexName, List<Vertex> inputVertices) {
    setVertices(TEZ_DAG_VERTEX_INPUT_VERTICES + "." + vertexName,
        inputVertices);
  }

  private void setVertices(String key, List<Vertex> vertices) {
    String[] verticesNames = new String[vertices.size()];
    for (int i = 0; i < vertices.size(); ++i) {
      verticesNames[i] = vertices.get(i).getVertexName();
    }
    setStrings(key, verticesNames);
  }

  public final String TEZ_DAG_VERTEX_OUTPUT_VERTICES = VERTEX
      + "output-vertices";
  @Public
  @Stable
  public String[] getOutputVertices(String vertexName) {
    String[] vertices =
        getStrings(TEZ_DAG_VERTEX_OUTPUT_VERTICES + "." + vertexName, EMPTY);
    return vertices == null? EMPTY : vertices;
  }
  @Private
  public void setOutputVertices(String vertexName,
      List<Vertex> outputVertices) {
    setVertices(TEZ_DAG_VERTEX_OUTPUT_VERTICES + "." + vertexName,
        outputVertices);
  }

  public final String TEZ_DAG_VERTEX_INPUT_EDGES = VERTEX + "input-edges";
  @Public
  @Stable
  public List<String> getInputEdgeIds(String vertexName) {
    return getEdgeIds(TEZ_DAG_VERTEX_INPUT_EDGES + "." + vertexName);
  }
  public void setInputEdgeIds(String vertexName, List<String> edgeIds) {
    setEdgeIds(TEZ_DAG_VERTEX_INPUT_EDGES + "." + vertexName, edgeIds);
  }

  public final String TEZ_DAG_VERTEX_OUTPUT_EDGES = VERTEX + "output-edges";
  @Public
  @Stable
  public List<String> getOutputEdgeIds(String vertexName) {
    return getEdgeIds(TEZ_DAG_VERTEX_OUTPUT_EDGES + "." + vertexName);
  }
  @Private
  public void setOutputEdgeIds(String vertexName, List<String> edgeIds) {
    setEdgeIds(TEZ_DAG_VERTEX_OUTPUT_EDGES + "." + vertexName, edgeIds);
  }

  private List<String> getEdgeIds(String key) {
    String[] edgeIds = getStrings(key, EMPTY);
    if (edgeIds == null) {
      return new ArrayList<String>();
    }
    return Arrays.asList(edgeIds);
  }

  private void setEdgeIds(String key, List<String> edgeIds) {
    setStrings(key, edgeIds.toArray(new String[]{}));
  }

  private void setVertexTaskModuleClassName(Vertex vertex) {
    setVertexTaskModuleClassName(vertex.getVertexName(),
        vertex.getProcessorName());
  }

  public final String TEZ_DAG_VERTEX_TASK_MODULE= VERTEX + "task-module";
  @Private
  public String getVertexTaskModuleClassName(String vertexName) {
    return get(TEZ_DAG_VERTEX_TASK_MODULE + "." + vertexName);
  }
  @Private
  public void setVertexTaskModuleClassName(String vertexName,
      String taskModule) {
    set(TEZ_DAG_VERTEX_TASK_MODULE + "." + vertexName, taskModule);
  }
  
  public final String TEZ_DAG_VERTEX_JAVAOPTS= VERTEX + "java-opts";
  @Private 
  public String getVertexJavaOpts(String vertexName) {
	  String opts = get(TEZ_DAG_VERTEX_JAVAOPTS + "." + vertexName);
	  return opts == null? "" : opts;
  }
  
  @Private
  public void setVertexJavaOpts(String vertexName, String javaOpts){
	  set(TEZ_DAG_VERTEX_JAVAOPTS + "." + vertexName, javaOpts);
  }

  /// File used for storing location hints that are passed to the DAG
  public static final String DAG_LOCATION_HINT_RESOURCE_FILE =
      DAG + "location-hint-resource-file";
  public static final String DEFAULT_DAG_LOCATION_HINT_RESOURCE_FILE =
      "tezdaglocationhint.info";

}
