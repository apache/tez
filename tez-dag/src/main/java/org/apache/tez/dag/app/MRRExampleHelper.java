package org.apache.tez.dag.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.tez.dag.api.DAGConfiguration;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty.SourceType;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.app.rm.container.AMContainerHelpers;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

public class MRRExampleHelper {

  private static final Log LOG = LogFactory.getLog(MRRExampleHelper.class);
  
  //TODO remove once client is in place
 private static Path getMRBaseDir() throws IOException {
   Path basePath = MRApps.getStagingAreaDir(new Configuration(),
       UserGroupInformation.getCurrentUser().getShortUserName());
   return new Path(basePath, "dagTest");
 }

 private static Path getMRRBaseDir() throws IOException {
   Path basePath = MRApps.getStagingAreaDir(new Configuration(),
       UserGroupInformation.getCurrentUser().getShortUserName());
   return new Path(basePath, "mrrTest");
 }

 private static String getConfFileName(String vertexName) {
   return MRJobConfig.JOB_CONF_FILE + "_" + vertexName;
 }

 // TODO remove once client is in place
 private static Map<String, LocalResource> createLocalResources(
     Path remoteBaseDir, String[] resourceNames) throws IOException {
   Configuration conf = new Configuration();
   FileSystem fs = FileSystem.get(conf);

   Map<String, LocalResource> localResources = new TreeMap<String, LocalResource>();

   for (String resourceName : resourceNames) {
     Path remoteFile = new Path(remoteBaseDir, resourceName);
     localResources.put(resourceName, AMContainerHelpers.createLocalResource(
         fs, remoteFile, LocalResourceType.FILE,
         LocalResourceVisibility.APPLICATION));
     LOG.info("Localizing file " + resourceName + " from location "
         + remoteFile.toString());
   }
   return localResources;
 }


 private static String[] getMRLocalRsrcList() {
   String[] resourceNames = new String[] { MRJobConfig.JOB_JAR,
       MRJobConfig.JOB_SPLIT, MRJobConfig.JOB_SPLIT_METAINFO,
       MRJobConfig.JOB_CONF_FILE };
   return resourceNames;
 }

 private static String[] getMRRLocalRsrcList() {
   String[] resourceNames = new String[] { MRJobConfig.JOB_JAR,
       MRJobConfig.JOB_SPLIT, MRJobConfig.JOB_SPLIT_METAINFO,
       MRJobConfig.JOB_CONF_FILE, getConfFileName("reduce1"),
       getConfFileName("reduce2") };
   return resourceNames;
 }

 static Configuration createDAGConfigurationForMRR() throws IOException {
   org.apache.tez.dag.api.DAG dag = new org.apache.tez.dag.api.DAG();
   Vertex mapVertex = new Vertex("map",
       "org.apache.tez.mapreduce.task.InitialTask", 6);
   Vertex reduce1Vertex = new Vertex("reduce1",
       "org.apache.tez.mapreduce.task.IntermediateTask", 3);
   Vertex reduce2Vertex = new Vertex("reduce2",
       "org.apache.tez.mapreduce.task.FinalTask", 3);
   Edge edge1 = new Edge(mapVertex, reduce1Vertex,
       new EdgeProperty(ConnectionPattern.BIPARTITE,
           SourceType.STABLE,
           ShuffledMergedInput.class.getName(),
           OnFileSortedOutput.class.getName()));
   Edge edge2 = new Edge(reduce1Vertex, reduce2Vertex,
       new EdgeProperty(ConnectionPattern.BIPARTITE,
           SourceType.STABLE,
           ShuffledMergedInput.class.getName(),
           OnFileSortedOutput.class.getName()));
   Map<String, LocalResource> jobRsrcs = createLocalResources(getMRRBaseDir(),
       getMRRLocalRsrcList());

   Map<String, LocalResource> mapRsrcs = new HashMap<String, LocalResource>();
   Map<String, LocalResource> reduce1Rsrcs = new HashMap<String, LocalResource>();
   Map<String, LocalResource> reduce2Rsrcs = new HashMap<String, LocalResource>();

   mapRsrcs.put(MRJobConfig.JOB_SPLIT, jobRsrcs.get(MRJobConfig.JOB_SPLIT));
   mapRsrcs.put(MRJobConfig.JOB_SPLIT_METAINFO, jobRsrcs.get(MRJobConfig.JOB_SPLIT_METAINFO));
   mapRsrcs.put(MRJobConfig.JOB_JAR, jobRsrcs.get(MRJobConfig.JOB_JAR));
   mapRsrcs.put(MRJobConfig.JOB_CONF_FILE, jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));
   mapRsrcs.put(getConfFileName("map"), jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));

   reduce1Rsrcs.put(MRJobConfig.JOB_JAR, jobRsrcs.get(MRJobConfig.JOB_JAR));
   reduce1Rsrcs.put(MRJobConfig.JOB_CONF_FILE, jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));
   reduce1Rsrcs.put(getConfFileName("reduce1"), jobRsrcs.get(getConfFileName("reduce1")));

   reduce2Rsrcs.put(MRJobConfig.JOB_JAR, jobRsrcs.get(MRJobConfig.JOB_JAR));
   reduce2Rsrcs.put(MRJobConfig.JOB_CONF_FILE, jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));
   reduce2Rsrcs.put(getConfFileName("reduce2"), jobRsrcs.get(getConfFileName("reduce2")));

   Resource mapResource = BuilderUtils.newResource(
        MRJobConfig.DEFAULT_MAP_MEMORY_MB,
        MRJobConfig.DEFAULT_MAP_CPU_VCORES);
   mapVertex.setTaskResource(mapResource);
   mapVertex.setTaskLocalResources(mapRsrcs);
   Resource reduceResource = BuilderUtils.newResource(
       MRJobConfig.DEFAULT_REDUCE_MEMORY_MB,
       MRJobConfig.DEFAULT_REDUCE_CPU_VCORES);
   reduce1Vertex.setTaskResource(reduceResource);
   reduce1Vertex.setTaskLocalResources(reduce1Rsrcs);

   reduce1Vertex.setTaskResource(reduceResource);
   reduce2Vertex.setTaskLocalResources(reduce2Rsrcs);

   dag.addVertex(mapVertex);
   dag.addVertex(reduce1Vertex);
   dag.addVertex(reduce2Vertex);
   dag.addEdge(edge1);
   dag.addEdge(edge2);
   dag.verify();
   DAGConfiguration dagConf = dag.serializeDag();

   dagConf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
   dagConf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

   return dagConf;
 }

 // TODO remove once client is in place
 static Configuration createDAGConfigurationForMR() throws IOException {
   org.apache.tez.dag.api.DAG dag = new org.apache.tez.dag.api.DAG();
   Vertex mapVertex = new Vertex("map",
       "org.apache.tez.mapreduce.task.InitialTask", 6);
   Vertex reduceVertex = new Vertex("reduce",
       "org.apache.tez.mapreduce.task.FinalTask", 1);
   Edge edge = new Edge(mapVertex, reduceVertex,
       new EdgeProperty(ConnectionPattern.BIPARTITE,
           SourceType.STABLE,
           ShuffledMergedInput.class.getName(),
           OnFileSortedOutput.class.getName()));

   Map<String, LocalResource> jobRsrcs = createLocalResources(getMRBaseDir(),
       getMRLocalRsrcList());

   Map<String, LocalResource> mapRsrcs = new HashMap<String, LocalResource>();
   Map<String, LocalResource> reduceRsrcs = new HashMap<String, LocalResource>();

   mapRsrcs.put(MRJobConfig.JOB_SPLIT, jobRsrcs.get(MRJobConfig.JOB_SPLIT));
   mapRsrcs.put(MRJobConfig.JOB_SPLIT_METAINFO, jobRsrcs.get(MRJobConfig.JOB_SPLIT_METAINFO));
   mapRsrcs.put(MRJobConfig.JOB_JAR, jobRsrcs.get(MRJobConfig.JOB_JAR));
   mapRsrcs.put(MRJobConfig.JOB_CONF_FILE, jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));
   mapRsrcs.put(getConfFileName("map"), jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));

   reduceRsrcs.put(MRJobConfig.JOB_JAR, jobRsrcs.get(MRJobConfig.JOB_JAR));
   reduceRsrcs.put(MRJobConfig.JOB_CONF_FILE, jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));
   reduceRsrcs.put(getConfFileName("reduce"), jobRsrcs.get(MRJobConfig.JOB_CONF_FILE));

   Resource mapResource = BuilderUtils.newResource(
        MRJobConfig.DEFAULT_MAP_MEMORY_MB,
        MRJobConfig.DEFAULT_MAP_CPU_VCORES);
   mapVertex.setTaskResource(mapResource);
   mapVertex.setTaskLocalResources(mapRsrcs);
   Resource reduceResource = BuilderUtils.newResource(
       MRJobConfig.DEFAULT_REDUCE_MEMORY_MB,
       MRJobConfig.DEFAULT_REDUCE_CPU_VCORES);
   reduceVertex.setTaskResource(reduceResource);
   reduceVertex.setTaskLocalResources(reduceRsrcs);
   dag.addVertex(mapVertex);
   dag.addVertex(reduceVertex);
   dag.addEdge(edge);
   dag.verify();
   DAGConfiguration dagConf = dag.serializeDag();

   dagConf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
   dagConf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

   return dagConf;
 }
  
}
