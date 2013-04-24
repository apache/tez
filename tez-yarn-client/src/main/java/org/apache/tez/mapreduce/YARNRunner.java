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

package org.apache.tez.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DAGConfiguration;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class enables the current JobClient (0.22 hadoop) to run on YARN-TEZ.
 */
@SuppressWarnings({ "unchecked" })
public class YARNRunner implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private ResourceMgrDelegate resMgrDelegate;
  private ClientCache clientCache;
  private Configuration conf;
  private final FileContext defaultFileContext;

  private static final Object classpathLock = new Object();
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;

  final public static FsPermission DAG_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644);

  /**
   * Yarn runner incapsulates the client interface of
   * yarn
   * @param conf the configuration object for the client
   */
  public YARNRunner(Configuration conf) {
   this(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)));
  }

  /**
   * Similar to {@link #YARNRunner(Configuration)} but allowing injecting
   * {@link ResourceMgrDelegate}. Enables mocking and testing.
   * @param conf the configuration object for the client
   * @param resMgrDelegate the resourcemanager client handle.
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate) {
   this(conf, resMgrDelegate, new ClientCache(conf, resMgrDelegate));
  }

  /**
   * Similar to {@link YARNRunner#YARNRunner(Configuration, ResourceMgrDelegate)}
   * but allowing injecting {@link ClientCache}. Enable mocking and testing.
   * @param conf the configuration object
   * @param resMgrDelegate the resource manager delegate
   * @param clientCache the client cache object.
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate,
      ClientCache clientCache) {
    this.conf = conf;
    try {
      this.resMgrDelegate = resMgrDelegate;
      this.clientCache = clientCache;
      this.defaultFileContext = FileContext.getFileContext(this.conf);
    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
  }

  @Private
  /**
   * Used for testing mostly.
   * @param resMgrDelegate the resource manager delegate to set to.
   */
  public void setResourceMgrDelegate(ResourceMgrDelegate resMgrDelegate) {
    this.resMgrDelegate = resMgrDelegate;
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Use Token.renew instead");
  }

  @Override
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getActiveTrackers();
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    return resMgrDelegate.getAllJobs();
  }

  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getBlacklistedTrackers();
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    return resMgrDelegate.getClusterMetrics();
  }

  @VisibleForTesting
  Token<?> getDelegationTokenFromHS(MRClientProtocol hsProxy)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("No HistoryServer for Tez");
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException, InterruptedException {
    // The token is only used for serialization. So the type information
    // mismatch should be fine.
    return resMgrDelegate.getDelegationToken(renewer);
  }

  @Override
  public String getFilesystemName() throws IOException, InterruptedException {
    return resMgrDelegate.getFilesystemName();
  }

  @Override
  public JobID getNewJobID() throws IOException, InterruptedException {
    return resMgrDelegate.getNewJobID();
  }

  @Override
  public QueueInfo getQueue(String queueName) throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueue(queueName);
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueueAclsForCurrentUser();
  }

  @Override
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getQueues();
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getRootQueues();
  }

  @Override
  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    return resMgrDelegate.getChildQueues(parent);
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    return resMgrDelegate.getStagingAreaDir();
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    return resMgrDelegate.getSystemDir();
  }

  @Override
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return resMgrDelegate.getTaskTrackerExpiryInterval();
  }

  private Map<String, LocalResource> createJobLocalResources(
      Configuration jobConf, String jobSubmitDir)
      throws IOException {

    // Setup LocalResources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JOB_CONF_FILE);

    URL yarnUrlForJobSubmitDir = ConverterUtils
        .getYarnUrlFromPath(defaultFileContext.getDefaultFileSystem()
            .resolvePath(
                defaultFileContext.makeQualified(new Path(jobSubmitDir))));
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    localResources.put(MRJobConfig.JOB_CONF_FILE,
        createApplicationResource(defaultFileContext,
            jobConfPath, LocalResourceType.FILE));
    if (jobConf.get(MRJobConfig.JAR) != null) {
      Path jobJarPath = new Path(jobConf.get(MRJobConfig.JAR));
      LocalResource rc = createApplicationResource(defaultFileContext,
          jobJarPath,
          LocalResourceType.FILE);
      // FIXME fix pattern support
      // String pattern = conf.getPattern(JobContext.JAR_UNPACK_PATTERN,
      // JobConf.UNPACK_JAR_PATTERN_DEFAULT).pattern();
      // rc.setPattern(pattern);
      localResources.put(MRJobConfig.JOB_JAR, rc);
    } else {
      // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
      // mapreduce jar itself which is already on the classpath.
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }

    // TODO gross hack
    for (String s : new String[] {
        MRJobConfig.JOB_SPLIT,
        MRJobConfig.JOB_SPLIT_METAINFO,
        MRJobConfig.APPLICATION_TOKENS_FILE }) {
      localResources.put(s,
          createApplicationResource(defaultFileContext,
              new Path(jobSubmitDir, s), LocalResourceType.FILE));
    }

    MRApps.setupDistributedCache(jobConf, localResources);

    return localResources;
  }

  // FIXME isn't this a nice mess of a client?
  // read input, write splits, read splits again
  private TaskLocationHint[] getMapLocationHintsFromInputSplits(JobID jobId,
      FileSystem fs, Configuration conf,
      String jobSubmitDir) throws IOException {
    LOG.info("XXXX Reading splits information");
    TaskSplitMetaInfo[] splitsInfo =
        SplitMetaInfoReader.readSplitMetaInfo(jobId, fs, conf,
            new Path(jobSubmitDir));
    int splitsCount = splitsInfo.length;
    LOG.info("XXXX Found splits information, splitCount=" + splitsCount);
    TaskLocationHint[] locationHints =
        new TaskLocationHint[splitsCount];
    for (int i = 0; i < splitsCount; ++i) {
      TaskLocationHint locationHint =
          new TaskLocationHint(splitsInfo[i].getLocations(), null);
      locationHints[i] = locationHint;
    }
    return locationHints;
  }

  private static String getInitialClasspath(Configuration conf)
      throws IOException {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();
      MRApps.setClasspath(env, conf);
      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }

  private void setupCommonChildEnv(Configuration conf,
      Map<String, String> environment) throws IOException {

      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          getInitialClasspath(conf));

    // Shell
    environment.put(Environment.SHELL.name(), conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_SHELL, MRJobConfig.DEFAULT_SHELL));

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(),
        Environment.PWD.$());

    // Add the env variables passed by the admin
    Apps.setEnvFromInputString(environment, conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_ENV,
        MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV));

    // FIXME is this really required?
    // Add stdout/stderr env
    environment.put(
        MRJobConfig.STDOUT_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDOUT)
        );
    environment.put(
        MRJobConfig.STDERR_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDERR)
        );

  }

  private static String getChildEnv(Configuration jobConf, boolean isMap) {
    if (isMap) {
      return jobConf.get(MRJobConfig.MAP_ENV, "");
    }
    return jobConf.get(MRJobConfig.REDUCE_ENV, "");
  }

  private static String getChildLogLevel(Configuration conf, boolean isMap) {
    if (isMap) {
      return conf.get(
          MRJobConfig.MAP_LOG_LEVEL,
          JobConf.DEFAULT_LOG_LEVEL.toString()
          );
    } else {
      return conf.get(
          MRJobConfig.REDUCE_LOG_LEVEL,
          JobConf.DEFAULT_LOG_LEVEL.toString()
          );
    }
  }

  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR
        + filter.toString();
  }

  private static void setupLog4jProperties(Configuration jobConf,
      boolean isMap,
      Vector<String> vargs,
      long logSize) {
    String logLevel = getChildLogLevel(jobConf, isMap);
    MRApps.addLog4jSystemProperties(logLevel, logSize, vargs);
  }

  private void setupMapReduceEnv(Configuration jobConf,
      Map<String, String> environment, boolean isMap) throws IOException {

    if (isMap) {
      warnForJavaLibPath(
          conf.get(MRJobConfig.MAP_JAVA_OPTS,""),
          "map",
          MRJobConfig.MAP_JAVA_OPTS,
          MRJobConfig.MAP_ENV);
      warnForJavaLibPath(
          conf.get(MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,""),
          "map",
          MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,
          MRJobConfig.MAPRED_ADMIN_USER_ENV);
    } else {
      warnForJavaLibPath(
          conf.get(MRJobConfig.REDUCE_JAVA_OPTS,""),
          "reduce",
          MRJobConfig.REDUCE_JAVA_OPTS,
          MRJobConfig.REDUCE_ENV);
      warnForJavaLibPath(
          conf.get(MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,""),
          "reduce",
          MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,
          MRJobConfig.MAPRED_ADMIN_USER_ENV);
    }

    setupCommonChildEnv(jobConf, environment);

    // Add the env variables passed by the user
    String mapredChildEnv = getChildEnv(jobConf, isMap);
    Apps.setEnvFromInputString(environment, mapredChildEnv);

    // Set logging level in the environment.
    // This is so that, if the child forks another "bin/hadoop" (common in
    // streaming) it will have the correct loglevel.
    environment.put(
        "HADOOP_ROOT_LOGGER",
        getChildLogLevel(jobConf, isMap) + ",CLA");

    // FIXME: don't think this is also needed given we already set java
    // properties.
    // TODO Change this not to use JobConf.
    long logSize = TaskLog.getTaskLogLength(new JobConf(jobConf));
    Vector<String> logProps = new Vector<String>(4);
    setupLog4jProperties(jobConf, isMap, logProps, logSize);
    Iterator<String> it = logProps.iterator();
    StringBuffer buffer = new StringBuffer();
    while (it.hasNext()) {
      buffer.append(" " + it.next());
    }

    // FIXME supposedly required for streaming, should we remove it and let
    // YARN set it for all containers?
    String hadoopClientOpts = System.getenv("HADOOP_CLIENT_OPTS");
    if (hadoopClientOpts == null) {
      hadoopClientOpts = "";
    } else {
      hadoopClientOpts = hadoopClientOpts + " ";
    }
    hadoopClientOpts = hadoopClientOpts + buffer.toString();
    // environment.put("HADOOP_CLIENT_OPTS", hadoopClientOpts);

    // FIXME for this to work, we need YARN-561 and the task runtime changed
    // to use YARN-561

    addTezClasspathToEnv(conf, environment);
    Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
        getInitialClasspath(conf));

    LOG.info("XXXX Dumping out env for child, isMap=" + isMap);
    for (Map.Entry<String, String> entry : environment.entrySet()) {
      LOG.info("XXXX env entry: "
          + entry.getKey()
          + "=" + entry.getValue());
    }
  }
  
  private Vertex configureReduceStage(FileSystem fs, JobID jobId,
      Configuration jobConf, String jobSubmitDir, Credentials ts,
      Map<String, LocalResource> jobLocalResources, int iReduceIndex)
      throws IOException {
    int stageNum = iReduceIndex + 1;
    Configuration conf = MultiStageMRConfigUtil.getIntermediateStageConf(jobConf, stageNum);
    int numTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
    Vertex vertex = new Vertex(
        MultiStageMRConfigUtil.getIntermediateReduceVertexName(stageNum),
        "org.apache.tez.mapreduce.task.IntermediateTask", numTasks);
    
    Map<String, String> reduceEnv = new HashMap<String, String>();
    setupMapReduceEnv(conf, reduceEnv, false);

    Resource reduceResource = BuilderUtils.newResource(conf.getInt(
        MRJobConfig.REDUCE_MEMORY_MB, MRJobConfig.DEFAULT_REDUCE_MEMORY_MB),
        conf.getInt(MRJobConfig.REDUCE_CPU_VCORES,
            MRJobConfig.DEFAULT_REDUCE_CPU_VCORES));

    Map<String, LocalResource> reduceLocalResources = new TreeMap<String, LocalResource>();
    reduceLocalResources.putAll(jobLocalResources);
    // TODO MRR Don't bother localizing the input splits for the reduce vertices.

    vertex.setTaskEnvironment(reduceEnv);
    vertex.setTaskLocalResources(reduceLocalResources);
    vertex.setTaskLocationsHint(null);
    vertex.setTaskResource(reduceResource);

    return vertex;
  }

  private Vertex[] configureMultStageMRR(FileSystem fs, JobID jobId,
      JobConf jobConf, String jobSubmitDir, Credentials ts,
      Map<String, LocalResource> jobLocalResources, DAG dag) throws IOException {

    int numIntermediateStages = MultiStageMRConfigUtil
        .getNumIntermediateStages(jobConf);

    Vertex[] vertices = new Vertex[numIntermediateStages];

    for (int i = 0; i < numIntermediateStages; i++) {
      vertices[i] = configureReduceStage(fs, jobId, jobConf, jobSubmitDir, ts,
          jobLocalResources, i);
      dag.addVertex(vertices[i]);
    }
    return vertices;
  }

  private DAG createDAG(FileSystem fs, JobID jobId, JobConf jobConf,
      String jobSubmitDir, Credentials ts,
      Map<String, LocalResource> jobLocalResources) throws IOException {

    DAG dag = new DAG();

    int numMaps = jobConf.getInt(MRJobConfig.NUM_MAPS, 0);
    int numReduces = jobConf.getInt(MRJobConfig.NUM_REDUCES, 0);
    int intermediateReduces = jobConf.getInt(
        MRJobConfig.MRR_INTERMEDIATE_STAGES, 0);

    boolean mapOnly =
        (numMaps > 0 && numReduces == 0 && intermediateReduces == 0);
    boolean isMRR = (intermediateReduces > 0);

    LOG.info("XXXX Parsing job config"
        + ", numMaps=" + numMaps
        + ", numReduces=" + numReduces
        + ", intermediateReduces=" + intermediateReduces);

    // configure map vertex
    String mapProcessor = mapOnly ?
        "org.apache.tez.mapreduce.task.MapOnlyTask"
        : "org.apache.tez.mapreduce.task.InitialTask";
    Vertex mapVertex = new Vertex("map", mapProcessor, numMaps);

    // FIXME set up map environment
    Map<String, String> mapEnv = new HashMap<String, String>();
    setupMapReduceEnv(jobConf, mapEnv, true);

    TaskLocationHint[] inputSplitLocations =
        getMapLocationHintsFromInputSplits(jobId, fs, jobConf, jobSubmitDir);

    Resource mapResource = BuilderUtils.newResource(
        jobConf.getInt(MRJobConfig.MAP_MEMORY_MB,
            MRJobConfig.DEFAULT_MAP_MEMORY_MB),
        jobConf.getInt(MRJobConfig.MAP_CPU_VCORES,
            MRJobConfig.DEFAULT_MAP_CPU_VCORES));

    Map<String, LocalResource> mapLocalResources =
        new TreeMap<String, LocalResource>();
    mapLocalResources.putAll(jobLocalResources);

    mapVertex.setTaskEnvironment(mapEnv);
    mapVertex.setTaskLocalResources(mapLocalResources);
    mapVertex.setTaskLocationsHint(inputSplitLocations);
    mapVertex.setTaskResource(mapResource);

    LOG.info("XXXX Adding map vertex to DAG"
        + ", vertexName=" + mapVertex.getVertexName()
        + ", processor=" + mapVertex.getProcessorName()
        + ", parrellism=" + mapVertex.getParallelism());
    dag.addVertex(mapVertex);

    Vertex[] intermediateVertices = null;
    // configure intermediate reduces
    if (isMRR) {
      intermediateVertices = configureMultStageMRR(fs, jobId, jobConf,
          jobSubmitDir, ts, jobLocalResources, dag);
    }

    // configure final reduce vertex
    if (numReduces > 0) {
      String reduceProcessor =
          "org.apache.tez.mapreduce.task.FinalTask";
      Vertex reduceVertex = new Vertex("reduce", reduceProcessor, numReduces);

      // FIXME set up reduce environment
      Map<String, String> reduceEnv = new HashMap<String, String>();
      setupMapReduceEnv(jobConf, reduceEnv, false);

      Resource reduceResource = BuilderUtils.newResource(
          jobConf.getInt(MRJobConfig.REDUCE_MEMORY_MB,
              MRJobConfig.DEFAULT_REDUCE_MEMORY_MB),
          jobConf.getInt(MRJobConfig.REDUCE_CPU_VCORES,
              MRJobConfig.DEFAULT_REDUCE_CPU_VCORES));

      Map<String, LocalResource> reduceLocalResources =
          new TreeMap<String, LocalResource>();
      reduceLocalResources.putAll(jobLocalResources);

      reduceVertex.setTaskEnvironment(reduceEnv);
      reduceVertex.setTaskLocalResources(reduceLocalResources);
      reduceVertex.setTaskLocationsHint(null);
      reduceVertex.setTaskResource(reduceResource);

      LOG.info("XXXX Adding reduce vertex to DAG"
          + ", vertexName=" + reduceVertex.getVertexName()
          + ", processor=" + reduceVertex.getProcessorName()
          + ", parrellism=" + reduceVertex.getParallelism());
      dag.addVertex(reduceVertex);

      EdgeProperty edgeProperty = new EdgeProperty();
      Edge edge = null;
      if (!isMRR) {
        edge = new Edge(mapVertex, reduceVertex, edgeProperty);
        dag.addEdge(edge);
      } else {

        Edge initialEdge = new Edge(mapVertex, intermediateVertices[0],
            edgeProperty);
        dag.addEdge(initialEdge);

        int numIntermediateEdges = intermediateVertices.length - 1;
        for (int i = 0; i < numIntermediateEdges; i++) {
          Edge iEdge = new Edge(intermediateVertices[i],
              intermediateVertices[i + 1], edgeProperty);
          dag.addEdge(iEdge);
        }

        Edge finalEdge = new Edge(
            intermediateVertices[intermediateVertices.length - 1],
            reduceVertex, edgeProperty);
        dag.addEdge(finalEdge);
      }
      
    }

    return dag;
  }

  private void addTezClasspathToEnv(Configuration conf,
      Map<String, String> environment) {
    for (String c : conf.getStrings(
        TezConfiguration.TEZ_APPLICATION_CLASSPATH,
        TezConfiguration.DEFAULT_TEZ_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(environment,
          ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
    }
  }

  private void setDAGParamsFromMRConf(DAG dag) {
    Configuration mrConf = this.conf;
    Map<String, String> mrParamToDAGParamMap = DeprecatedKeys.getMRToDAGParamMap();
    for (Entry<String, String> entry : mrParamToDAGParamMap.entrySet()) {
      if (mrConf.get(entry.getKey()) != null) {
        LOG.info("DEBUG: MR->DAG Setting new key: " + entry.getValue());
        dag.addConfiguration(entry.getValue(), mrConf.get(entry.getKey()));
      }
    }
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
      FileSystem fs, DAG dag,
      Configuration jobConf, String jobSubmitDir, Credentials ts,
      Map<String, LocalResource> jobLocalResources) throws IOException {

    ApplicationId applicationId = resMgrDelegate.getApplicationId();

    // Setup resource requirements
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(
        conf.getInt(TezConfiguration.DAG_AM_RESOURCE_MEMORY_MB,
            TezConfiguration.DEFAULT_DAG_AM_RESOURCE_MEMORY_MB));
    capability.setVirtualCores(
        conf.getInt(TezConfiguration.DAG_AM_RESOURCE_CPU_VCORES,
            TezConfiguration.DEFAULT_DAG_AM_RESOURCE_CPU_VCORES));
    LOG.debug("AppMaster capability = " + capability);

    // Setup security tokens
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens  = ByteBuffer.wrap(dob.getData(),
        0, dob.getLength());

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    // FIXME set up logging related properties
    // TODO -Dtez.root.logger??
    // MRApps.addLog4jSystemProperties(logLevel, logSize, vargs);

    // FIXME admin command opts and user command opts for tez?
    String mrAppMasterAdminOptions = conf.get(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_ADMIN_COMMAND_OPTS);
    warnForJavaLibPath(mrAppMasterAdminOptions, "app master",
        MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, MRJobConfig.MR_AM_ADMIN_USER_ENV);
    vargs.add(mrAppMasterAdminOptions);

    // Add AM user command opts
    String mrAppMasterUserOptions = conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS);
    warnForJavaLibPath(mrAppMasterUserOptions, "app master",
        MRJobConfig.MR_AM_COMMAND_OPTS, MRJobConfig.MR_AM_ENV);
    vargs.add(mrAppMasterUserOptions);

    vargs.add(MRJobConfig.APPLICATION_MASTER_CLASS);
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDERR);


    Vector<String> vargsFinal = new Vector<String>(8);
    // Final command
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.debug("Command to launch container for ApplicationMaster is : "
        + mergedCommand);

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    Map<String, String> environment = new HashMap<String, String>();
    addTezClasspathToEnv(conf, environment);
    Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
        getInitialClasspath(conf));

    // Setup the environment variables for Admin first
    MRApps.setEnvFromInputString(environment,
        conf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV));
    // Setup the environment variables (LD_LIBRARY_PATH, etc)
    MRApps.setEnvFromInputString(environment,
        conf.get(MRJobConfig.MR_AM_ENV));

    // FIXME remove this
    Map<String, LocalResource> localResources =
        new TreeMap<String, LocalResource>();

    localResources.putAll(jobLocalResources);

    setDAGParamsFromMRConf(dag);

    // FIXME add serialized dag conf
    DAGConfiguration dagConf = dag.serializeDag();
    
    Path dagConfFilePath = new Path(jobSubmitDir,
        TezConfiguration.DAG_AM_PLAN_CONFIG_XML);
    FSDataOutputStream dagConfOut =
        FileSystem.create(fs, dagConfFilePath,
            new FsPermission(DAG_FILE_PERMISSION));
    try {
      dagConf.writeXml(dagConfOut);
    } finally {
      dagConfOut.close();
    }
    localResources.put(TezConfiguration.DAG_AM_PLAN_CONFIG_XML,
        createApplicationResource(defaultFileContext,
            dagConfFilePath, LocalResourceType.FILE));

    // FIXME add tez conf if needed

    // FIXME are we using MR acls for tez?
    Map<ApplicationAccessType, String> acls
        = new HashMap<ApplicationAccessType, String>();

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer = BuilderUtils
        .newContainerLaunchContext(UserGroupInformation
            .getCurrentUser().getShortUserName(), localResources,
            environment, vargsFinal, null, securityTokens, acls);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId);                // ApplicationId
    appContext.setResource(capability);                        // resource
    appContext.setQueue(                                       // Queue name
        jobConf.get(JobContext.QUEUE_NAME,
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    appContext.setApplicationName(                             // Job name
        jobConf.get(JobContext.JOB_NAME,
        YarnConfiguration.DEFAULT_APPLICATION_NAME));
    appContext.setCancelTokensWhenComplete(
        conf.getBoolean(MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN, true));
    appContext.setAMContainerSpec(amContainer);         // AM Container

    return appContext;
  }

  @Override
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
  throws IOException, InterruptedException {

    // Upload only in security mode: TODO
    Path applicationTokensFile =
        new Path(jobSubmitDir, MRJobConfig.APPLICATION_TOKENS_FILE);
    try {
      ts.writeTokenStorageFile(applicationTokensFile, conf);
    } catch (IOException e) {
      throw new YarnException(e);
    }

    FileSystem fs = FileSystem.get(conf);
    JobConf jobConf = new JobConf(new TezConfiguration(conf));

    // FIXME set up job resources
    Map<String, LocalResource> jobLocalResources =
        createJobLocalResources(jobConf, jobSubmitDir);
    DAG dag = createDAG(fs, jobId, jobConf, jobSubmitDir, ts,
        jobLocalResources);
    ApplicationSubmissionContext appContext =
        createApplicationSubmissionContext(fs, dag, jobConf, jobSubmitDir, ts,
            jobLocalResources);

    // Submit to ResourceManager
    ApplicationId applicationId = resMgrDelegate.submitApplication(appContext);

    ApplicationReport appMasterReport = resMgrDelegate
        .getApplicationReport(applicationId);
    String diagnostics =
        (appMasterReport == null ?
            "application report is null" : appMasterReport.getDiagnostics());
    if (appMasterReport == null
        || appMasterReport.getYarnApplicationState() == YarnApplicationState.FAILED
        || appMasterReport.getYarnApplicationState() == YarnApplicationState.KILLED) {
      throw new IOException("Failed to run job : " +
        diagnostics);
    }

    if (LOG.isDebugEnabled()) {
      while (true) {
        appMasterReport = resMgrDelegate
            .getApplicationReport(applicationId);
        diagnostics =
            (appMasterReport == null ?
                "application report is null"
                : appMasterReport.getDiagnostics());
        if (appMasterReport == null) {
          throw new IOException("Failed to run job : " +
            diagnostics);
        }
        YarnApplicationState state = appMasterReport.getYarnApplicationState();
        if (state.equals(YarnApplicationState.FAILED)
            || state.equals(YarnApplicationState.FINISHED)
            || state.equals(YarnApplicationState.KILLED)) {
          LOG.info("Job completed"
              + ", finalStatus=" + appMasterReport.getFinalApplicationStatus()
              + ", finalState=" + appMasterReport.getYarnApplicationState()
              + ", diagnostics=" + diagnostics);
          break;
        } else {
          LOG.info("Job in progress"
              + ", finalStatus=" + appMasterReport.getFinalApplicationStatus()
              + ", finalState=" + appMasterReport.getYarnApplicationState());
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    // FIXME
    return clientCache.getClient(jobId).getJobStatus(jobId);
  }

  private LocalResource createApplicationResource(FileContext fs, Path p, LocalResourceType type)
      throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
        .getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  @Override
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    resMgrDelegate.setJobPriority(arg0, arg1);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return resMgrDelegate.getProtocolVersion(arg0, arg1);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Use Token.renew instead");
  }


  @Override
  public Counters getJobCounters(JobID arg0) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0).getJobCounters(arg0);
  }

  @Override
  public String getJobHistoryDir() throws IOException, InterruptedException {
    return JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
  }

  @Override
  public JobStatus getJobStatus(JobID jobID) throws IOException,
      InterruptedException {
    JobStatus status = clientCache.getClient(jobID).getJobStatus(jobID);
    return status;
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    return clientCache.getClient(arg0).getTaskCompletionEvents(arg0, arg1, arg2);
  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0.getJobID()).getTaskDiagnostics(arg0);
  }

  @Override
  public TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
  throws IOException, InterruptedException {
    return clientCache.getClient(jobID)
        .getTaskReports(jobID, taskType);
  }

  @Override
  public void killJob(JobID arg0) throws IOException, InterruptedException {
    /* check if the status is not running, if not send kill to RM */
    JobStatus status = clientCache.getClient(arg0).getJobStatus(arg0);
    if (status.getState() != JobStatus.State.RUNNING) {
      resMgrDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
      return;
    }

    try {
      /* send a kill to the AM */
      clientCache.getClient(arg0).killJob(arg0);
      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L) && (status.getState()
          != JobStatus.State.KILLED)) {
          try {
            Thread.sleep(1000L);
          } catch(InterruptedException ie) {
            /** interrupted, just break */
            break;
          }
          currentTimeMillis = System.currentTimeMillis();
          status = clientCache.getClient(arg0).getJobStatus(arg0);
      }
    } catch(IOException io) {
      LOG.debug("Error when checking for application status", io);
    }
    if (status.getState() != JobStatus.State.KILLED) {
      resMgrDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
    }
  }

  @Override
  public boolean killTask(TaskAttemptID arg0, boolean arg1) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0.getJobID()).killTask(arg0, arg1);
  }

  @Override
  public AccessControlList getQueueAdmins(String arg0) throws IOException {
    return new AccessControlList("*");
  }

  @Override
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
      InterruptedException {
    return JobTrackerStatus.RUNNING;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion,
        clientMethodsHash);
  }

  @Override
  public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException {
    return clientCache.getClient(jobID).getLogFilePath(jobID, taskAttemptID);
  }

  private static void warnForJavaLibPath(String opts, String component,
      String javaConf, String envConf) {
    if (opts != null && opts.contains("-Djava.library.path")) {
      LOG.warn("Usage of -Djava.library.path in " + javaConf + " can cause " +
               "programs to no longer function if hadoop native libraries " +
               "are used. These values should be set as part of the " +
               "LD_LIBRARY_PATH in the " + component + " JVM env using " +
               envConf + " config settings.");
    }
  }
}
