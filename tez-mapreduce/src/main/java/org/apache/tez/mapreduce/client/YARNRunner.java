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

package org.apache.tez.mapreduce.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
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
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.runtime.library.input.ShuffledMergedInputLegacy;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class enables the current JobClient (0.22 hadoop) to run on YARN-TEZ.
 */
@SuppressWarnings({ "unchecked" })
public class YARNRunner implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  private ResourceMgrDelegate resMgrDelegate;
  private ClientCache clientCache;
  private Configuration conf;
  private final FileContext defaultFileContext;

  final public static FsPermission DAG_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644);
  final public static int UTF8_CHUNK_SIZE = 16 * 1024;

  private final TezConfiguration tezConf;
  private TezClient tezSession;
  private DAGClient dagClient;

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
    this.tezConf = new TezConfiguration(conf);
    try {
      this.resMgrDelegate = resMgrDelegate;
      this.clientCache = clientCache;
      this.defaultFileContext = FileContext.getFileContext(this.conf);

    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
  }

  @VisibleForTesting
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
        MRJobConfig.JOB_SPLIT_METAINFO}) {
      localResources.put(s,
          createApplicationResource(defaultFileContext,
              new Path(jobSubmitDir, s), LocalResourceType.FILE));
    }

    MRApps.setupDistributedCache(jobConf, localResources);

    return localResources;
  }

  // FIXME isn't this a nice mess of a client?
  // read input, write splits, read splits again
  private List<TaskLocationHint> getMapLocationHintsFromInputSplits(JobID jobId,
      FileSystem fs, Configuration conf,
      String jobSubmitDir) throws IOException {
    TaskSplitMetaInfo[] splitsInfo =
        SplitMetaInfoReader.readSplitMetaInfo(jobId, fs, conf,
            new Path(jobSubmitDir));
    int splitsCount = splitsInfo.length;
    List<TaskLocationHint> locationHints =
        new ArrayList<TaskLocationHint>(splitsCount);
    for (int i = 0; i < splitsCount; ++i) {
      TaskLocationHint locationHint =
          new TaskLocationHint(
              new HashSet<String>(
                  Arrays.asList(splitsInfo[i].getLocations())), null);
      locationHints.add(locationHint);
    }
    return locationHints;
  }

  private void setupMapReduceEnv(Configuration jobConf,
      Map<String, String> environment, boolean isMap) throws IOException {

    if (isMap) {
      warnForJavaLibPath(
          jobConf.get(MRJobConfig.MAP_JAVA_OPTS,""),
          "map",
          MRJobConfig.MAP_JAVA_OPTS,
          MRJobConfig.MAP_ENV);
      warnForJavaLibPath(
          jobConf.get(MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,""),
          "map",
          MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,
          MRJobConfig.MAPRED_ADMIN_USER_ENV);
    } else {
      warnForJavaLibPath(
          jobConf.get(MRJobConfig.REDUCE_JAVA_OPTS,""),
          "reduce",
          MRJobConfig.REDUCE_JAVA_OPTS,
          MRJobConfig.REDUCE_ENV);
      warnForJavaLibPath(
          jobConf.get(MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,""),
          "reduce",
          MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,
          MRJobConfig.MAPRED_ADMIN_USER_ENV);
    }

    MRHelpers.updateEnvironmentForMRTasks(jobConf, environment, isMap);
  }

  private Vertex createVertexForStage(Configuration stageConf,
      Map<String, LocalResource> jobLocalResources,
      List<TaskLocationHint> locations, int stageNum, int totalStages)
      throws IOException {
    // stageNum starts from 0, goes till numStages - 1
    boolean isMap = false;
    if (stageNum == 0) {
      isMap = true;
    }

    int numTasks = isMap ? stageConf.getInt(MRJobConfig.NUM_MAPS, 0)
        : stageConf.getInt(MRJobConfig.NUM_REDUCES, 0);
    String processorName = isMap ? MapProcessor.class.getName()
        : ReduceProcessor.class.getName();
    String vertexName = null;
    if (isMap) {
      vertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    } else {
      if (stageNum == totalStages - 1) {
        vertexName = MultiStageMRConfigUtil.getFinalReduceVertexName();
      } else {
        vertexName = MultiStageMRConfigUtil
            .getIntermediateStageVertexName(stageNum);
      }
    }

    Resource taskResource = isMap ? MRHelpers.getMapResource(stageConf)
        : MRHelpers.getReduceResource(stageConf);
    
    stageConf.set(MRJobConfig.MROUTPUT_FILE_NAME_PREFIX, "part");
    
    byte[] vertexUserPayload = MRHelpers.createUserPayloadFromConf(stageConf);
    Vertex vertex = new Vertex(vertexName, new ProcessorDescriptor(processorName).
        setUserPayload(vertexUserPayload),
        numTasks, taskResource);
    if (isMap) {
      byte[] mapInputPayload = MRHelpers.createMRInputPayload(vertexUserPayload, null);
      MRHelpers.addMRInput(vertex, mapInputPayload, null);
    }
    // Map only jobs.
    if (stageNum == totalStages -1) {
      MRHelpers.addMROutputLegacy(vertex, vertexUserPayload);
    }

    Map<String, String> taskEnv = new HashMap<String, String>();
    setupMapReduceEnv(stageConf, taskEnv, isMap);

    Map<String, LocalResource> taskLocalResources =
        new TreeMap<String, LocalResource>();
    // PRECOMMIT Remove split localization for reduce tasks if it's being set
    // here
    taskLocalResources.putAll(jobLocalResources);

    String taskJavaOpts = isMap ? MRHelpers.getMapJavaOpts(stageConf)
        : MRHelpers.getReduceJavaOpts(stageConf);

    vertex.setTaskEnvironment(taskEnv)
        .setTaskLocalResources(taskLocalResources)
        .setTaskLocationsHint(locations)
        .setJavaOpts(taskJavaOpts);
    
    if (!isMap) {
      vertex.setVertexManagerPlugin(new VertexManagerPluginDescriptor(
          ShuffleVertexManager.class.getName()));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding vertex to DAG" + ", vertexName="
          + vertex.getVertexName() + ", processor="
          + vertex.getProcessorDescriptor().getClassName() + ", parallelism="
          + vertex.getParallelism() + ", javaOpts=" + vertex.getJavaOpts()
          + ", resources=" + vertex.getTaskResource()
      // TODO Add localResources and Environment
      );
    }

    return vertex;
  }

  private DAG createDAG(FileSystem fs, JobID jobId, Configuration[] stageConfs,
      String jobSubmitDir, Credentials ts,
      Map<String, LocalResource> jobLocalResources) throws IOException {

    String jobName = stageConfs[0].get(MRJobConfig.JOB_NAME,
        YarnConfiguration.DEFAULT_APPLICATION_NAME);
    DAG dag = new DAG(jobName);

    LOG.info("Number of stages: " + stageConfs.length);

    List<TaskLocationHint> mapInputLocations =
        getMapLocationHintsFromInputSplits(
            jobId, fs, stageConfs[0], jobSubmitDir);
    List<TaskLocationHint> reduceInputLocations = null;

    Vertex[] vertices = new Vertex[stageConfs.length];
    for (int i = 0; i < stageConfs.length; i++) {
      vertices[i] = createVertexForStage(stageConfs[i], jobLocalResources,
          i == 0 ? mapInputLocations : reduceInputLocations, i,
          stageConfs.length);
    }

    for (int i = 0; i < vertices.length; i++) {
      dag.addVertex(vertices[i]);
      if (i > 0) {
        EdgeProperty edgeProperty = new EdgeProperty(
            DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(OnFileSortedOutput.class.getName()),
            new InputDescriptor(ShuffledMergedInputLegacy.class.getName()));

        Edge edge = null;
        edge = new Edge(vertices[i - 1], vertices[i], edgeProperty);
        dag.addEdge(edge);
      }

    }
    return dag;
  }

  private TezConfiguration getDAGAMConfFromMRConf() {
    TezConfiguration finalConf = new TezConfiguration(this.tezConf);
    Map<String, String> mrParamToDAGParamMap = DeprecatedKeys
        .getMRToDAGParamMap();

    for (Entry<String, String> entry : mrParamToDAGParamMap.entrySet()) {
      if (finalConf.get(entry.getKey()) != null) {
        finalConf.set(entry.getValue(), finalConf.get(entry.getKey()));
        finalConf.unset(entry.getKey());
        if (LOG.isDebugEnabled()) {
          LOG.debug("MR->DAG Translating MR key: " + entry.getKey()
              + " to Tez key: " + entry.getValue() + " with value "
              + finalConf.get(entry.getValue()));
        }
      }
    }
    return finalConf;
  }

  private void maybeKillSession() throws IOException {
    String sessionAppIdToKill = conf.get("mapreduce.tez.session.tokill-application-id");
    if (sessionAppIdToKill != null) {
      ApplicationId killAppId = ConverterUtils.toApplicationId(sessionAppIdToKill);
      try {
        resMgrDelegate.killApplication(killAppId);
      } catch (YarnException e) {
        throw new IOException("Failed while killing Session AppId", e);
      }
    }
  }
  
  @Override
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
  throws IOException, InterruptedException {

    // HACK! TEZ-604. Get rid of this once Hive moves all of it's tasks over to Tez native.
    maybeKillSession();
    
    ApplicationId appId = resMgrDelegate.getApplicationId();

    FileSystem fs = FileSystem.get(conf);
    // Loads the job.xml written by the user.
    JobConf jobConf = new JobConf(new TezConfiguration(conf));

    // Extract individual raw MR configs.
    Configuration[] stageConfs = MultiStageMRConfToTezTranslator
        .getStageConfs(jobConf);

    // Transform all confs to use Tez keys
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(stageConfs[0],
        null);
    for (int i = 1; i < stageConfs.length; i++) {
      MultiStageMRConfToTezTranslator.translateVertexConfToTez(stageConfs[i],
          stageConfs[i - 1]);
    }

    // create inputs to tezClient.submit()

    // FIXME set up job resources
    Map<String, LocalResource> jobLocalResources =
        createJobLocalResources(stageConfs[0], jobSubmitDir);

    // FIXME createDAG should take the tezConf as a parameter, instead of using
    // MR keys.
    DAG dag = createDAG(fs, jobId, stageConfs, jobSubmitDir, ts,
        jobLocalResources);

    List<String> vargs = new LinkedList<String>();
    // admin command opts and user command opts
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

    StringBuilder javaOpts = new StringBuilder();
    for (String varg : vargs) {
      javaOpts.append(varg).append(" ");
    }

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    Map<String, String> environment = new HashMap<String, String>();

    // Setup the environment variables for AM
    MRHelpers.updateEnvironmentForMRAM(conf, environment);
    StringBuilder envStrBuilder = new StringBuilder();
    boolean first = true;
    for (Entry<String, String> entry : environment.entrySet()) {
      if (!first) {
        envStrBuilder.append(",");
      } else {
        first = false;
      }
      envStrBuilder.append(entry.getKey()).append("=").append(entry.getValue());
    }
    String envStr = envStrBuilder.toString();

    TezConfiguration dagAMConf = getDAGAMConfFromMRConf();
    dagAMConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, javaOpts.toString());
    if (envStr.length() > 0) {
      dagAMConf.set(TezConfiguration.TEZ_AM_LAUNCH_ENV, envStr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting MR AM env to : " + envStr);
      }
    }

    // Submit to ResourceManager
    try {
      dagAMConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
          jobSubmitDir);
      
      // Set Tez parameters based on MR parameters.
      String queueName = jobConf.get(JobContext.QUEUE_NAME,
          YarnConfiguration.DEFAULT_QUEUE_NAME);
      dagAMConf.set(TezConfiguration.TEZ_QUEUE_NAME, queueName);
      
      int amMemMB = jobConf.getInt(MRJobConfig.MR_AM_VMEM_MB, MRJobConfig.DEFAULT_MR_AM_VMEM_MB);
      int amCores = jobConf.getInt(MRJobConfig.MR_AM_CPU_VCORES, MRJobConfig.DEFAULT_MR_AM_CPU_VCORES);
      dagAMConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, amMemMB);
      dagAMConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES, amCores);

      dagAMConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 
          jobConf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));
      
      tezSession = new TezClient("MapReduce", dagAMConf, false, jobLocalResources, ts);
      tezSession.start();
      tezSession.submitDAGApplication(appId, dag);
      tezSession.stop();
    } catch (TezException e) {
      throw new IOException(e);
    }

    return getJobStatus(jobId);
  }

  private LocalResource createApplicationResource(FileContext fs, Path p,
      LocalResourceType type) throws IOException {
    LocalResource rsrc = Records.newRecord(LocalResource.class);
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
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    String jobFile = MRApps.getJobFile(conf, user, jobID);
    DAGStatus dagStatus;
    try {
      if(dagClient == null) {
        dagClient = TezClient.getDAGClient(TypeConverter.toYarn(jobID).getAppId(), tezConf);
      }
      dagStatus = dagClient.getDAGStatus(null);
      return new DAGJobStatus(dagClient.getApplicationReport(), dagStatus, jobFile);
    } catch (TezException e) {
      throw new IOException(e);
    }
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
    JobStatus status = getJobStatus(arg0);
    if (status.getState() == JobStatus.State.RUNNING ||
        status.getState() == JobStatus.State.PREP) {
      try {
        resMgrDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
      } catch (YarnException e) {
        throw new IOException(e);
      }
      return;
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
    try {
      return clientCache.getClient(jobID).getLogFilePath(jobID, taskAttemptID);
    } catch (YarnException e) {
      throw new IOException(e);
    }
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
