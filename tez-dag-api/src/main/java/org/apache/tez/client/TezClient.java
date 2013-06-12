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

package org.apache.tez.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;

public class TezClient {
  private static final Log LOG = LogFactory.getLog(TezClient.class);

  final public static FsPermission TEZ_AM_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------
  final public static FsPermission TEZ_AM_FILE_PERMISSION = 
      FsPermission.createImmutable((short) 0644); // rw-r--r--
  
  private final TezConfiguration conf;
  private YarnClient yarnClient;
  
  public TezClient(TezConfiguration conf) {
    this.conf = conf;
    yarnClient = new YarnClientImpl();
    yarnClient.init(conf);
    yarnClient.start();
  }

  /**
   * Returns a <code>DAGClient</code> for the currently running attempt of a
   * Tez DAG application
   * @param appIdStr The application id of the app.
   * @return DAGClient if the app is running. null otherwise. 
   * @throws IOException
   * @throws TezException
   */
  public DAGClient getDAGClient(String appIdStr) throws IOException, TezUncheckedException {
    try {
      ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if(appReport.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        return null;
      }
      String host = appReport.getHost();
      int port = appReport.getRpcPort();
      return getDAGClient(host, port);
    } catch (YarnException e) {
      throw new TezUncheckedException(e);
    }
  }
  
  /**
   * Returns a <code>DAGClient</code> for a Tez application listening at the 
   * given host and port  
   * @param host
   * @param port
   * @return
   * @throws IOException
   */
  public DAGClient getDAGClient(String host, int port) throws IOException {
    InetSocketAddress addr = new InetSocketAddress(host, port);
    DAGClient dagClient;
    dagClient = new DAGClientRPCImpl(1, addr, conf);
    return dagClient;    
  }
  
  /**
   * Submit a Tez DAG to YARN as an application
   * 
   * @param dag <code>DAG</code> to be submitted
   * @param appStagingDir FileSystem path in which resources will be copied
   * @param ts Application credentials
   * @param amQueueName Queue to which the application will be submitted
   * @param amArgs Command line Java arguments for the ApplicationMaster
   * @param amEnv Environment to be added to the ApplicationMaster
   * @param amLocalResources YARN local resource for the ApplicationMaster
   * @return <code>ApplicationId</code> of the submitted Tez application
   * @throws IOException
   * @throws YarnException
   */
  public ApplicationId submitDAGApplication(DAG dag, Path appStagingDir,
      Credentials ts, String amQueueName, List<String> amArgs,
      Map<String, String> amEnv, Map<String, LocalResource> amLocalResources)
      throws IOException, YarnException {
    ApplicationId appId = createApplication();
    submitDAGApplication(appId, dag, appStagingDir, ts, amQueueName,
        amArgs, amEnv, amLocalResources);
    return appId;
  }

  /**
   * Submit a Tez DAG to YARN with known <code>ApplicationId</code>
   * 
   * @param appId - <code>ApplicationId</code> to be used
   * @param dag <code>DAG</code> to be submitted
   * @param appStagingDir FileSystem path in which resources will be copied
   * @param ts Application credentials
   * @param amQueueName Queue to which the application will be submitted
   * @param amArgs Command line Java arguments for the ApplicationMaster
   * @param amEnv Environment to be added to the ApplicationMaster
   * @param amLocalResources YARN local resource for the ApplicationMaster
   * @return <code>ApplicationId</code> of the submitted Tez application
   * @throws IOException
   * @throws YarnException
   */
  public void submitDAGApplication(ApplicationId appId, DAG dag,
      Path appStagingDir, Credentials ts, String amQueueName,
      List<String> amArgs, Map<String, String> amEnv,
      Map<String, LocalResource> amLocalResources) throws IOException,
      YarnException {
    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(
        appId, dag, appStagingDir, ts, amQueueName, dag.getName(), amArgs, amEnv,
        amLocalResources);

    yarnClient.submitApplication(appContext);
  }
  
  /**
   * Create a new YARN application
   * @return <code>ApplicationId</code> for the new YARN application
   * @throws YarnException
   * @throws IOException
   */
  public ApplicationId createApplication() throws YarnException, IOException {
    return yarnClient.getNewApplication().getApplicationId();
  }
  
  private void addLog4jSystemProperties(String logLevel,
      List<String> vargs) {
    vargs.add("-Dlog4j.configuration=container-log4j.properties");
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Setting this to 0 to avoid log size restrictions.
    // Should be enforced by YARN.
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_SIZE + "=" + 0);
    vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA");
  }

  private void addTezClasspathToEnv(Configuration conf,
      Map<String, String> environment) {
    for (String c : conf.getStrings(
        TezConfiguration.TEZ_APPLICATION_CLASSPATH,
        TezConfiguration.DEFAULT_TEZ_APPLICATION_CLASSPATH)) {
      // TEZ-194 - TezConfiguration.DEFAULT_TEZ_APPLICATION_CLASSPATH references
      // TEZ_HOME_ENV etc which is not really expanded nor defined as ENV_VARS
      Apps.addToEnvironment(environment,
          ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
    }
  }
  
  public FileSystem ensureExists(Path stagingArea)
      throws IOException {
    FileSystem fs = stagingArea.getFileSystem(conf);
    String realUser;
    String currentUser;
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    realUser = ugi.getShortUserName();
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    if (fs.exists(stagingArea)) {
      FileStatus fsStatus = fs.getFileStatus(stagingArea);
      String owner = fsStatus.getOwner();
      if (!(owner.equals(currentUser) || owner.equals(realUser))) {
        throw new IOException("The ownership on the staging directory "
            + stagingArea + " is not as expected. " + "It is owned by " + owner
            + ". The directory must " + "be owned by the submitter "
            + currentUser + " or " + "by " + realUser);
      }
      if (!fsStatus.getPermission().equals(TEZ_AM_DIR_PERMISSION)) {
        LOG.info("Permissions on staging directory " + stagingArea + " are "
            + "incorrect: " + fsStatus.getPermission()
            + ". Fixing permissions " + "to correct value "
            + TEZ_AM_DIR_PERMISSION);
        fs.setPermission(stagingArea, TEZ_AM_DIR_PERMISSION);
      }
    } else {
      fs.mkdirs(stagingArea, new FsPermission(TEZ_AM_DIR_PERMISSION));
    }
    return fs;
  }
  
  private LocalResource createApplicationResource(FileSystem fs, Path p,
      LocalResourceType type) throws IOException {
    LocalResource rsrc = Records.newRecord(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs.resolvePath(rsrcStat
        .getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
      ApplicationId appId, DAG dag, Path appStagingDir, Credentials ts,
      String amQueueName, String amName, List<String> amArgs,
      Map<String, String> amEnv, Map<String, LocalResource> amLocalResources)
      throws IOException, YarnException {

    FileSystem fs = ensureExists(appStagingDir);

    // Setup resource requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(
        conf.getInt(TezConfiguration.DAG_AM_RESOURCE_MEMORY_MB,
            TezConfiguration.DEFAULT_DAG_AM_RESOURCE_MEMORY_MB));
    capability.setVirtualCores(
        conf.getInt(TezConfiguration.DAG_AM_RESOURCE_CPU_VCORES,
            TezConfiguration.DEFAULT_DAG_AM_RESOURCE_CPU_VCORES));
    LOG.debug("AppMaster capability = " + capability);

    ByteBuffer securityTokens = null;
    // Setup security tokens
    if (ts != null) {
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      securityTokens = ByteBuffer.wrap(dob.getData(), 0,
          dob.getLength());
    }

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    String amLogLevel = conf.get(TezConfiguration.TEZ_AM_LOG_LEVEL,
                                 TezConfiguration.DEFAULT_TEZ_AM_LOG_LEVEL);
    addLog4jSystemProperties(amLogLevel, vargs);
    
    vargs.addAll(amArgs);

    vargs.add(TezConfiguration.DAG_APPLICATION_MASTER_CLASS);
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
    
    for (Map.Entry<String, String> entry : amEnv.entrySet()) {
      Apps.addToEnvironment(environment, entry.getKey(), entry.getValue());
    }

    Map<String, LocalResource> localResources =
        new TreeMap<String, LocalResource>();

    localResources.putAll(amLocalResources);
    
    // emit protobuf DAG file style
    Path binaryPath =  new Path(appStagingDir,
        TezConfiguration.DAG_AM_PLAN_PB_BINARY + "." + appId.toString());
    dag.addConfiguration(TezConfiguration.DAG_AM_PLAN_REMOTE_PATH,
        binaryPath.toUri().toString());
    DAGPlan dagPB = dag.createDag();
    FSDataOutputStream dagPBOutBinaryStream = null;
    
    try {
      //binary output
      dagPBOutBinaryStream = FileSystem.create(fs, binaryPath,
          new FsPermission(TEZ_AM_FILE_PERMISSION));
      dagPB.writeTo(dagPBOutBinaryStream);
    } finally {
      if(dagPBOutBinaryStream != null){
        dagPBOutBinaryStream.close();
      }
    }

    localResources.put(TezConfiguration.DAG_AM_PLAN_PB_BINARY,
        createApplicationResource(fs,
            binaryPath, LocalResourceType.FILE));

    Map<ApplicationAccessType, String> acls
        = new HashMap<ApplicationAccessType, String>();

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(localResources, environment,
            vargsFinal, null, securityTokens, acls);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);

    appContext.setApplicationId(appId);
    appContext.setResource(capability);
    appContext.setQueue(amQueueName);
    appContext.setApplicationName(amName);
    appContext.setCancelTokensWhenComplete(conf.getBoolean(
        TezConfiguration.TEZ_AM_CANCEL_DELEGATION_TOKEN,
        TezConfiguration.DEFAULT_TEZ_AM_CANCEL_DELEGATION_TOKEN));
    appContext.setAMContainerSpec(amContainer);

    return appContext;
  }

  
  public static void main(String[] args) {
    try {
      TezClient tezClient = new TezClient(
          new TezConfiguration(new YarnConfiguration()));
      DAGClient dagClient = tezClient.getDAGClient(args[1]);
      String dagId = dagClient.getAllDAGs().get(0);
      DAGStatus dagStatus = dagClient.getDAGStatus(dagId);
      System.out.println("DAG: " + dagId + 
                         " State: " + dagStatus.getState() +
                         " Progress: " + dagStatus.getDAGProgress());
      for(String vertexName : dagStatus.getVertexProgress().keySet()) {
        System.out.println("VertexStatus from DagStatus:" +
                           " Vertex: " + vertexName +
                           " Progress: " + dagStatus.getVertexProgress().get(vertexName));
        VertexStatus vertexStatus = dagClient.getVertexStatus(dagId, vertexName);
        System.out.println("VertexStatus:" + 
                           " Vertex: " + vertexName + 
                           " State: " + vertexStatus.getState() + 
                           " Progress: " + vertexStatus.getProgress());
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
