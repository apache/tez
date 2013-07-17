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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;

public class TezClient {
  private static final Log LOG = LogFactory.getLog(TezClient.class);

  final public static FsPermission TEZ_AM_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------
  final public static FsPermission TEZ_AM_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  public static final int UTF8_CHUNK_SIZE = 16 * 1024;

  private final TezConfiguration conf;
  private YarnClient yarnClient;

  /**
   * <p>
   * Create an instance of the TezClient which will be used to communicate with
   * a specific instance of YARN, or TezService when that exists.
   * </p>
   * <p>
   * Separate instances of TezClient should be created to communicate with
   * different instances of YARN
   * </p>
   *
   * @param conf
   *          the configuration which will be used to establish which YARN or
   *          Tez service instance this client is associated with.
   */
  public TezClient(TezConfiguration conf) {
    this.conf = conf;
    yarnClient = new YarnClientImpl();
    yarnClient.init(new YarnConfiguration(conf));
    yarnClient.start();
  }

  /**
   * Submit a Tez DAG to YARN as an application. The job will be submitted to
   * the yarn cluster or tez service which was specified when creating this
   * {@link TezClient} instance.
   *
   * @param dag
   *          <code>DAG</code> to be submitted
   * @param appStagingDir
   *          FileSystem path in which resources will be copied
   * @param ts
   *          Application credentials
   * @param amQueueName
   *          Queue to which the application will be submitted
   * @param amArgs
   *          Command line Java arguments for the ApplicationMaster
   * @param amEnv
   *          Environment to be added to the ApplicationMaster
   * @param amLocalResources
   *          YARN local resource for the ApplicationMaster
   * @param conf
   *          Configuration for the Tez DAG AM. tez configuration keys from this
   *          config will be used when running the AM. Look at
   *          {@link TezConfiguration} for keys. This can be null if no DAG AM
   *          parameters need to be changed.
   * @return <code>ApplicationId</code> of the submitted Tez application
   * @throws IOException
   * @throws TezException
   */
  public DAGClient submitDAGApplication(DAG dag, Path appStagingDir,
      Credentials ts, String amQueueName, List<String> amArgs,
      Map<String, String> amEnv, Map<String, LocalResource> amLocalResources,
      TezConfiguration amConf) throws IOException, TezException {
    ApplicationId appId = createApplication();
    return submitDAGApplication(appId, dag, appStagingDir, ts, amQueueName,
        amArgs, amEnv, amLocalResources, amConf);
  }

  /**
   * Submit a Tez DAG to YARN with known <code>ApplicationId</code>. This is a
   * private method and is only meant to be used within Tez for MR client
   * backward compatibility.
   *
   * @param appId
   *          - <code>ApplicationId</code> to be used
   * @param dag
   *          <code>DAG</code> to be submitted
   * @param appStagingDir
   *          FileSystem path in which resources will be copied
   * @param ts
   *          Application credentials
   * @param amQueueName
   *          Queue to which the application will be submitted
   * @param amArgs
   *          Command line Java arguments for the ApplicationMaster
   * @param amEnv
   *          Environment to be added to the ApplicationMaster
   * @param amLocalResources
   *          YARN local resource for the ApplicationMaster
   * @param conf
   *          Configuration for the Tez DAG AM. tez configuration keys from this
   *          config will be used when running the AM. Look at
   *          {@link TezConfiguration} for keys. This can be null if no DAG AM
   *          parameters need to be changed.
   * @return <code>ApplicationId</code> of the submitted Tez application
   * @throws IOException
   * @throws TezException
   */
  @Private
  public DAGClient submitDAGApplication(ApplicationId appId, DAG dag,
      Path appStagingDir, Credentials ts, String amQueueName,
      List<String> amArgs, Map<String, String> amEnv,
      Map<String, LocalResource> amLocalResources, TezConfiguration amConf)
      throws IOException, TezException {
    try {
      ApplicationSubmissionContext appContext = createApplicationSubmissionContext(
          appId, dag, appStagingDir, ts, amQueueName, dag.getName(), amArgs,
          amEnv, amLocalResources, amConf);
      yarnClient.submitApplication(appContext);
    } catch (YarnException e) {
      throw new TezException(e);
    }

    return getDAGClient(appId);
  }

  /**
   * Create a new YARN application
   * @return <code>ApplicationId</code> for the new YARN application
   * @throws YarnException
   * @throws IOException
   */
  public ApplicationId createApplication() throws TezException, IOException {
    try {
      return yarnClient.createApplication().
          getNewApplicationResponse().getApplicationId();
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

  @Private
  public DAGClient getDAGClient(ApplicationId appId)
      throws IOException, TezException {
      return new DAGClientRPCImpl(appId, getDefaultTezDAGID(appId), conf);
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

  private Map<String, LocalResource> setupTezJarsLocalResources()
      throws IOException {
    Map<String, LocalResource> tezJarResources =
        new TreeMap<String, LocalResource>();
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      return tezJarResources;
    }

    // Add tez jars to local resource
    String[] tezJarUris = conf.getStrings(
        TezConfiguration.TEZ_LIB_URIS);
    if (tezJarUris == null
        || tezJarUris.length == 0) {
      throw new TezUncheckedException("Invalid configuration of tez jars"
          + ", " + TezConfiguration.TEZ_LIB_URIS
          + " is not defined in the configurartion");
    }

    for (String tezJarUri : tezJarUris) {
      URI uri;
      try {
        uri = new URI(tezJarUri.trim());
      } catch (URISyntaxException e) {
        String message = "Invalid URI defined in configuration for"
            + " location of TEZ jars. providedURI=" + tezJarUri;
        LOG.error(message);
        throw new TezUncheckedException(message, e);
      }
      if (!uri.isAbsolute()) {
        String message = "Non-absolute URI defined in configuration for"
            + " location of TEZ jars. providedURI=" + tezJarUri;
        LOG.error(message);
        throw new TezUncheckedException(message);
      }
      Path p = new Path(uri);
      FileSystem pathfs = p.getFileSystem(conf);
      RemoteIterator<LocatedFileStatus> iter = pathfs.listFiles(p, false);
      while (iter.hasNext()) {
        LocatedFileStatus fStatus = iter.next();
        String rsrcName = fStatus.getPath().getName();
        // FIXME currently not checking for duplicates due to quirks
        // in assembly generation
        if (tezJarResources.containsKey(rsrcName)) {
          String message = "Duplicate resource found"
              + ", resourceName=" + rsrcName
              + ", existingPath=" +
              tezJarResources.get(rsrcName).getResource().toString()
              + ", newPath=" + fStatus.getPath();
          LOG.warn(message);
          // throw new TezUncheckedException(message);
        }
        tezJarResources.put(rsrcName,
            LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(fStatus.getPath()),
                LocalResourceType.FILE,
                LocalResourceVisibility.PUBLIC,
                fStatus.getLen(),
                fStatus.getModificationTime()));
      }
    }
    if (tezJarResources.isEmpty()) {
      LOG.warn("No tez jars found in configured locations"
          + ". Ignoring for now. Errors may occur");
    }
    return tezJarResources;
  }

  private Configuration createFinalAMConf(TezConfiguration amConf) {
    if (amConf == null) {
      return new TezConfiguration();
    } else {

      Configuration conf = new Configuration(false);
      conf.setQuietMode(true);

      Iterator<Entry<String, String>> tezConfIter = this.conf.iterator();
      while (tezConfIter.hasNext()) {
        Entry<String, String> entry = tezConfIter.next();
        conf.set(entry.getKey(), entry.getValue());
      }

      Iterator<Entry<String, String>> iter = amConf.iterator();
      while (iter.hasNext()) {
        Entry<String, String> entry = iter.next();
        // Copy all tez config parameters.
        if (entry.getKey().startsWith(TezConfiguration.TEZ_PREFIX)) {
          conf.set(entry.getKey(), entry.getValue());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding tez dag am parameter: " + entry.getKey()
                + ", with value: " + entry.getValue());
          }
        }
      }
      return conf;
    }
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
      ApplicationId appId, DAG dag, Path appStagingDir, Credentials ts,
      String amQueueName, String amName, List<String> amArgs,
      Map<String, String> amEnv, Map<String, LocalResource> amLocalResources,
      TezConfiguration amConf) throws IOException, YarnException {

    if (amConf == null) {
      amConf = new TezConfiguration();
    }

    FileSystem fs = ensureExists(appStagingDir);

    // Setup resource requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(
        conf.getInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB,
            TezConfiguration.DEFAULT_TEZ_AM_RESOURCE_MEMORY_MB));
    capability.setVirtualCores(
        conf.getInt(TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES,
            TezConfiguration.DEFAULT_TEZ_AM_RESOURCE_CPU_VCORES));
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

    if (amArgs != null) {
      vargs.addAll(amArgs);
    }

    vargs.add(TezConfiguration.TEZ_APPLICATION_MASTER_CLASS);
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        File.separator + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        File.separator + ApplicationConstants.STDERR);


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

    boolean isMiniCluster =
        conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false);
    if (isMiniCluster) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          System.getProperty("java.class.path"));
    }

    Apps.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$());

    Apps.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*");

    // Add YARN/COMMON/HDFS jars to path
    if (!isMiniCluster) {
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
            c.trim());
      }
    }

    if (amEnv != null) {
      for (Map.Entry<String, String> entry : amEnv.entrySet()) {
        Apps.addToEnvironment(environment, entry.getKey(), entry.getValue());
      }
    }

    Map<String, LocalResource> localResources =
        new TreeMap<String, LocalResource>();

    if (amLocalResources != null) {
      localResources.putAll(amLocalResources);
    }

    Map<String, LocalResource> tezJarResources =
        setupTezJarsLocalResources();
    localResources.putAll(tezJarResources);

    // Add tez jars to vertices too
    for (Vertex v : dag.getVertices()) {
      v.getTaskLocalResources().putAll(tezJarResources);
    }

    // emit protobuf DAG file style
    Path binaryPath =  new Path(appStagingDir,
        TezConfiguration.TEZ_AM_PLAN_PB_BINARY + "." + appId.toString());
    amConf.set(TezConfiguration.TEZ_AM_PLAN_REMOTE_PATH, binaryPath.toUri()
        .toString());

    Configuration finalAMConf = createFinalAMConf(amConf);

    DAGPlan dagPB = dag.createDag(finalAMConf);

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

    localResources.put(TezConfiguration.TEZ_AM_PLAN_PB_BINARY,
        createApplicationResource(fs,
            binaryPath, LocalResourceType.FILE));

    if (Level.DEBUG.isGreaterOrEqual(Level.toLevel(amLogLevel))) {
      Path textPath = localizeDagPlanAsText(dagPB, fs, appStagingDir, appId);
      localResources.put(TezConfiguration.TEZ_AM_PLAN_PB_TEXT,
          createApplicationResource(fs, textPath, LocalResourceType.FILE));
    }

    Map<ApplicationAccessType, String> acls
        = new HashMap<ApplicationAccessType, String>();

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(localResources, environment,
            vargsFinal, null, securityTokens, acls);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);

    appContext.setApplicationType(TezConfiguration.TEZ_APPLICATION_TYPE);
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

  private Path localizeDagPlanAsText(DAGPlan dagPB, FileSystem fs,
      Path appStagingDir, ApplicationId appId) throws IOException {
    Path textPath = new Path(appStagingDir,
        TezConfiguration.TEZ_AM_PLAN_PB_TEXT + "." + appId.toString());
    FSDataOutputStream dagPBOutTextStream = null;
    try {
      dagPBOutTextStream = FileSystem.create(fs, textPath, new FsPermission(
          TEZ_AM_FILE_PERMISSION));
      String dagPBStr = dagPB.toString();
      int dagPBStrLen = dagPBStr.length();
      if (dagPBStrLen <= UTF8_CHUNK_SIZE) {
        dagPBOutTextStream.writeUTF(dagPBStr);
      } else {
        int startIndex = 0;
        while (startIndex < dagPBStrLen) {
          int endIndex = startIndex + UTF8_CHUNK_SIZE;
          if (endIndex > dagPBStrLen) {
            endIndex = dagPBStrLen;
          }
          dagPBOutTextStream.writeUTF(dagPBStr.substring(startIndex, endIndex));
          startIndex += UTF8_CHUNK_SIZE;
        }
      }
    } finally {
      if (dagPBOutTextStream != null) {
        dagPBOutTextStream.close();
      }
    }
    return textPath;
  }

  // DO NOT CHANGE THIS. This code is replicated from TezDAGID.java
  private static final char SEPARATOR = '_';
  private static final String DAG = "dag";
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }

  String getDefaultTezDAGID(ApplicationId appId) {
     return (new StringBuilder(DAG)).append(SEPARATOR).
                   append(appId.getClusterTimestamp()).
                   append(SEPARATOR).
                   append(appId.getId()).
                   append(SEPARATOR).
                   append(idFormat.format(1)).toString();
  }
}
