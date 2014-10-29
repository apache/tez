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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezYARNUtils;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@Private
public class TezClientUtils {

  private static Log LOG = LogFactory.getLog(TezClientUtils.class);
  private static final int UTF8_CHUNK_SIZE = 16 * 1024;

  private static FileStatus[] getLRFileStatus(String fileName, Configuration conf) throws
      IOException {
    URI uri;
    try {
      uri = new URI(fileName);
    } catch (URISyntaxException e) {
      String message = "Invalid URI defined in configuration for"
          + " location of TEZ jars. providedURI=" + fileName;
      LOG.error(message);
      throw new TezUncheckedException(message, e);
    }

    Path p = new Path(uri);
    FileSystem fs = p.getFileSystem(conf);
    p = fs.resolvePath(p);

    if (fs.isDirectory(p)) {
      return fs.listStatus(p);
    } else {
      FileStatus fStatus = fs.getFileStatus(p);
      return new FileStatus[]{fStatus};
    }
  }

  /**
   * Setup LocalResource map for Tez jars based on provided Configuration
   * 
   * @param conf
   *          Configuration to use to access Tez jars' locations
   * @param credentials
   *          a credentials instance into which tokens for the Tez local
   *          resources will be populated
   * @param tezJarResources Map of LocalResources to use for AM and DAGs
   * @return Whether the archive-based deployment of Tez was used.
   * @throws IOException
   */
  static boolean setupTezJarsLocalResources(TezConfiguration conf,
      Credentials credentials, Map<String, LocalResource> tezJarResources)
      throws IOException {
    Preconditions.checkNotNull(credentials, "A non-null credentials object should be specified");
    boolean usingTezArchive = false;

    if (conf.getBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, false)){
      LOG.info("Ignoring '" + TezConfiguration.TEZ_LIB_URIS + "' since  '" + 
            TezConfiguration.TEZ_IGNORE_LIB_URIS + "' is set to true");
    } else {
      // Add tez jars to local resource
      String[] tezJarUris = conf.getStrings(TezConfiguration.TEZ_LIB_URIS);

      if (tezJarUris == null || tezJarUris.length == 0) {
        throw new TezUncheckedException("Invalid configuration of tez jars"
            + ", " + TezConfiguration.TEZ_LIB_URIS
            + " is not defined in the configuration");
      }

      LOG.info("Using tez.lib.uris value from configuration: "
          + conf.get(TezConfiguration.TEZ_LIB_URIS));

      if (tezJarUris.length == 1 && (
              tezJarUris[0].endsWith(".tar.gz") ||
              tezJarUris[0].endsWith(".tgz") ||
              tezJarUris[0].endsWith(".zip") ||
              tezJarUris[0].endsWith(".tar"))) {
        String fileName = tezJarUris[0];

        FileStatus fStatus = getLRFileStatus(fileName, conf)[0];
        LocalResourceVisibility lrVisibility;
        if (checkAncestorPermissionsForAllUsers(conf, fileName, FsAction.EXECUTE) &&
            fStatus.getPermission().getOtherAction().implies(FsAction.READ)) {
          lrVisibility = LocalResourceVisibility.PUBLIC;
        } else {
          lrVisibility = LocalResourceVisibility.PRIVATE;
        }
        tezJarResources.put(TezConstants.TEZ_TAR_LR_NAME,
            LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(fStatus.getPath()),
                LocalResourceType.ARCHIVE,
                lrVisibility,
                fStatus.getLen(),
                fStatus.getModificationTime()));
        Path[] tezJarPaths = { fStatus.getPath() };
        // obtain credentials
        TokenCache.obtainTokensForFileSystems(credentials, tezJarPaths, conf);
        usingTezArchive = true;
      } else { // Treat as non-archives
        addLocalResources(conf, tezJarUris, tezJarResources, credentials);
      }
      
      if (tezJarResources.isEmpty()) {
        throw new TezUncheckedException(
            "No files found in locations specified in "
                + TezConfiguration.TEZ_LIB_URIS + " . Locations: "
                + StringUtils.join(tezJarUris, ','));
      }
    }

    // Add aux uris to local resources
    addLocalResources(conf, conf.getStrings(TezConfiguration.TEZ_AUX_URIS),
        tezJarResources, credentials);

    return usingTezArchive;
  }

  private static void addLocalResources(Configuration conf, String[] configUris,
      Map<String, LocalResource> tezJarResources, Credentials credentials) throws IOException {
    if (configUris == null || configUris.length == 0) {
      return;
    }
    List<Path> configuredPaths = Lists.newArrayListWithCapacity(configUris.length);
    for (String configUri : configUris) {
      boolean ancestorsHavePermission = checkAncestorPermissionsForAllUsers(conf, configUri,
          FsAction.EXECUTE);
      FileStatus [] fileStatuses = getLRFileStatus(configUri, conf);
      for (FileStatus fStatus : fileStatuses) {
        if (fStatus.isDirectory()) {
          // Skip directories - no recursive search support.
          continue;
        }
        LocalResourceVisibility lrVisibility;
        if (ancestorsHavePermission &&
            fStatus.getPermission().getOtherAction().implies(FsAction.READ)) {
          lrVisibility = LocalResourceVisibility.PUBLIC;
        } else {
          lrVisibility = LocalResourceVisibility.PRIVATE;
        }
        String rsrcName = fStatus.getPath().getName();
        if (tezJarResources.containsKey(rsrcName)) {
          String message = "Duplicate resource found"
              + ", resourceName=" + rsrcName
              + ", existingPath=" +
              tezJarResources.get(rsrcName).getResource().toString()
              + ", newPath=" + fStatus.getPath();
          LOG.warn(message);
        }
        tezJarResources.put(rsrcName,
            LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(fStatus.getPath()),
                LocalResourceType.FILE,
                lrVisibility,
                fStatus.getLen(),
                fStatus.getModificationTime()));
        configuredPaths.add(fStatus.getPath());
      }
    }
    // Obtain credentials.
    if (!configuredPaths.isEmpty()) {
      TokenCache.obtainTokensForFileSystems(credentials,
          configuredPaths.toArray(new Path[configuredPaths.size()]), conf);
    }
  }

  static void processTezLocalCredentialsFile(Credentials credentials, Configuration conf)
      throws IOException {
    String path = conf.get(TezConfiguration.TEZ_CREDENTIALS_PATH);
    if (path == null) {
      return;
    } else {
      TokenCache.mergeBinaryTokens(credentials, conf, path);
    }
  }

  /**
   * Verify or create the Staging area directory on the configured Filesystem
   * @param stagingArea Staging area directory path
   * @return the FileSytem for the staging area directory
   * @throws IOException
   */
  public static FileSystem ensureStagingDirExists(Configuration conf,
      Path stagingArea)
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
      if (!fsStatus.getPermission().equals(TezCommonUtils.TEZ_AM_DIR_PERMISSION)) {
        LOG.info("Permissions on staging directory " + stagingArea + " are "
            + "incorrect: " + fsStatus.getPermission()
            + ". Fixing permissions " + "to correct value "
            + TezCommonUtils.TEZ_AM_DIR_PERMISSION);
        fs.setPermission(stagingArea, TezCommonUtils.TEZ_AM_DIR_PERMISSION);
      }
    } else {
      TezCommonUtils.mkDirForAM(fs, stagingArea);
    }
    return fs;
  }
  
  /**
   * Populate {@link Credentials} for the URI's to access them from their {@link FileSystem}s
   * @param uris URIs that need to be accessed
   * @param credentials Credentials object into which to add the credentials
   * @param conf Configuration to access the FileSystem
   * @throws IOException
   */
  public static void addFileSystemCredentialsFromURIs(Collection<URI> uris, Credentials credentials,
      Configuration conf) throws IOException {
    // Obtain Credentials for any paths that the user may have configured.
    if (uris != null && !uris.isEmpty()) {
      Iterator<Path> pathIter = Iterators.transform(uris.iterator(), new Function<URI, Path>() {
        @Override
        public Path apply(URI input) {
          return new Path(input);
        }
      });

      Path[] paths = Iterators.toArray(pathIter, Path.class);
      TokenCache.obtainTokensForFileSystems(credentials, paths, conf);
    }
  }

  /**
   * Obtains tokens for the DAG based on the list of URIs setup in the DAG. The
   * fetched credentials are populated back into the DAG and can be retrieved
   * via dag.getCredentials
   * 
   * @param dag
   *          the dag for which credentials need to be setup
   * @param sessionCredentials
   *          session credentials which have already been obtained, and will be
   *          required for the DAG
   * @param conf
   * @throws IOException
   */
  @Private
  static Credentials setupDAGCredentials(DAG dag, Credentials sessionCredentials,
      Configuration conf) throws IOException {

    Preconditions.checkNotNull(sessionCredentials);
    TezCommonUtils.logCredentials(LOG, sessionCredentials, "session");

    Credentials dagCredentials = new Credentials();
    // All session creds are required for the DAG.
    dagCredentials.mergeAll(sessionCredentials);
    
    // Add additional credentials based on any URIs that the user may have specified.
    
    // Obtain Credentials for any paths that the user may have configured.
    addFileSystemCredentialsFromURIs(dag.getURIsForCredentials(), dagCredentials, conf);

    // Obtain Credentials for the local resources configured on the DAG
    try {
      Set<Path> lrPaths = new HashSet<Path>();
      for (Vertex v: dag.getVertices()) {
        for (LocalResource lr: v.getTaskLocalFiles().values()) {
          lrPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
        }
        List<DataSourceDescriptor> dataSources = v.getDataSources();
        for (DataSourceDescriptor dataSource : dataSources) {
          addFileSystemCredentialsFromURIs(dataSource.getURIsForCredentials(), dagCredentials, conf);
        }
        List<DataSinkDescriptor> dataSinks = v.getDataSinks();
        for (DataSinkDescriptor dataSink : dataSinks) {
          addFileSystemCredentialsFromURIs(dataSink.getURIsForCredentials(), dagCredentials, conf);
        }
      }
      
      for (LocalResource lr: dag.getTaskLocalFiles().values()) {
        lrPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
      }

      Path[] paths = lrPaths.toArray(new Path[lrPaths.size()]);
      TokenCache.obtainTokensForFileSystems(dagCredentials, paths, conf);

    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    
    return dagCredentials;
  }

  /**
   * Create an ApplicationSubmissionContext to launch a Tez AM
   * @param appId Application Id
   * @param dag DAG to be submitted
   * @param amName Name for the application
   * @param amConfig AM Configuration
   * @param tezJarResources Resources to be used by the AM
   * @param sessionCreds the credential object which will be populated with session specific
   * @return an ApplicationSubmissionContext to launch a Tez AM
   * @throws IOException
   * @throws YarnException
   */
  static ApplicationSubmissionContext createApplicationSubmissionContext(
      ApplicationId appId, DAG dag, String amName,
      AMConfiguration amConfig, Map<String, LocalResource> tezJarResources,
      Credentials sessionCreds, boolean tezLrsAsArchive,
      TezApiVersionInfo apiVersionInfo) throws IOException, YarnException{

    Preconditions.checkNotNull(sessionCreds);
    TezConfiguration conf = amConfig.getTezConfiguration();

    FileSystem fs = TezClientUtils.ensureStagingDirExists(conf,
        TezCommonUtils.getTezBaseStagingPath(conf));
    String strAppId = appId.toString();
    Path tezSysStagingPath = TezCommonUtils.createTezSystemStagingPath(conf, strAppId);
    Path binaryConfPath = TezCommonUtils.getTezConfStagingPath(tezSysStagingPath);
    binaryConfPath = fs.makeQualified(binaryConfPath);

    // Setup resource requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(
        amConfig.getTezConfiguration().getInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB,
            TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT));
    capability.setVirtualCores(
        amConfig.getTezConfiguration().getInt(TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES,
            TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES_DEFAULT));
    if (LOG.isDebugEnabled()) {
      LOG.debug("AppMaster capability = " + capability);
    }

    // Setup required Credentials for the AM launch. DAG specific credentials
    // are handled separately.
    ByteBuffer securityTokens = null;
    // Setup security tokens
    Credentials amLaunchCredentials = new Credentials();
    if (amConfig.getCredentials() != null) {
      amLaunchCredentials.addAll(amConfig.getCredentials());
    }

    // Add Staging dir creds to the list of session credentials.
    TokenCache.obtainTokensForFileSystems(sessionCreds, new Path[] {binaryConfPath}, conf);

    // Add session specific credentials to the AM credentials.
    amLaunchCredentials.mergeAll(sessionCreds);

    DataOutputBuffer dob = new DataOutputBuffer();
    amLaunchCredentials.writeTokenStorageToStream(dob);
    securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    String amOpts = constructAMLaunchOpts(amConfig.getTezConfiguration(), capability);
    vargs.add(amOpts);

    String amLogLevel = amConfig.getTezConfiguration().get(
        TezConfiguration.TEZ_AM_LOG_LEVEL,
        TezConfiguration.TEZ_AM_LOG_LEVEL_DEFAULT);
    maybeAddDefaultLoggingJavaOpts(amLogLevel, vargs);

    // FIX sun bug mentioned in TEZ-327
    vargs.add("-Dsun.nio.ch.bugLevel=''");

    vargs.add(TezConstants.TEZ_APPLICATION_MASTER_CLASS);
    if (dag == null) {
      vargs.add("--" + TezConstants.TEZ_SESSION_MODE_CLI_OPTION);
    }

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

    if (LOG.isDebugEnabled()) {
      LOG.debug("Command to launch container for ApplicationMaster is : "
          + mergedCommand);
    }

    Map<String, String> environment = new TreeMap<String, String>();
    TezYARNUtils.setupDefaultEnv(environment, conf, TezConfiguration.TEZ_AM_LAUNCH_ENV,
        TezConfiguration.TEZ_AM_LAUNCH_ENV_DEFAULT, tezLrsAsArchive);
    
    // finally apply env set in the code. This could potentially be removed in
    // TEZ-692
    if (amConfig.getEnv() != null) {
      for (Map.Entry<String, String> entry : amConfig.getEnv().entrySet()) {
        TezYARNUtils.addToEnvironment(environment, entry.getKey(),
            entry.getValue(), File.pathSeparator);
      }
    }

    addVersionInfoToEnv(environment, apiVersionInfo);
    
    Map<String, LocalResource> amLocalResources =
        new TreeMap<String, LocalResource>();

    // Not fetching credentials for AMLocalResources. Expect this to be provided via AMCredentials.
    if (amConfig.getAMLocalResources() != null) {
      amLocalResources.putAll(amConfig.getAMLocalResources());
    }
    amLocalResources.putAll(tezJarResources);

    // emit conf as PB file
    ConfigurationProto finalConfProto = createFinalConfProtoForApp(amConfig.getTezConfiguration());
    
    FSDataOutputStream amConfPBOutBinaryStream = null;
    try {
      amConfPBOutBinaryStream = TezCommonUtils.createFileForAM(fs, binaryConfPath);
      finalConfProto.writeTo(amConfPBOutBinaryStream);
    } finally {
      if(amConfPBOutBinaryStream != null){
        amConfPBOutBinaryStream.close();
      }
    }

    LocalResource binaryConfLRsrc =
        TezClientUtils.createLocalResource(fs,
            binaryConfPath, LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION);
    amConfig.setBinaryConfLR(binaryConfLRsrc);
    amLocalResources.put(TezConstants.TEZ_PB_BINARY_CONF_NAME,
        binaryConfLRsrc);

    // Create Session Jars definition to be sent to AM as a local resource
    Path sessionJarsPath = TezCommonUtils.getTezAMJarStagingPath(tezSysStagingPath);
    FSDataOutputStream sessionJarsPBOutStream = null;
    try {
      sessionJarsPBOutStream = TezCommonUtils.createFileForAM(fs, sessionJarsPath);
      // Write out the initial list of resources which will be available in the AM
      DAGProtos.PlanLocalResourcesProto amResourceProto;
      if (amLocalResources != null && !amLocalResources.isEmpty()) {
        amResourceProto = DagTypeConverters.convertFromLocalResources(amLocalResources);
      } else {
        amResourceProto = DAGProtos.PlanLocalResourcesProto.getDefaultInstance(); 
      }
      amResourceProto.writeDelimitedTo(sessionJarsPBOutStream);
    } finally {
      if (sessionJarsPBOutStream != null) {
        sessionJarsPBOutStream.close();
      }
    }

    LocalResource sessionJarsPBLRsrc =
      TezClientUtils.createLocalResource(fs,
        sessionJarsPath, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION);
    amLocalResources.put(
      TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME,
      sessionJarsPBLRsrc);

    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    ACLManager aclManager = new ACLManager(user, amConfig.getTezConfiguration());
    Map<ApplicationAccessType, String> acls = aclManager.toYARNACls();

    if(dag != null) {
      
      DAGPlan dagPB = prepareAndCreateDAGPlan(dag, amConfig, tezJarResources, tezLrsAsArchive,
          sessionCreds);

      // emit protobuf DAG file style
      Path binaryPath = TezCommonUtils.getTezBinPlanStagingPath(tezSysStagingPath);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stage directory information for AppId :" + appId + " tezSysStagingPath :"
            + tezSysStagingPath + " binaryConfPath :" + binaryConfPath + " sessionJarsPath :"
            + sessionJarsPath + " binaryPlanPath :" + binaryPath);
      }

      FSDataOutputStream dagPBOutBinaryStream = null;

      try {
        //binary output
        dagPBOutBinaryStream = TezCommonUtils.createFileForAM(fs, binaryPath);
        dagPB.writeTo(dagPBOutBinaryStream);
      } finally {
        if(dagPBOutBinaryStream != null){
          dagPBOutBinaryStream.close();
        }
      }

      amLocalResources.put(TezConstants.TEZ_PB_PLAN_BINARY_NAME,
        TezClientUtils.createLocalResource(fs,
          binaryPath, LocalResourceType.FILE,
          LocalResourceVisibility.APPLICATION));

      if (Level.DEBUG.isGreaterOrEqual(Level.toLevel(amLogLevel))) {
        Path textPath = localizeDagPlanAsText(dagPB, fs, amConfig, strAppId, tezSysStagingPath);
        amLocalResources.put(TezConstants.TEZ_PB_PLAN_TEXT_NAME,
            TezClientUtils.createLocalResource(fs,
                textPath, LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION));
      }
    }

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(amLocalResources, environment,
            vargsFinal, null, securityTokens, acls);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);

    appContext.setApplicationType(TezConstants.TEZ_APPLICATION_TYPE);
    appContext.setApplicationId(appId);
    appContext.setResource(capability);
    if (amConfig.getQueueName() != null) {
      appContext.setQueue(amConfig.getQueueName());
    }
    appContext.setApplicationName(amName);
    appContext.setCancelTokensWhenComplete(amConfig.getTezConfiguration().getBoolean(
        TezConfiguration.TEZ_CANCEL_DELEGATION_TOKENS_ON_COMPLETION,
        TezConfiguration.TEZ_CANCEL_DELEGATION_TOKENS_ON_COMPLETION_DEFAULT));
    appContext.setAMContainerSpec(amContainer);

    appContext.setMaxAppAttempts(
      amConfig.getTezConfiguration().getInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS,
        TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS_DEFAULT));

    return appContext;

  }
  
  static DAGPlan prepareAndCreateDAGPlan(DAG dag, AMConfiguration amConfig,
      Map<String, LocalResource> tezJarResources, boolean tezLrsAsArchive,
      Credentials credentials) throws IOException {
    Credentials dagCredentials = setupDAGCredentials(dag, credentials, amConfig.getTezConfiguration());
    return dag.createDag(amConfig.getTezConfiguration(), dagCredentials, tezJarResources,
        amConfig.getBinaryConfLR(), tezLrsAsArchive);
  }
  
  static void maybeAddDefaultLoggingJavaOpts(String logLevel, List<String> vargs) {
    if (vargs != null && !vargs.isEmpty()) {
      for (String arg : vargs) {
        if (arg.contains(TezConstants.TEZ_ROOT_LOGGER_NAME)) {
          return ;
        }
      }
    }
    TezClientUtils.addLog4jSystemProperties(logLevel, vargs);
  }
  
  static String maybeAddDefaultLoggingJavaOpts(String logLevel, String javaOpts) {
    List<String> vargs = new ArrayList<String>(5);
    if (javaOpts != null) {
      vargs.add(javaOpts);
    } else {
      vargs.add("");
    }
    maybeAddDefaultLoggingJavaOpts(logLevel, vargs);
    if (vargs.size() == 1) {
      return vargs.get(0);
    }
    return StringUtils.join(vargs, " ").trim();
  }
  
  @Private
  public static String addDefaultsToTaskLaunchCmdOpts(String vOpts, Configuration conf) {
    String vConfigOpts = "";
    String taskDefaultOpts = conf.get(TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS,
        TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT);
    if (taskDefaultOpts != null && !taskDefaultOpts.isEmpty()) {
      vConfigOpts = taskDefaultOpts + " ";
    }
    vConfigOpts = vConfigOpts + conf.get(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS,
        TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS_DEFAULT);
    if (vConfigOpts != null && !vConfigOpts.isEmpty()) {
      // Add options specified in the DAG at the end.
      vOpts = vConfigOpts + " " + vOpts;
    }
    
    vOpts = maybeAddDefaultLoggingJavaOpts(conf.get(
        TezConfiguration.TEZ_TASK_LOG_LEVEL,
        TezConfiguration.TEZ_TASK_LOG_LEVEL_DEFAULT), vOpts);
    return vOpts;
  }

  @Private
  @VisibleForTesting
  public static void addLog4jSystemProperties(String logLevel,
      List<String> vargs) {
    vargs.add("-Dlog4j.configuration="
        + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + logLevel
        + "," + TezConstants.TEZ_CONTAINER_LOGGER_NAME);
  }

  static ConfigurationProto createFinalConfProtoForApp(Configuration amConf) {
    assert amConf != null;
    ConfigurationProto.Builder builder = ConfigurationProto.newBuilder();
    for (Entry<String, String> entry : amConf) {
      PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
      kvp.setKey(entry.getKey());
      kvp.setValue(entry.getValue());
      builder.addConfKeyValues(kvp);
    }
    return builder.build();
  }

  /**
   * Helper function to create a YARN LocalResource
   * @param fs FileSystem object
   * @param p Path of resource to localize
   * @param type LocalResource Type
   * @return a YARN LocalResource for the given Path
   * @throws IOException
   */
  static LocalResource createLocalResource(FileSystem fs, Path p,
      LocalResourceType type,
      LocalResourceVisibility visibility) throws IOException {
    LocalResource rsrc = Records.newRecord(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs.resolvePath(rsrcStat
        .getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(visibility);
    return rsrc;
  }

  private static Path localizeDagPlanAsText(DAGPlan dagPB, FileSystem fs, AMConfiguration amConfig,
      String strAppId, Path tezSysStagingPath) throws IOException {
    Path textPath =
        TezCommonUtils.getTezTextPlanStagingPath(tezSysStagingPath, strAppId, dagPB.getName());
    FSDataOutputStream dagPBOutTextStream = null;
    try {
      dagPBOutTextStream = TezCommonUtils.createFileForAM(fs, textPath);
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

  static DAGClientAMProtocolBlockingPB getSessionAMProxy(FrameworkClient yarnClient,
      Configuration conf,
      ApplicationId applicationId) throws TezException, IOException {
    ApplicationReport appReport;
    try {
      appReport = yarnClient.getApplicationReport(
          applicationId);

      if(appReport == null) {
        throw new TezUncheckedException("Could not retrieve application report"
            + " from YARN, applicationId=" + applicationId);
      }
      YarnApplicationState appState = appReport.getYarnApplicationState();
      if(appState != YarnApplicationState.RUNNING) {
        if (appState == YarnApplicationState.FINISHED
            || appState == YarnApplicationState.KILLED
            || appState == YarnApplicationState.FAILED) {
          throw new SessionNotRunning("Application not running"
              + ", applicationId=" + applicationId
              + ", yarnApplicationState=" + appReport.getYarnApplicationState()
              + ", finalApplicationStatus="
              + appReport.getFinalApplicationStatus()
              + ", trackingUrl=" + appReport.getTrackingUrl());
        }
        return null;
      }
    } catch (YarnException e) {
      throw new TezException(e);
    }
    return getAMProxy(conf, appReport.getHost(),
        appReport.getRpcPort(), appReport.getClientToAMToken());
  }

  @Private
  public static DAGClientAMProtocolBlockingPB getAMProxy(final Configuration conf, String amHost,
      int amRpcPort, org.apache.hadoop.yarn.api.records.Token clientToAMToken) throws IOException {

    final InetSocketAddress serviceAddr = NetUtils.createSocketAddrForHost(amHost, amRpcPort);
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(UserGroupInformation
        .getCurrentUser().getUserName());
    if (clientToAMToken != null) {
      Token<ClientToAMTokenIdentifier> token = ConverterUtils.convertFromYarn(clientToAMToken,
          serviceAddr);
      userUgi.addToken(token);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to Tez AM at " + serviceAddr);
    }
    DAGClientAMProtocolBlockingPB proxy = null;
    try {
      proxy = userUgi.doAs(new PrivilegedExceptionAction<DAGClientAMProtocolBlockingPB>() {
        @Override
        public DAGClientAMProtocolBlockingPB run() throws IOException {
          RPC.setProtocolEngine(conf, DAGClientAMProtocolBlockingPB.class, ProtobufRpcEngine.class);
          return (DAGClientAMProtocolBlockingPB) RPC.getProxy(DAGClientAMProtocolBlockingPB.class,
              0, serviceAddr, conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException("Failed to connect to AM", e);
    }
    return proxy;
  }

  static void createSessionToken(String tokenIdentifier,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials credentials) {
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(
        tokenIdentifier));
    Token<JobTokenIdentifier> sessionToken = new Token<JobTokenIdentifier>(identifier,
        jobTokenSecretManager);
    sessionToken.setService(identifier.getJobId());
    TokenCache.setSessionToken(sessionToken, credentials);
  }

  @Private
  /**
   * Add computed Xmx value to java opts if both -Xms and -Xmx are not specified
   * @param javaOpts Current java opts
   * @param resource Resource capability based on which java opts will be computed
   * @param maxHeapFactor Factor to size Xmx ( valid range is 0.0 < x < 1.0)
   * @return Modified java opts with computed Xmx value
   */
  public static String maybeAddDefaultMemoryJavaOpts(String javaOpts, Resource resource,
      double maxHeapFactor) {
    if ((javaOpts != null && !javaOpts.isEmpty()
          && (javaOpts.contains("-Xmx") || javaOpts.contains("-Xms")))
        || (resource.getMemory() <= 0)) {
      return javaOpts;
    }
    if (maxHeapFactor <= 0 || maxHeapFactor >= 1) {
      return javaOpts;
    }
    int maxMemory = (int)(resource.getMemory() * maxHeapFactor);
    maxMemory = maxMemory <= 0 ? 1 : maxMemory;

    return " -Xmx" + maxMemory + "m "
        + ( javaOpts != null ? javaOpts : "");
  }

  private static boolean checkAncestorPermissionsForAllUsers(Configuration conf, String uri,
                                                             FsAction permission) throws IOException {
    Path pathComponent = new Path(uri);
    FileSystem fs = pathComponent.getFileSystem(conf);

    if (Shell.WINDOWS && fs instanceof LocalFileSystem) {
      // Relax the requirement for public cache on LFS on Windows since default permissions are
      // "700" all the way up to the drive letter. In this model, the only requirement for a user
      // is to give EVERYONE group permission on the file and the file will be considered public.
      // This code path is only hit when fs.default.name is file:/// (mainly in tests).
      return true;
    }

    if (fs.getFileStatus(pathComponent).isFile()) {
      pathComponent = pathComponent.getParent();
    }

    while (pathComponent != null) {
      if (!fs.getFileStatus(pathComponent).getPermission().getOtherAction().implies(permission)) {
        return false;
      }
      pathComponent = pathComponent.getParent();
    }

    return true;
  }

  @Private
  @VisibleForTesting
  static String constructAMLaunchOpts(TezConfiguration tezConf, Resource capability) {
    String defaultOpts = tezConf.get(TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS,
        TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT);
    String amOpts = "";
    if (defaultOpts != null && !defaultOpts.isEmpty()) {
      amOpts = defaultOpts + " ";
    }
    amOpts = amOpts + tezConf.get(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
        TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT);

    amOpts = maybeAddDefaultMemoryJavaOpts(amOpts, capability,
        tezConf.getDouble(TezConfiguration.TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION,
            TezConfiguration.TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION_DEFAULT));
    return amOpts;
  }

  private static void addVersionInfoToEnv(Map<String, String> environment,
      TezApiVersionInfo versionInfo) {
    if (!versionInfo.getVersion().equals(VersionInfo.UNKNOWN)) {
      TezYARNUtils.replaceInEnv(environment, TezConstants.TEZ_CLIENT_VERSION_ENV,
          versionInfo.getVersion());
    }
  }

}
