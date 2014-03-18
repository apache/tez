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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.tez.common.TezYARNUtils;
import org.apache.tez.common.impl.LogUtils;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DagTypeConverters;
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

public class TezClientUtils {

  private static Log LOG = LogFactory.getLog(TezClientUtils.class);

  public static final FsPermission TEZ_AM_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------
  public static final FsPermission TEZ_AM_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  private static final int UTF8_CHUNK_SIZE = 16 * 1024;

  /**
   * Setup LocalResource map for Tez jars based on provided Configuration
   * 
   * @param conf
   *          Configuration to use to access Tez jars' locations
   * @param credentials
   *          a credentials instance into which tokens for the Tez local
   *          resources will be populated
   * @return Map of LocalResources to use when launching Tez AM
   * @throws IOException
   */
  static Map<String, LocalResource> setupTezJarsLocalResources(
      TezConfiguration conf, Credentials credentials)
      throws IOException {
    Preconditions.checkNotNull(credentials, "A non-null credentials object should be specified");
    Map<String, LocalResource> tezJarResources =
        new TreeMap<String, LocalResource>();

    // Add tez jars to local resource
    String[] tezJarUris = conf.getStrings(
        TezConfiguration.TEZ_LIB_URIS);
    if (tezJarUris == null
        || tezJarUris.length == 0) {
      throw new TezUncheckedException("Invalid configuration of tez jars"
          + ", " + TezConfiguration.TEZ_LIB_URIS
          + " is not defined in the configurartion");
    }
    
    List<Path> tezJarPaths = Lists.newArrayListWithCapacity(tezJarUris.length);

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
      p = pathfs.makeQualified(p);
      tezJarPaths.add(p);
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
    } else {
      // Obtain credentials.
      TokenCache.obtainTokensForFileSystems(credentials,
          tezJarPaths.toArray(new Path[tezJarPaths.size()]), conf);
    }
    return tezJarResources;
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
  static void setupDAGCredentials(DAG dag, Credentials sessionCredentials,
      Configuration conf) throws IOException {

    Preconditions.checkNotNull(sessionCredentials);
    LogUtils.logCredentials(LOG, sessionCredentials, "session");

    Credentials dagCredentials = dag.getCredentials();
    if (dagCredentials == null) {
      dagCredentials = new Credentials();
      dag.setCredentials(dagCredentials);
    }
    // All session creds are required for the DAG.
    dagCredentials.mergeAll(sessionCredentials);
    
    // Add additional credentials based on any URIs that the user may have specified.
    
    // Obtain Credentials for any paths that the user may have configured.
    Collection<URI> uris = dag.getURIsForCredentials();
    if (uris != null && !uris.isEmpty()) {
      Iterator<Path> pathIter = Iterators.transform(uris.iterator(), new Function<URI, Path>() {
        @Override
        public Path apply(URI input) {
          return new Path(input);
        }
      });

      Path[] paths = Iterators.toArray(pathIter, Path.class);
      TokenCache.obtainTokensForFileSystems(dagCredentials, paths, conf);
    }

    // Obtain Credentials for the local resources configured on the DAG
    try {
      Set<Path> lrPaths = new HashSet<Path>();
      for (Vertex v: dag.getVertices()) {
        for (LocalResource lr: v.getTaskLocalResources().values()) {
          lrPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
        }
      }

      Path[] paths = lrPaths.toArray(new Path[lrPaths.size()]);
      TokenCache.obtainTokensForFileSystems(dagCredentials, paths, conf);

    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Create an ApplicationSubmissionContext to launch a Tez AM
   * @param conf TezConfiguration
   * @param appId Application Id
   * @param dag DAG to be submitted
   * @param amName Name for the application
   * @param amConfig AM Configuration
   * @param tezJarResources Resources to be used by the AM
   * @param sessionCredentials the credential object which will be populated with session specific
   * @return an ApplicationSubmissionContext to launch a Tez AM
   * @throws IOException
   * @throws YarnException
   */
  static ApplicationSubmissionContext createApplicationSubmissionContext(
      TezConfiguration conf, ApplicationId appId, DAG dag, String amName,
      AMConfiguration amConfig, Map<String, LocalResource> tezJarResources,
      Credentials sessionCreds)
          throws IOException, YarnException{

    Preconditions.checkNotNull(sessionCreds);
    FileSystem fs = TezClientUtils.ensureStagingDirExists(conf,
        amConfig.getStagingDir());
    Path binaryConfPath =  new Path(amConfig.getStagingDir(),
        TezConfiguration.TEZ_PB_BINARY_CONF_NAME + "." + appId.toString());
    binaryConfPath = fs.makeQualified(binaryConfPath);

    // Setup resource requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(
        amConfig.getAMConf().getInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB,
            TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT));
    capability.setVirtualCores(
        amConfig.getAMConf().getInt(TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES,
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

    // Need to set credentials based on DAG and the URIs which have been set for the DAG.

    if (dag != null) {
      setupDAGCredentials(dag, sessionCreds, conf);
    }

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    String amLogLevel = amConfig.getAMConf().get(
        TezConfiguration.TEZ_AM_LOG_LEVEL,
        TezConfiguration.TEZ_AM_LOG_LEVEL_DEFAULT);
    addLog4jSystemProperties(amLogLevel, vargs);

    vargs.add(amConfig.getAMConf().get(TezConfiguration.TEZ_AM_JAVA_OPTS,
        TezConfiguration.TEZ_AM_JAVA_OPTS_DEFAULT));
    // FIX sun bug mentioned in TEZ-327
    vargs.add("-Dsun.nio.ch.bugLevel=''");

    vargs.add(TezConfiguration.TEZ_APPLICATION_MASTER_CLASS);
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

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    String classpath = getFrameworkClasspath(conf);

    Map<String, String> environment = new TreeMap<String, String>();
    environment.put(Environment.CLASSPATH.name(), classpath);

    if (amConfig.getEnv() != null) {
      for (Map.Entry<String, String> entry : amConfig.getEnv().entrySet()) {
        TezYARNUtils.addToEnvironment(environment, entry.getKey(), entry.getValue(),
            File.pathSeparator);
      }
    }

    Map<String, LocalResource> localResources =
        new TreeMap<String, LocalResource>();

    // Not fetching credentials for AMLocalResources. Expect this to be provided via AMCredentials.
    if (amConfig.getLocalResources() != null) {
      localResources.putAll(amConfig.getLocalResources());
    }
    localResources.putAll(tezJarResources);

    // emit conf as PB file
    Configuration finalTezConf = createFinalTezConfForApp(conf,
      amConfig.getAMConf());
    
    FSDataOutputStream amConfPBOutBinaryStream = null;
    try {
      ConfigurationProto.Builder confProtoBuilder =
          ConfigurationProto.newBuilder();
      Iterator<Entry<String, String>> iter = finalTezConf.iterator();
      while (iter.hasNext()) {
        Entry<String, String> entry = iter.next();
        PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
        kvp.setKey(entry.getKey());
        kvp.setValue(entry.getValue());
        confProtoBuilder.addConfKeyValues(kvp);
      }
      //binary output
      amConfPBOutBinaryStream = FileSystem.create(fs, binaryConfPath,
          new FsPermission(TEZ_AM_FILE_PERMISSION));
      confProtoBuilder.build().writeTo(amConfPBOutBinaryStream);
    } finally {
      if(amConfPBOutBinaryStream != null){
        amConfPBOutBinaryStream.close();
      }
    }

    LocalResource binaryConfLRsrc =
        TezClientUtils.createLocalResource(fs,
            binaryConfPath, LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION);
    localResources.put(TezConfiguration.TEZ_PB_BINARY_CONF_NAME,
        binaryConfLRsrc);

    // Create Session Jars definition to be sent to AM as a local resource
    Path sessionJarsPath = new Path(amConfig.getStagingDir(),
      TezConfiguration.TEZ_SESSION_LOCAL_RESOURCES_PB_FILE_NAME + "."
      + appId.toString());
    FSDataOutputStream sessionJarsPBOutStream = null;
    try {
      Map<String, LocalResource> sessionJars =
        new HashMap<String, LocalResource>(tezJarResources.size() + 1);
      sessionJars.putAll(tezJarResources);
      sessionJars.put(TezConfiguration.TEZ_PB_BINARY_CONF_NAME,
        binaryConfLRsrc);
      DAGProtos.PlanLocalResourcesProto proto =
        DagTypeConverters.convertFromLocalResources(sessionJars);
      sessionJarsPBOutStream = FileSystem.create(fs, sessionJarsPath,
        new FsPermission(TEZ_AM_FILE_PERMISSION));
      proto.writeTo(sessionJarsPBOutStream);
    } finally {
      if (sessionJarsPBOutStream != null) {
        sessionJarsPBOutStream.close();
      }
    }

    LocalResource sessionJarsPBLRsrc =
      TezClientUtils.createLocalResource(fs,
        sessionJarsPath, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION);
    localResources.put(
      TezConfiguration.TEZ_SESSION_LOCAL_RESOURCES_PB_FILE_NAME,
      sessionJarsPBLRsrc);

    if(dag != null) {

      for (Vertex v : dag.getVertices()) {
        if (tezJarResources != null) {
          v.getTaskLocalResources().putAll(tezJarResources);
        }
        v.getTaskLocalResources().put(TezConfiguration.TEZ_PB_BINARY_CONF_NAME,
            binaryConfLRsrc);

        Map<String, String> taskEnv = v.getTaskEnvironment();
        for (Map.Entry<String, String> entry : environment.entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue();
          if (!taskEnv.containsKey(key)) {
            taskEnv.put(key, value);
          }
        }
      }

      // emit protobuf DAG file style
      Path binaryPath =  new Path(amConfig.getStagingDir(),
          TezConfiguration.TEZ_PB_PLAN_BINARY_NAME + "." + appId.toString());
      amConfig.getAMConf().set(TezConfiguration.TEZ_AM_PLAN_REMOTE_PATH,
          binaryPath.toUri().toString());

      DAGPlan dagPB = dag.createDag(null);

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

      localResources.put(TezConfiguration.TEZ_PB_PLAN_BINARY_NAME,
        TezClientUtils.createLocalResource(fs,
          binaryPath, LocalResourceType.FILE,
          LocalResourceVisibility.APPLICATION));

      if (Level.DEBUG.isGreaterOrEqual(Level.toLevel(amLogLevel))) {
        Path textPath = localizeDagPlanAsText(dagPB, fs,
            amConfig.getStagingDir(), appId);
        localResources.put(TezConfiguration.TEZ_PB_PLAN_TEXT_NAME,
            TezClientUtils.createLocalResource(fs,
                textPath, LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION));
      }
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
    if (amConfig.getQueueName() != null) {
      appContext.setQueue(amConfig.getQueueName());
    }
    appContext.setApplicationName(amName);
    appContext.setCancelTokensWhenComplete(amConfig.getAMConf().getBoolean(
        TezConfiguration.TEZ_AM_CANCEL_DELEGATION_TOKEN,
        TezConfiguration.TEZ_AM_CANCEL_DELEGATION_TOKEN_DEFAULT));
    appContext.setAMContainerSpec(amContainer);
    
    appContext.setMaxAppAttempts(
      finalTezConf.getInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS,
        TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS_DEFAULT));

    return appContext;

  }
  
  static String getFrameworkClasspath(Configuration conf) {
    Map<String, String> environment = new HashMap<String, String>();

    TezYARNUtils.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$(),
        File.pathSeparator);

    TezYARNUtils.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*",
        File.pathSeparator);

    // Add YARN/COMMON/HDFS jars and conf locations to path
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      TezYARNUtils.addToEnvironment(environment, Environment.CLASSPATH.name(),
          c.trim(), File.pathSeparator);
    }
    return environment.get(Environment.CLASSPATH.name());
  }

  @VisibleForTesting
  static void addLog4jSystemProperties(String logLevel,
      List<String> vargs) {
    vargs.add("-Dlog4j.configuration="
        + TezConfiguration.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + TezConfiguration.TEZ_ROOT_LOGGER_NAME + "=" + logLevel
        + "," + TezConfiguration.TEZ_CONTAINER_LOGGER_NAME);
  }

  static Configuration createFinalTezConfForApp(TezConfiguration tezConf,
      TezConfiguration amConf) {
    Configuration conf = new Configuration(false);
    conf.setQuietMode(true);

    assert tezConf != null;
    assert amConf != null;

    Entry<String, String> entry;
    Iterator<Entry<String, String>> iter = tezConf.iterator();
    while (iter.hasNext()) {
      entry = iter.next();
      // Copy all tez config parameters.
      if (entry.getKey().startsWith(TezConfiguration.TEZ_PREFIX)) {
        conf.set(entry.getKey(), entry.getValue());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding tez dag am parameter from conf: " + entry.getKey()
            + ", with value: " + entry.getValue());
        }
      }
    }

    iter = amConf.iterator();
    while (iter.hasNext()) {
      entry = iter.next();
      // Copy all tez config parameters.
      if (entry.getKey().startsWith(TezConfiguration.TEZ_PREFIX)) {
        conf.set(entry.getKey(), entry.getValue());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding tez dag am parameter from amConf: " + entry.getKey()
              + ", with value: " + entry.getValue());
        }
      }
    }
    return conf;
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

  private static Path localizeDagPlanAsText(DAGPlan dagPB, FileSystem fs,
      Path appStagingDir, ApplicationId appId) throws IOException {
    Path textPath = new Path(appStagingDir,
        TezConfiguration.TEZ_PB_PLAN_TEXT_NAME + "." + appId.toString());
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

  static DAGClientAMProtocolBlockingPB getSessionAMProxy(YarnClient yarnClient,
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

    final InetSocketAddress serviceAddr = new InetSocketAddress(amHost, amRpcPort);
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
}
