/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.task;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.apache.tez.client.registry.zookeeper.ZkConfig;
import org.apache.tez.common.TezClassLoader;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.hadoop.shim.HadoopShimsLoader;
import org.apache.tez.runtime.api.impl.LocalExecutionContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneTezWorker {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneTezWorker.class);

  public static void main(String[] args) throws Exception {
    TezClassLoader.setupTezClassLoader();
    final Configuration defaultConf = setupConfiguration();

    ZkAMRegistryClient registry = ZkAMRegistryClient.getClient(defaultConf);
    registry.start();

    // Wait until registry initializes
    while (!registry.isInitialized()) {
      TimeUnit.SECONDS.sleep(5);
    }

    AMRecord amRecord = discoverAM(registry);
    String host = amRecord.getHostName();
    String portRange = defaultConf.getTrimmed(TezConfiguration.TEZ_AM_TASK_AM_PORT_RANGE, "12000-12000");
    int port = Integer.parseInt(portRange.split("[-,]")[0]);
    String appIdStr = amRecord.getApplicationId().toString();
    int attemptNumber = 1;

    ZkConfig zkconfig = new ZkConfig(defaultConf);
    String zkQuorum = defaultConf.get(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM);
    CuratorFramework zkWorkerClient = CuratorFrameworkFactory.newClient(zkQuorum, zkconfig.getRetryPolicy());
    zkWorkerClient.start();

    Credentials credentials = setupCredentials(defaultConf, appIdStr);

    String[] localDirs = TezCommonUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS.name()));
    Map<String, String> envMap = new HashMap<>(System.getenv());
    int shufflePort = startEmbeddedShuffleHandler(defaultConf, amRecord.getApplicationId(), credentials, localDirs, envMap);

    String containerIdentifier = registerWorkerInZK(zkWorkerClient, zkconfig.getZkTaskNameSpace(), appIdStr, host, shufflePort);

    LOG.info("ZK Mode: Discovered AM {} at {}:{}", appIdStr, host, port);

    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());

    // Fix for Docker PID issue: use containerIdentifier if JVM_PID is 1 (or unset)
    String pid = System.getenv().get("JVM_PID");
    if (pid == null || pid.equals("1")) {
      pid = containerIdentifier;
    }

    CallerContext.setCurrent(new CallerContext.Builder("tez_" + appIdStr).build());
    LOG.info("StandaloneTezWorker starting with PID={}, containerIdentifier={}", pid, containerIdentifier);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Info from ZK registry: AM-host: {} AM-port: {} containerIdentifier: {} appAttemptNumber: {} " +
                "tokenIdentifier: {}", host, port, containerIdentifier, attemptNumber, appIdStr);
    }

    UserGroupInformation.setConfiguration(defaultConf);
    HadoopShim hadoopShim = new HadoopShimsLoader(defaultConf).getHadoopShim();

    if (LOG.isInfoEnabled()) {
      String systemPropsToLog = TezCommonUtils.getSystemPropertiesToLog(defaultConf);
      LOG.info(systemPropsToLog);
    }

    TezChild tezChild =
        TezChild.newTezChild(defaultConf, host, port, containerIdentifier, appIdStr, attemptNumber, localDirs,
            System.getenv(Environment.PWD.name()), envMap, pid,
            new LocalExecutionContext(java.net.InetAddress.getLocalHost().getHostName()), credentials,
            Runtime.getRuntime().maxMemory(), System.getenv(ApplicationConstants.Environment.USER.toString()), null,
            true, hadoopShim);

    TezChild.ContainerExecutionResult result = tezChild.run();
    LOG.info("StandaloneTezWorker is about to exit from main(), run() returned result: {}", result.toString());
  }

  private static Configuration setupConfiguration() throws Exception {
    Configuration defaultConf = new Configuration();
    DAGProtos.ConfigurationProto confProtoBefore = TezUtilsInternal.loadConfProtoFromText();
    TezUtilsInternal.addUserSpecifiedTezConfiguration(defaultConf, confProtoBefore.getConfKeyValuesList());

    /*
     Disable local fetch optimization for standalone mode since containers do not share
     a local filesystem (unlike NodeManagers in a classic YARN cluster).
     TODO: Later, this code path could be optimized so fetchers know that if they are on
     the same physical host and the folder is shared, they can leverage local fetch.
    */
    defaultConf.setBoolean("tez.runtime.optimize.local.fetch", false);
    return defaultConf;
  }

  private static AMRecord discoverAM(ZkAMRegistryClient registry) {
    List<AMRecord> records = registry.getAllRecords();
    if (records.isEmpty()) {
      throw new RuntimeException("No AM found in ZooKeeper registry");
    }

    String expectedAppId = System.getenv("TEZ_APPLICATION_ID");
    if (expectedAppId != null && !expectedAppId.trim().isEmpty()) {
      for (AMRecord record : records) {
        if (record.getApplicationId().toString().equals(expectedAppId.trim())) {
          return record;
        }
      }
      throw new RuntimeException("No AM found in ZooKeeper registry for TEZ_APPLICATION_ID=" + expectedAppId);
    }

    LOG.warn(
        "TEZ_APPLICATION_ID is not provided in environment variables. Falling back to picking the first discoverable " +
        "AM.");
    return records.getFirst();
  }

  private static String registerWorkerInZK(CuratorFramework zkWorkerClient, String zkTaskNameSpace, String appIdStr,
                                           String host, int shufflePort) {
    String baseContainerId = appIdStr.replace("application_", "container_");
    int workerSeq = 1;

    while (true) {
      String containerIdentifier = baseContainerId + "_01_" + String.format("%06d", workerSeq);
      String workerPath = zkTaskNameSpace + "/" + appIdStr + "/" + containerIdentifier;
      try {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.node.ObjectNode jsonNode = mapper.createObjectNode();
        jsonNode.put("host", host);
        jsonNode.put("shufflePort", shufflePort);
        String jsonStr = jsonNode.toString();

        zkWorkerClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
            .forPath(workerPath, jsonStr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        LOG.info("Registered Tez Worker in ZK at path: {}", workerPath);
        return containerIdentifier;
      } catch (KeeperException.NodeExistsException e) {
        workerSeq++;
      } catch (Exception e) {
        throw new RuntimeException("Failed to register worker in ZK", e);
      }
    }
  }

  private static Credentials setupCredentials(Configuration conf, String appIdStr) {
    Credentials credentials = new Credentials();
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(appIdStr));
    Token<JobTokenIdentifier> sessionToken = new Token<>(identifier, TezCommonUtils.createJobTokenSecretManager(conf));
    credentials.addToken(new Text("SessionToken"), sessionToken);
    TokenCache.setSessionToken(sessionToken, credentials);
    return credentials;
  }

  private static int startEmbeddedShuffleHandler(Configuration defaultConf, ApplicationId appId,
                                                  Credentials credentials, String[] localDirs,
                                                  Map<String, String> envMap) {
    try {
      Class<?> shuffleHandlerClass = Class.forName("org.apache.tez.auxservices.ShuffleHandler");
      org.apache.hadoop.service.Service shuffleHandler =
          (org.apache.hadoop.service.Service) shuffleHandlerClass.getDeclaredConstructor().newInstance();

      Configuration shuffleConf = new Configuration(defaultConf);
      shuffleConf.setInt("tez.shuffle.port", 0);
      shuffleConf.setStrings(Environment.LOCAL_DIRS.name(), localDirs);

      final LocalDirAllocator localDirAllocator = new LocalDirAllocator(Environment.LOCAL_DIRS.name());
      AuxiliaryLocalPathHandler pathHandler = new AuxiliaryLocalPathHandler() {
        private String stripUserCache(String path) {
          return path.replaceAll("^usercache/[^/]+/appcache/[^/]+/", "");
        }

        @Override
        public Path getLocalPathForRead(String path) throws java.io.IOException {
          return localDirAllocator.getLocalPathToRead(stripUserCache(path), shuffleConf);
        }

        @Override
        public Path getLocalPathForWrite(String path) throws java.io.IOException {
          return localDirAllocator.getLocalPathForWrite(stripUserCache(path), shuffleConf);
        }

        @Override
        public Path getLocalPathForWrite(String path, long size) throws java.io.IOException {
          return localDirAllocator.getLocalPathForWrite(stripUserCache(path), size, shuffleConf);
        }

        @Override
        public Iterable<Path> getAllLocalPathsForRead(String path) throws java.io.IOException {
          return localDirAllocator.getAllLocalPathsToRead(stripUserCache(path), shuffleConf);
        }
      };

      Class<?> auxServiceClass = Class.forName("org.apache.hadoop.yarn.server.api.AuxiliaryService");
      auxServiceClass.getMethod("setAuxiliaryLocalPathHandler", AuxiliaryLocalPathHandler.class)
          .invoke(shuffleHandler, pathHandler);

      shuffleHandler.init(shuffleConf);
      shuffleHandler.start();

      ByteBuffer shuffleMetaData = (ByteBuffer) shuffleHandlerClass.getMethod("getMetaData").invoke(shuffleHandler);
      String shuffleAuxName = defaultConf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
          TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
      AuxiliaryServiceHelper.setServiceDataIntoEnv(shuffleAuxName, shuffleMetaData, envMap);

      Class<?> initContextClass = Class.forName("org.apache.hadoop.yarn.server.api.ApplicationInitializationContext");
      Object initContext = initContextClass.getDeclaredConstructor(String.class, ApplicationId.class, ByteBuffer.class)
          .newInstance(System.getenv(ApplicationConstants.Environment.USER.toString()), appId,
              TezCommonUtils.convertJobTokenToBytes(TokenCache.getSessionToken(credentials)));

      shuffleHandlerClass.getMethod("initializeApplication", initContextClass).invoke(shuffleHandler, initContext);

      int shufflePort = (int) shuffleHandlerClass.getMethod("getPort").invoke(shuffleHandler);
      LOG.info("Started embedded ShuffleHandler via reflection on port {}", shufflePort);
      return shufflePort;
    } catch (Exception e) {
      LOG.error("Could not start embedded ShuffleHandler. Standalone worker cannot serve shuffle data.", e);
      throw new RuntimeException("Failed to start embedded ShuffleHandler", e);
    }
  }
}
