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

package org.apache.tez.service;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.tez.service.impl.TezTestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniTezTestServiceCluster extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(MiniTezTestServiceCluster.class);

  private final File testWorkDir;
  private final long availableMemory;
  private final int numExecutorsPerService;
  private final String[] localDirs;
  private final Configuration clusterSpecificConfiguration = new Configuration(false);

  private TezTestService tezTestService;

  public static MiniTezTestServiceCluster create(String clusterName, int numExecutorsPerService, long availableMemory, int numLocalDirs) {
    return new MiniTezTestServiceCluster(clusterName, numExecutorsPerService, availableMemory, numLocalDirs);
  }

  // TODO Add support for multiple instances
  private MiniTezTestServiceCluster(String clusterName, int numExecutorsPerService, long availableMemory, int numLocalDirs) {
    super(clusterName + "_TezTestServerCluster");
    Preconditions.checkArgument(numExecutorsPerService > 0);
    Preconditions.checkArgument(availableMemory > 0);
    Preconditions.checkArgument(numLocalDirs > 0);
    String clusterNameTrimmed = clusterName.replace("$", "") + "_TezTestServerCluster";
    File targetWorkDir = new File("target", clusterNameTrimmed);
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(targetWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      LOG.warn("Could not cleanup test workDir: " + targetWorkDir, e);
      throw new RuntimeException("Could not cleanup test workDir: " + targetWorkDir, e);
    }

    if (Shell.WINDOWS) {
      // The test working directory can exceed the maximum path length supported
      // by some Windows APIs and cmd.exe (260 characters).  To work around this,
      // create a symlink in temporary storage with a much shorter path,
      // targeting the full path to the test working directory.  Then, use the
      // symlink as the test working directory.
      String targetPath = targetWorkDir.getAbsolutePath();
      File link = new File(System.getProperty("java.io.tmpdir"),
          String.valueOf(System.currentTimeMillis()));
      String linkPath = link.getAbsolutePath();

      try {
        FileContext.getLocalFSFileContext().delete(new Path(linkPath), true);
      } catch (IOException e) {
        throw new YarnRuntimeException("could not cleanup symlink: " + linkPath, e);
      }

      // Guarantee target exists before creating symlink.
      targetWorkDir.mkdirs();

      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          Shell.getSymlinkCommand(targetPath, linkPath));
      try {
        shexec.execute();
      } catch (IOException e) {
        throw new YarnRuntimeException(String.format(
            "failed to create symlink from %s to %s, shell output: %s", linkPath,
            targetPath, shexec.getOutput()), e);
      }

      this.testWorkDir = link;
    } else {
      this.testWorkDir = targetWorkDir;
    }
    this.numExecutorsPerService = numExecutorsPerService;
    this.availableMemory = availableMemory;

    // Setup Local Dirs
    localDirs = new String[numLocalDirs];
    for (int i = 0 ; i < numLocalDirs ; i++) {
      File f = new File(testWorkDir, "localDir");
      f.mkdirs();
      LOG.info("Created localDir: " + f.getAbsolutePath());
      localDirs[i] = f.getAbsolutePath();
    }
  }

  @Override
  public void serviceInit(Configuration conf) {
    tezTestService = new TezTestService(conf, numExecutorsPerService, availableMemory, localDirs);
    tezTestService.init(conf);

  }

  @Override
  public void serviceStart() {
    tezTestService.start();

    clusterSpecificConfiguration.set(TezTestServiceConfConstants.TEZ_TEST_SERVICE_HOSTS,
        getServiceAddress().getHostName());
    clusterSpecificConfiguration.setInt(TezTestServiceConfConstants.TEZ_TEST_SERVICE_RPC_PORT,
        getServiceAddress().getPort());

    clusterSpecificConfiguration.setInt(
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_NUM_EXECUTORS_PER_INSTANCE,
        numExecutorsPerService);
    clusterSpecificConfiguration.setLong(
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_MEMORY_PER_INSTANCE_MB, availableMemory);
  }

  @Override
  public void serviceStop() {
    if (tezTestService != null) {
      tezTestService.stop();
      tezTestService = null;
    }
  }

  /**
   * return the address at which the service is listening
   * @return host:port
   */
  public InetSocketAddress getServiceAddress() {
    Preconditions.checkState(getServiceState() == STATE.STARTED);
    return tezTestService.getListenerAddress();
  }

  public int getShufflePort() {
    Preconditions.checkState(getServiceState() == STATE.STARTED);
    return tezTestService.getShufflePort();
  }

  public Configuration getClusterSpecificConfiguration() {
    Preconditions.checkState(getServiceState() == STATE.STARTED);
    return clusterSpecificConfiguration;
  }

  // Mainly for verification
  public int getNumSubmissions() {
    return tezTestService.getNumSubmissions();
  }

}