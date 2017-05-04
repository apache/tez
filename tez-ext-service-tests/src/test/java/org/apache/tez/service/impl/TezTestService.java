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

package org.apache.tez.service.impl;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.service.ContainerRunner;
import org.apache.tez.shufflehandler.ShuffleHandler;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerRequestProto;
import org.slf4j.LoggerFactory;

public class TezTestService extends AbstractService implements ContainerRunner {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TezTestService.class);

  private final Configuration shuffleHandlerConf;
  private final int numExecutors;

  private final TezTestServiceProtocolServerImpl server;
  private final ContainerRunnerImpl containerRunner;
  private final String[] localDirs;

  private final AtomicInteger numSubmissions = new AtomicInteger(0);


  private final AtomicReference<InetSocketAddress> address = new AtomicReference<InetSocketAddress>();

  private final TezExecutors sharedExecutor;

  public TezTestService(Configuration conf, int numExecutors, long memoryAvailable, String[] localDirs) {
    super(TezTestService.class.getSimpleName());
    this.numExecutors = numExecutors;
    this.localDirs = localDirs;

    long memoryAvailableBytes = memoryAvailable;
    long jvmMax = Runtime.getRuntime().maxMemory();

    LOG.info(TezTestService.class.getSimpleName() + " created with the following configuration: " +
        "numExecutors=" + numExecutors +
        ", workDirs=" + Arrays.toString(localDirs) +
        ", memoryAvailable=" + memoryAvailable +
        ", jvmMaxMemory=" + jvmMax);

    Preconditions.checkArgument(this.numExecutors > 0);
    Preconditions.checkArgument(this.localDirs != null && this.localDirs.length > 0,
        "Work dirs must be specified");
    Preconditions.checkState(jvmMax >= memoryAvailableBytes,
        "Invalid configuration. Xmx value too small. maxAvailable=" + jvmMax + ", configured=" +
            memoryAvailableBytes);

    this.shuffleHandlerConf = new Configuration(conf);
    // Start Shuffle on a random port
    this.shuffleHandlerConf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    this.shuffleHandlerConf.set(ShuffleHandler.SHUFFLE_HANDLER_LOCAL_DIRS, StringUtils.arrayToString(localDirs));

    this.server = new TezTestServiceProtocolServerImpl(this, address);
    this.sharedExecutor = new TezSharedExecutor(conf);
    this.containerRunner = new ContainerRunnerImpl(numExecutors, localDirs, address,
        memoryAvailableBytes, sharedExecutor);
  }

  @Override
  public void serviceInit(Configuration conf) {
    server.init(conf);
    containerRunner.init(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    ShuffleHandler.initializeAndStart(shuffleHandlerConf);
    String auxiliaryService = getConfig().get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    containerRunner.setShufflePort(auxiliaryService, ShuffleHandler.get().getPort());
    server.start();
    containerRunner.start();
  }

  public void serviceStop() throws Exception {
    containerRunner.stop();
    server.stop();
    ShuffleHandler.get().stop();
    sharedExecutor.shutdownNow();
  }

  public InetSocketAddress getListenerAddress() {
    return server.getBindAddress();
  }

  public int getShufflePort() {
    return ShuffleHandler.get().getPort();
  }



  @Override
  public void queueContainer(RunContainerRequestProto request) throws TezException {
    numSubmissions.incrementAndGet();
    containerRunner.queueContainer(request);
  }

  @Override
  public void submitWork(TezTestServiceProtocolProtos.SubmitWorkRequestProto request) throws
      TezException {
    numSubmissions.incrementAndGet();
    containerRunner.submitWork(request);
  }

  public int getNumSubmissions() {
    return numSubmissions.get();
  }
}
