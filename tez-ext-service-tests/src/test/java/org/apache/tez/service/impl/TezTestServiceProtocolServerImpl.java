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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.service.ContainerRunner;
import org.apache.tez.service.TezTestServiceProtocolBlockingPB;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerRequestProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerResponseProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.SubmitWorkRequestProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.SubmitWorkResponseProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezTestServiceProtocolServerImpl extends AbstractService
    implements TezTestServiceProtocolBlockingPB {

  private static final Logger LOG = LoggerFactory.getLogger(TezTestServiceProtocolServerImpl.class);

  private final ContainerRunner containerRunner;
  private RPC.Server server;
  private final AtomicReference<InetSocketAddress> bindAddress;


  public TezTestServiceProtocolServerImpl(ContainerRunner containerRunner,
                                          AtomicReference<InetSocketAddress> address) {
    super(TezTestServiceProtocolServerImpl.class.getSimpleName());
    this.containerRunner = containerRunner;
    this.bindAddress = address;
  }

  @Override
  public RunContainerResponseProto runContainer(RpcController controller,
                                                RunContainerRequestProto request) throws
      ServiceException {
    LOG.info("Received request: " + request);
    try {
      containerRunner.queueContainer(request);
    } catch (TezException e) {
      throw new ServiceException(e);
    }
    return RunContainerResponseProto.getDefaultInstance();
  }

  @Override
  public SubmitWorkResponseProto submitWork(RpcController controller, SubmitWorkRequestProto request) throws
      ServiceException {
    LOG.info("Received submitWork request: " + request);
    try {
      containerRunner.submitWork(request);
    } catch (TezException e) {
      throw new ServiceException(e);
    }
    return SubmitWorkResponseProto.getDefaultInstance();
  }


  @Override
  public void serviceStart() {
    Configuration conf = getConfig();

    int numHandlers = 3;
    InetSocketAddress addr = new InetSocketAddress(0);

    try {
      server = createServer(TezTestServiceProtocolBlockingPB.class, addr, conf, numHandlers,
          TezTestServiceProtocolProtos.TezTestServiceProtocol.newReflectiveBlockingService(this));
      server.start();
    } catch (IOException e) {
      LOG.error("Failed to run RPC Server", e);
      throw new RuntimeException(e);
    }

    InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
    this.bindAddress.set(NetUtils.createSocketAddrForHost(
        serverBindAddress.getAddress().getCanonicalHostName(),
        serverBindAddress.getPort()));
    LOG.info("Instantiated TestTestServiceListener at " + bindAddress);
  }

  @Override
  public void serviceStop() {
    if (server != null) {
      server.stop();
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  InetSocketAddress getBindAddress() {
    return this.bindAddress.get();
  }

  private RPC.Server createServer(Class<?> pbProtocol, InetSocketAddress addr, Configuration conf,
                                  int numHandlers, BlockingService blockingService) throws
      IOException {
    RPC.setProtocolEngine(conf, pbProtocol, ProtobufRpcEngine.class);
    RPC.Server server = new RPC.Builder(conf)
        .setProtocol(pbProtocol)
        .setInstance(blockingService)
        .setBindAddress(addr.getHostName())
        .setPort(0)
        .setNumHandlers(numHandlers)
        .build();
    // TODO Add security.
    return server;
  }
}
