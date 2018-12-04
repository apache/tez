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

package org.apache.tez.dag.api.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPBServerImpl;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.DAGClientAMProtocol;
import org.apache.tez.dag.app.security.authorize.TezAMPolicyProvider;

import com.google.protobuf.BlockingService;

public class DAGClientServer extends AbstractService {
  static final Logger LOG = LoggerFactory.getLogger(DAGClientServer.class);

  ClientToAMTokenSecretManager secretManager;
  DAGClientHandler realInstance;
  Server server;
  InetSocketAddress bindAddress;
  final FileSystem stagingFs;

  public DAGClientServer(DAGClientHandler realInstance,
      ApplicationAttemptId attemptId, FileSystem stagingFs) {
    super("DAGClientRPCServer");
    this.realInstance = realInstance;
    this.secretManager = new ClientToAMTokenSecretManager(attemptId, null);
    this.stagingFs = stagingFs;
  }

  @Override
  public void serviceStart() {
    try {
      Configuration conf = getConfig();
      InetSocketAddress addr = new InetSocketAddress(0);

      DAGClientAMProtocolBlockingPBServerImpl service =
          new DAGClientAMProtocolBlockingPBServerImpl(realInstance, stagingFs);

      BlockingService blockingService =
                DAGClientAMProtocol.newReflectiveBlockingService(service);

      int numHandlers = conf.getInt(TezConfiguration.TEZ_AM_CLIENT_THREAD_COUNT,
                          TezConfiguration.TEZ_AM_CLIENT_THREAD_COUNT_DEFAULT);
      if (numHandlers < 2) {
        numHandlers = 2;
      }

      server = createServer(DAGClientAMProtocolBlockingPB.class, addr, conf,
                            numHandlers, blockingService, TezConfiguration.TEZ_AM_CLIENT_AM_PORT_RANGE);
      
      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          false)) {
        refreshServiceAcls(conf, new TezAMPolicyProvider());
      }

      server.start();
      InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
      this.bindAddress = NetUtils.createSocketAddrForHost(
          serverBindAddress.getAddress().getCanonicalHostName(),
          serverBindAddress.getPort());
      LOG.info("Instantiated DAGClientRPCServer at " + bindAddress);
    } catch (Exception e) {
      LOG.error("Failed to start DAGClientServer: ", e);
      throw new TezUncheckedException(e);
    }
  }

  private void refreshServiceAcls(Configuration configuration, PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public void serviceStop() {
    if(server != null) {
      server.stop();
    }
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
  
  public void setClientAMSecretKey(ByteBuffer key) {
    if (key != null && key.hasRemaining()) {
      // non-empty key. must be useful
      secretManager.setMasterKey(key.array());
    }
  }

  private Server createServer(Class<?> pbProtocol, InetSocketAddress addr, Configuration conf,
      int numHandlers,
      BlockingService blockingService, String portRangeConfig) throws IOException {
    RPC.setProtocolEngine(conf, pbProtocol, ProtobufRpcEngine.class);
    RPC.Server server = new RPC.Builder(conf).setProtocol(pbProtocol)
        .setInstance(blockingService).setBindAddress(addr.getHostString())
        .setPort(addr.getPort()).setNumHandlers(numHandlers).setVerbose(false)
        .setPortRangeConfig(portRangeConfig).setSecretManager(secretManager)
        .build();
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, pbProtocol, blockingService);
    return server;
  }
}
