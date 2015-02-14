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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.tez.service.TezTestServiceProtocolBlockingPB;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerRequestProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerResponseProto;


public class TezTestServiceProtocolClientImpl implements TezTestServiceProtocolBlockingPB {

  private final Configuration conf;
  private final InetSocketAddress serverAddr;
  TezTestServiceProtocolBlockingPB proxy;


  public TezTestServiceProtocolClientImpl(Configuration conf, String hostname, int port) {
    this.conf = conf;
    this.serverAddr = NetUtils.createSocketAddr(hostname, port);
  }

  @Override
  public RunContainerResponseProto runContainer(RpcController controller,
                                                RunContainerRequestProto request) throws
      ServiceException {
    try {
      return getProxy().runContainer(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public TezTestServiceProtocolProtos.SubmitWorkResponseProto submitWork(RpcController controller,
                                                                         TezTestServiceProtocolProtos.SubmitWorkRequestProto request) throws
      ServiceException {
    try {
      return getProxy().submitWork(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }


  public TezTestServiceProtocolBlockingPB getProxy() throws IOException {
    if (proxy == null) {
      proxy = createProxy();
    }
    return proxy;
  }

  public TezTestServiceProtocolBlockingPB createProxy() throws IOException {
    TezTestServiceProtocolBlockingPB p;
    // TODO Fix security
    RPC.setProtocolEngine(conf, TezTestServiceProtocolBlockingPB.class, ProtobufRpcEngine.class);
    p = (TezTestServiceProtocolBlockingPB) RPC
        .getProxy(TezTestServiceProtocolBlockingPB.class, 0, serverAddr, conf);
    return p;
  }
}