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

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDAGClientServer {

  // try 10 times to allocate random port, fail it if no one is succeed.
  @Test(timeout = 5000)
  public void testPortRange() {
    boolean succeedToAllocate = false;
    Random rand = new Random();
    for (int i = 0; i < 10; ++i) {
      int nextPort = 1024 + rand.nextInt(65535 - 1024);
      if (testPortRange(nextPort)) {
        succeedToAllocate = true;
      }
    }
    if (!succeedToAllocate) {
      fail("Can not allocate free port even in 10 iterations for DAGClientServer");
    }
  }

  private boolean testPortRange(int port) {
    DAGClientServer clientServer = null;
    boolean succeedToAllocate = true;
    try {
      DAGClientHandler mockDAGClientHander = mock(DAGClientHandler.class);
      ApplicationAttemptId mockAppAttempId = mock(ApplicationAttemptId.class);
      Configuration conf = new Configuration();
      conf.set(TezConfiguration.TEZ_AM_CLIENT_AM_PORT_RANGE, port + "-" + port);
      clientServer = new DAGClientServer(mockDAGClientHander, mockAppAttempId, mock(FileSystem.class));
      clientServer.init(conf);
      clientServer.start();
      int resultedPort = clientServer.getBindAddress().getPort();
      System.out.println("bind to address: " + clientServer.getBindAddress());
      assertEquals(port, resultedPort);
    } catch (TezUncheckedException e) {
      assertTrue(e.getMessage().contains(
          "Could not find a free port in " + port + "-" + port));
      succeedToAllocate = false;
    } finally {
      if (clientServer != null) {
        try {
          clientServer.close();
        } catch (IOException e) {
          e.printStackTrace();
          fail("fail to stop DAGClientServer");
        }
      }
    }
    return succeedToAllocate;
  }
}
