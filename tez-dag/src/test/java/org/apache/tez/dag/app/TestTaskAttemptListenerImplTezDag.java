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

package org.apache.tez.dag.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

public class TestTaskAttemptListenerImplTezDag {

  // try 10 times to allocate random port, fail it if no one is succeed.
  @Test(timeout = 5000)
  public void testPortRange() {
    boolean succeedToAllocate = false;
    Random rand = new Random();
    for (int i = 0; i < 10; ++i) {
      int nextPort = 1024 + rand.nextInt(65535 - 1024);
      if (testPortRange(nextPort)) {
        succeedToAllocate = true;
        break;
      }
    }
    if (!succeedToAllocate) {
      fail("Can not allocate free port even in 10 iterations for TaskAttemptListener");
    }
  }

  @Test(timeout = 5000)
  public void testPortRange_NotSpecified() {
    Configuration conf = new Configuration();
    TaskAttemptListenerImpTezDag taskAttemptListener = new TaskAttemptListenerImpTezDag(
        mock(AppContext.class), mock(TaskHeartbeatHandler.class),
        mock(ContainerHeartbeatHandler.class), null);
    // no exception happen, should started properly
    taskAttemptListener.init(conf);
    taskAttemptListener.start();
  }

  private boolean testPortRange(int port) {
    boolean succeedToAllocate = true;
    TaskAttemptListenerImpTezDag taskAttemptListener = null;
    try {
      Configuration conf = new Configuration();
      conf.set(TezConfiguration.TEZ_AM_TASK_AM_PORT_RANGE, port + "-" + port);
      taskAttemptListener = new TaskAttemptListenerImpTezDag(
          mock(AppContext.class), mock(TaskHeartbeatHandler.class),
          mock(ContainerHeartbeatHandler.class), null);
      taskAttemptListener.init(conf);
      taskAttemptListener.start();
      int resultedPort = taskAttemptListener.getAddress().getPort();
      assertEquals(port, resultedPort);
    } catch (Exception e) {
      succeedToAllocate = false;
    } finally {
      if (taskAttemptListener != null) {
        try {
          taskAttemptListener.close();
        } catch (IOException e) {
          e.printStackTrace();
          fail("fail to stop TaskAttemptListener");
        }
      }
    }
    return succeedToAllocate;
  }
}
