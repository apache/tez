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

package org.apache.tez.runtime.library.input;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestOrderedGroupedKVInput {

  @Test(timeout = 5000)
  public void testInterruptWhileAwaitingInput() throws IOException, TezException {

    InputContext inputContext = createMockInputContext();
    OrderedGroupedKVInput kvInput = new OrderedGroupedKVInputForTest(inputContext, 10);
    kvInput.initialize();

    kvInput.start();

    try {
      kvInput.getReader();
      Assert.fail("getReader should not return since underlying inputs are not ready");
    } catch (IOException e) {
      Assert.assertTrue(e instanceof IOInterruptedException);
    }

  }


  private InputContext createMockInputContext() throws IOException {
    InputContext inputContext = mock(InputContext.class);
    Configuration conf = new TezConfiguration();
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    String[] workingDirs = new String[]{"workDir1"};
    TezCounters counters = new TezCounters();


    doReturn(payLoad).when(inputContext).getUserPayload();
    doReturn(workingDirs).when(inputContext).getWorkDirs();
    doReturn(200 * 1024 * 1024l).when(inputContext).getTotalMemoryAvailableToTask();
    doReturn(counters).when(inputContext).getCounters();

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();

        if (args[1] instanceof MemoryUpdateCallbackHandler) {
          MemoryUpdateCallbackHandler memUpdateCallbackHandler =
              (MemoryUpdateCallbackHandler) args[1];
          memUpdateCallbackHandler.memoryAssigned(200 * 1024 * 1024);
        } else {
          Assert.fail();
        }
        return null;
      }
    }).when(inputContext).requestInitialMemory(any(long.class),
        any(MemoryUpdateCallbackHandler.class));

    return inputContext;
  }

  static class OrderedGroupedKVInputForTest extends OrderedGroupedKVInput {

    public OrderedGroupedKVInputForTest(InputContext inputContext, int numPhysicalInputs) {
      super(inputContext, numPhysicalInputs);
    }

    Shuffle createShuffle() throws IOException {
      Shuffle shuffle = mock(Shuffle.class);
      try {
        doThrow(new InterruptedException()).when(shuffle).waitForInput();
      } catch (InterruptedException e) {
        Assert.fail();
      } catch (TezException e) {
        Assert.fail();
      }
      return shuffle;
    }
  }

}
