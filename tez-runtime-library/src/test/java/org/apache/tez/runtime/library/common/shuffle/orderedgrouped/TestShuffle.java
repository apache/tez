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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.common.Constants;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestShuffle {

  private static final Logger LOG = LoggerFactory.getLogger(TestShuffle.class);

  @Test(timeout = 10000)
  public void testSchedulerTerminatesOnException() throws IOException, InterruptedException {

    InputContext inputContext = createTezInputContext();
    TezConfiguration conf = new TezConfiguration();
    conf.setLong(Constants.TEZ_RUNTIME_TASK_MEMORY, 300000l);
    Shuffle shuffle = new Shuffle(inputContext, conf, 1, 3000000l);
    try {
      shuffle.run();
      ShuffleScheduler scheduler = shuffle.scheduler;
      MergeManager mergeManager = shuffle.merger;
      assertFalse(scheduler.isShutdown());
      assertFalse(mergeManager.isShutdown());

      String exceptionMessage = "Simulating fetch failure";
      shuffle.reportException(new IOException(exceptionMessage));

      while (!scheduler.isShutdown()) {
        Thread.sleep(100l);
      }
      assertTrue(scheduler.isShutdown());

      while (!mergeManager.isShutdown()) {
        Thread.sleep(100l);
      }
      assertTrue(mergeManager.isShutdown());

      ArgumentCaptor<Throwable> throwableArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
      ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
      verify(inputContext, times(1)).fatalError(throwableArgumentCaptor.capture(),
          stringArgumentCaptor.capture());

      Throwable t = throwableArgumentCaptor.getValue();
      assertTrue(t.getCause().getMessage().contains(exceptionMessage));

    } finally {
      shuffle.shutdown();
    }


  }


  private InputContext createTezInputContext() throws IOException {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    InputContext inputContext = mock(InputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn("sourceVertex").when(inputContext).getSourceVertexName();
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    ExecutionContext executionContext = new ExecutionContextImpl("localhost");
    doReturn(executionContext).when(inputContext).getExecutionContext();
    ByteBuffer shuffleBuffer = ByteBuffer.allocate(4).putInt(0, 4);
    doReturn(shuffleBuffer).when(inputContext).getServiceProviderMetaData(anyString());
    Token<JobTokenIdentifier>
        sessionToken = new Token<JobTokenIdentifier>(new JobTokenIdentifier(new Text("text")),
        new JobTokenSecretManager());
    ByteBuffer tokenBuffer = TezCommonUtils.serializeServiceData(sessionToken);
    doReturn(tokenBuffer).when(inputContext).getServiceConsumerMetaData(anyString());
    return inputContext;
  }
}
