/*
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

package org.apache.tez.runtime.task;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestTaskReporter {

  @Test(timeout = 10000)
  public void testContinuousHeartbeatsOnMaxEvents() throws Exception {

    final Object lock = new Object();
    final AtomicBoolean hb2Done = new AtomicBoolean(false);

    TezTaskUmbilicalProtocol mockUmbilical = mock(TezTaskUmbilicalProtocol.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        TezHeartbeatRequest request = (TezHeartbeatRequest) args[0];
        if (request.getRequestId() == 1 || request.getRequestId() == 2) {
          TezHeartbeatResponse response = new TezHeartbeatResponse(createEvents(5));
          response.setLastRequestId(request.getRequestId());
          return response;
        } else if (request.getRequestId() == 3) {
          TezHeartbeatResponse response = new TezHeartbeatResponse(createEvents(1));
          response.setLastRequestId(request.getRequestId());
          synchronized (lock) {
            hb2Done.set(true);
            lock.notify();
          }
          return response;
        } else {
          throw new TezUncheckedException("Invalid request id for test: " + request.getRequestId());
        }
      }
    }).when(mockUmbilical).heartbeat(any(TezHeartbeatRequest.class));

    TezTaskAttemptID mockTaskAttemptId = mock(TezTaskAttemptID.class);
    LogicalIOProcessorRuntimeTask mockTask = mock(LogicalIOProcessorRuntimeTask.class);
    doReturn("vertexName").when(mockTask).getVertexName();
    doReturn(mockTaskAttemptId).when(mockTask).getTaskAttemptID();

    // Setup the sleep time to be way higher than the test timeout
    TaskReporter.HeartbeatCallable heartbeatCallable =
        new TaskReporter.HeartbeatCallable(mockTask, mockUmbilical, 100000, 100000, 5,
            new AtomicLong(0),
            "containerIdStr");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(heartbeatCallable);
    try {
      synchronized (lock) {
        if (!hb2Done.get()) {
          lock.wait();
        }
      }
      verify(mockUmbilical, times(3)).heartbeat(any(TezHeartbeatRequest.class));
      Thread.sleep(2000l);
      // Sleep for 2 seconds, less than the callable sleep time. No more invocations.
      verify(mockUmbilical, times(3)).heartbeat(any(TezHeartbeatRequest.class));
    } finally {
      executor.shutdownNow();
    }

  }

  private List<TezEvent> createEvents(int numEvents) {
    List<TezEvent> list = Lists.newArrayListWithCapacity(numEvents);
    for (int i = 0; i < numEvents; i++) {
      list.add(mock(TezEvent.class));
    }
    return list;
  }
}
