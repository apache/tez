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

package org.apache.tez.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.ProgressFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProgressHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ProgressHelper.class);
  private String processorName;
  protected final Map<String, LogicalInput> inputs;
  final ProcessorContext processorContext;

  volatile ScheduledExecutorService scheduledExecutorService;
  Runnable monitorProgress = new Runnable() {
    @Override
    public void run() {
      try {
        float progSum = 0.0f;
        float progress;
        if (inputs != null && inputs.size() != 0) {
          for (LogicalInput input : inputs.values()) {
            if (input instanceof AbstractLogicalInput) {
              float inputProgress = ((AbstractLogicalInput) input).getProgress();
              if (inputProgress >= 0.0f && inputProgress <= 1.0f) {
                progSum += inputProgress;
              }
            }
          }
          progress = (1.0f) * progSum / inputs.size();
        } else {
          progress = 1.0f;
        }
        processorContext.setProgress(progress);
      } catch (ProgressFailedException pe) {
        LOG.warn("Encountered ProgressFailedException during Processor progress update"
            + pe);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered InterruptedException during Processor progress update"
            + ie);
      }
    }
  };

  public ProgressHelper(Map<String, LogicalInput> _inputs, ProcessorContext context, String processorName) {
    this.inputs = _inputs;
    this.processorContext = context;
    this.processorName = processorName;
  }

  public void scheduleProgressTaskService(long delay, long period) {
    scheduledExecutorService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("TaskProgressService{" + processorName+ ":" + processorContext.getTaskVertexName()
            + "} #%d").build());
    scheduledExecutorService.scheduleWithFixedDelay(monitorProgress, delay, period,
        TimeUnit.MILLISECONDS);
  }

  public void shutDownProgressTaskService() {
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
      scheduledExecutorService = null;
    }
  }

}
