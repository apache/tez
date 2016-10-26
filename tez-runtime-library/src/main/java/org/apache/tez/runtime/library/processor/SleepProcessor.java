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

package org.apache.tez.runtime.library.processor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

/**
 * A simple sleep processor implementation that sleeps for the configured
 * time in milliseconds.
 *
 * @see SleepProcessorConfig for configuring the SleepProcessor
 */
@Private
public class SleepProcessor extends AbstractLogicalIOProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(SleepProcessor.class);

  private int timeToSleepMS;
  protected Map<String, LogicalInput> inputs;
  protected Map<String, LogicalOutput> outputs;

  Timer progressTimer = new Timer();
  TimerTask progressTask = new TimerTask() {

    @Override
    public void run() {
      try {
        float progSum = 0.0f;
        if (inputs != null) {
          for(LogicalInput input : inputs.values()) {
            if (input instanceof AbstractLogicalInput) {
              progSum += ((AbstractLogicalInput) input).getProgress();
            }
          }
          float progress = (1.0f) * progSum / inputs.size();
          getContext().setProgress(progress);
        }
      } catch (IOException e) {
        LOG.warn("Encountered IOException during Processor progress update" +
            e.getMessage());
      } catch (InterruptedException e) {
        LOG.warn("Encountered InterruptedException during Processor progress" +
            "update" + e.getMessage());
      }
    }
  };

  public SleepProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize()
    throws Exception {
    if (getContext().getUserPayload() == null) {
      LOG.info("No processor user payload specified"
        + ", using default timeToSleep of 1 ms");
      timeToSleepMS = 1;
    } else {
      SleepProcessorConfig cfg =
        new SleepProcessorConfig();
      cfg.fromUserPayload(getContext().getUserPayload());
      timeToSleepMS = cfg.getTimeToSleepMS();
    }
    LOG.info("Initialized SleepProcessor, timeToSleepMS=" + timeToSleepMS);
  }

  @Override
  public void run(Map<String, LogicalInput> _inputs,
                  Map<String, LogicalOutput> _outputs) throws Exception {
    inputs = _inputs;
    outputs = _outputs;
    LOG.info("Running the Sleep Processor, sleeping for "
      + timeToSleepMS + " ms");
    for (LogicalInput input : _inputs.values()) {
      input.start();
    }
    progressTimer.schedule(progressTask, 0, 100);
    for (LogicalOutput output : _outputs.values()) {
      output.start();
    }
    try {
      Thread.sleep(timeToSleepMS);
    } catch (InterruptedException ie) {
      // ignore
    }
  }

  @Override
  public void handleEvents(List<Event> processorEvents) {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
    progressTimer.cancel();
  }

  /**
   * Configuration for the Sleep Processor.
   * Only configuration option is time to sleep in milliseconds.
   */
  public static class SleepProcessorConfig {
    private int timeToSleepMS;
    private final Charset charSet = Charsets.UTF_8;

    public SleepProcessorConfig() {
    }

    /**
     * @param timeToSleepMS Time to sleep in milliseconds
     */
    public SleepProcessorConfig (int timeToSleepMS) {
      this.timeToSleepMS = timeToSleepMS;
    }

    public UserPayload toUserPayload() {
      return UserPayload.create(ByteBuffer.wrap(Integer.toString(timeToSleepMS).getBytes(
          charSet)));
    }

    public void fromUserPayload(UserPayload userPayload) throws CharacterCodingException {
      timeToSleepMS = Integer.valueOf(charSet.newDecoder().decode(userPayload.getPayload()).toString()).intValue();
    }

    public int getTimeToSleepMS() {
      return timeToSleepMS;
    }
  }

}
