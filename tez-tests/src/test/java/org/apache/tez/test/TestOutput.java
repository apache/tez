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

package org.apache.tez.test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.api.events.DataMovementEvent;

import com.google.common.collect.Lists;

public class TestOutput extends AbstractLogicalOutput {
  private static final Logger LOG = LoggerFactory.getLogger(TestOutput.class);

  public static final String COUNTER_NAME = "TestOutput";
  public TestOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }

  public static OutputDescriptor getOutputDesc(UserPayload payload) {
    OutputDescriptor desc = OutputDescriptor.create(TestOutput.class.getName());
    if (payload != null) {
      desc.setUserPayload(payload);
    }
    return desc;
  }
  
  int output;
  
  @Override
  public List<Event> initialize() throws Exception {
    getContext().requestInitialMemory(0l, null); //Mandatory call
    return Collections.emptyList();
  }
  
  void write(int value) {
    this.output = value;
  }

  @Override
  public void start() {
  }

  @Override
  public Writer getWriter() throws Exception {
    return null;
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
  }

  @Override
  public List<Event> close() throws Exception {
    LOG.info("Sending data movement event with value: " + output);
    getContext().getCounters().findCounter(COUNTER_NAME, COUNTER_NAME).increment(1);;
    ByteBuffer result = ByteBuffer.allocate(4).putInt(output);
    result.flip();
    List<Event> events = Lists.newArrayListWithCapacity(getNumPhysicalOutputs());
    for (int i = 0; i < getNumPhysicalOutputs(); i++) {
      DataMovementEvent event = DataMovementEvent.create(i, result);
      events.add(event);
    }
    return events;
  }

}
