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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.api.events.DataMovementEvent;

import com.google.common.collect.Lists;

public class TestOutput implements LogicalOutput {
  private static final Log LOG = LogFactory.getLog(TestOutput.class);
  
  public static OutputDescriptor getOutputDesc(byte[] payload) {
    return new OutputDescriptor(TestOutput.class.getName()).
        setUserPayload(payload);
  }
  
  int output;
  int numOutputs;
  TezOutputContext outputContext;
  
  @Override
  public List<Event> initialize(TezOutputContext outputContext)
      throws Exception {
    this.outputContext = outputContext;
    this.outputContext.requestInitialMemory(0l, null); //Mandatory call
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
    byte[] result = ByteBuffer.allocate(4).putInt(output).array();
    List<Event> events = Lists.newArrayListWithCapacity(numOutputs);
    for (int i = 0; i < numOutputs; i++) {
      DataMovementEvent event = new DataMovementEvent(i, result);
      events.add(event);
    }
    return events;
  }

  @Override
  public void setNumPhysicalOutputs(int numOutputs) {
    this.numOutputs = numOutputs;
  }

}
