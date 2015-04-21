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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

public class VertexManagerPluginForTest extends VertexManagerPlugin {
  VertexManagerPluginForTestConfig pluginConfig = new VertexManagerPluginForTestConfig();
  
  public static class VertexManagerPluginForTestConfig {
    Configuration conf = new Configuration(false);

    static final String RECONFIGURE_ON_START = "reconfigureOnStart";
    static final String NUM_TASKS = "numTasks";
    
    public void setReconfigureOnStart(boolean value) {
      conf.setBoolean(RECONFIGURE_ON_START, value);
    }
    
    public void setNumTasks(int value) {
      conf.setInt(NUM_TASKS, value);
    }
    
    boolean getReconfigureOnStart() {
      return conf.getBoolean(RECONFIGURE_ON_START, false);
    }
    
    int getNumTasks() {
      return conf.getInt(NUM_TASKS, 1);
    }
    
    public ByteBuffer getPayload() {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(b);
      try {
        conf.write(out);
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
      return ByteBuffer.wrap(b.toByteArray());
    }
    
    void initialize(ByteBuffer buff) {
      ByteBuffer copy = ByteBuffer.allocate(buff.capacity());
      copy.put(buff);
      copy.flip();
      ByteArrayInputStream b = new ByteArrayInputStream(copy.array());
      DataInputStream in = new DataInputStream(b);
      try {
        conf.readFields(in);
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
  }
  public VertexManagerPluginForTest(VertexManagerPluginContext context) {
    super(context);
  }
  

  @Override
  public void initialize() {
    UserPayload payload = getContext().getUserPayload();
    if (payload != null && payload.getPayload() != null) {
      pluginConfig.initialize(getContext().getUserPayload().getPayload());
    }
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    if (pluginConfig.getReconfigureOnStart()) {
      getContext().reconfigureVertex(pluginConfig.getNumTasks(), null, null);
    }
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {}

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {}

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {}
}
