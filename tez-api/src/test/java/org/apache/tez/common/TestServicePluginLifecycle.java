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

package org.apache.tez.common;

import org.apache.tez.common.plugin.InMemoryInterPluginCommunicator;
import org.apache.tez.common.plugin.InterPluginCommunicator;
import org.apache.tez.common.plugin.InterPluginListener;
import org.apache.tez.common.plugin.ServicePluginAware;
import org.apache.tez.serviceplugins.api.InterPluginId;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class TestServicePluginLifecycle {
  private static InterPluginCommunicator interPluginCommunicator;


  private static class TestServicePluginAware extends ServicePluginAware {
    @Override public void initialize() throws Exception {}

    @Override public void start() throws Exception {}

    @Override public void shutdown() throws Exception {}
  }

  @BeforeClass
  public static void setup() throws Exception {
    interPluginCommunicator = new InMemoryInterPluginCommunicator();
  }

  /**
   * Test we can send objects between plugins using queues
   */
  @Test
  public void testInterPluginCommunicationStorage() {
    TestServicePluginAware firstPlugin = new TestServicePluginAware();
    TestServicePluginAware secondPlugin = new TestServicePluginAware();
    firstPlugin.setInterPluginCommunicator(interPluginCommunicator);
    secondPlugin.setInterPluginCommunicator(interPluginCommunicator);

    // Verify we can send values
    InterPluginId key = InterPluginId.fromObject("key");
    Integer value = 1;
    firstPlugin.put(key, value);
    Assert.assertEquals(value, secondPlugin.peek(key));
    Assert.assertEquals(value, secondPlugin.get(key));
    Assert.assertNull(secondPlugin.get(key));

    // Verify we can send values in both directions
    InterPluginId secondKey = InterPluginId.fromObject("key");
    String secondValue = "2";
    secondPlugin.put(secondKey, value);
    secondPlugin.put(secondKey, secondValue);
    Assert.assertEquals(value, firstPlugin.get(secondKey));
    Assert.assertEquals(secondValue, firstPlugin.get(secondKey));

    secondPlugin.put(secondKey, null);
    Assert.assertNull(firstPlugin.get(secondKey));

  }

  private class Listener extends InterPluginListener {
    List<Object> recorder;
    Listener(List<Object> recorder) {
      this.recorder = recorder;
    }
    @Override
    public void onSentValue(Object value) {
      recorder.add(value);
    }
  }

  /**
   * Test we can send objects between plugins using subscription
   */
  @Test
  public void testInterPluginCommunicationSubscription() {
    TestServicePluginAware firstPlugin = new TestServicePluginAware();
    TestServicePluginAware secondPlugin = new TestServicePluginAware();
    firstPlugin.setInterPluginCommunicator(interPluginCommunicator);
    secondPlugin.setInterPluginCommunicator(interPluginCommunicator);

    InterPluginId key = InterPluginId.fromObject("key");

    List<Object> recorder = new ArrayList<>();
    Listener listener = new Listener(recorder);
    secondPlugin.subscribe(key, listener);

    // Verify sent objects are received
    Integer valueOne = 1;
    Integer valueTwo = 2;
    Integer valueThree = 3;
    Integer valueFour = 4;
    firstPlugin.send(key, valueOne);
    firstPlugin.send(key, valueTwo);
    firstPlugin.send(key, valueThree);
    Assert.assertArrayEquals(new Object[]{valueOne, valueTwo, valueThree},
        recorder.toArray());

    firstPlugin.send(key, valueFour);
    Assert.assertArrayEquals(new Object[]{valueOne, valueTwo,
            valueThree, valueFour}, recorder.toArray());
    secondPlugin.unsubscribe(key, listener);

    // Verify no objects are added after unsubscribe
    firstPlugin.send(key, valueOne);
    Assert.assertArrayEquals(new Object[]{valueOne, valueTwo,
        valueThree, valueFour}, recorder.toArray());


  }
}
