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

package org.apache.tez.mapreduce.input;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.junit.Test;

public class TestMRInput {

  @Test(timeout = 5000)
  public void test0PhysicalInputs() throws IOException {
    InputContext inputContext = mock(InputContext.class);

    DataSourceDescriptor dsd = MRInput.createConfigBuilder(new Configuration(false),
        FileInputFormat.class, "testPath").build();

    ApplicationId applicationId = ApplicationId.newInstance(1000, 1);
    doReturn(dsd.getInputDescriptor().getUserPayload()).when(inputContext).getUserPayload();
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn(1).when(inputContext).getTaskIndex();
    doReturn(1).when(inputContext).getTaskAttemptNumber();
    doReturn(new TezCounters()).when(inputContext).getCounters();


    MRInput mrInput = new MRInput(inputContext, 0);

    mrInput.initialize();

    mrInput.start();

    assertFalse(mrInput.getReader().next());

    List<Event> events = new LinkedList<Event>();
    try {
      mrInput.handleEvents(events);
      fail("HandleEvents should cause an input with 0 physical inputs to fail");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }
  }
}
