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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
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
    doReturn("dagName").when(inputContext).getDAGName();
    doReturn("vertexName").when(inputContext).getTaskVertexName();
    doReturn("inputName").when(inputContext).getSourceVertexName();
    doReturn("uniqueIdentifier").when(inputContext).getUniqueIdentifier();
    doReturn(1).when(inputContext).getTaskIndex();
    doReturn(1).when(inputContext).getTaskAttemptNumber();
    doReturn(new TezCounters()).when(inputContext).getCounters();


    MRInput mrInput = new MRInput(inputContext, 0);

    mrInput.initialize();

    mrInput.start();

    assertFalse(mrInput.getReader().next());
    verify(inputContext, times(1)).notifyProgress();

    List<Event> events = new LinkedList<>();
    try {
      mrInput.handleEvents(events);
      fail("HandleEvents should cause an input with 0 physical inputs to fail");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }
  }

  private static final String TEST_ATTRIBUTES_DAG_NAME = "dagName";
  private static final String TEST_ATTRIBUTES_VERTEX_NAME = "vertexName";
  private static final String TEST_ATTRIBUTES_INPUT_NAME = "inputName";
  private static final ApplicationId TEST_ATTRIBUTES_APPLICATION_ID = ApplicationId.newInstance(0, 0);
  private static final String TEST_ATTRIBUTES_UNIQUE_IDENTIFIER = "uniqueId";
  private static final int TEST_ATTRIBUTES_DAG_INDEX = 1000;
  private static final int TEST_ATTRIBUTES_VERTEX_INDEX = 2000;
  private static final int TEST_ATTRIBUTES_TASK_INDEX = 3000;
  private static final int TEST_ATTRIBUTES_TASK_ATTEMPT_INDEX = 4000;
  private static final int TEST_ATTRIBUTES_INPUT_INDEX = 5000;
  private static final int TEST_ATTRIBUTES_DAG_ATTEMPT_NUMBER = 6000;

  @Test(timeout = 5000)
  public void testAttributesInJobConf() throws Exception {
    InputContext inputContext = mock(InputContext.class);
    doReturn(TEST_ATTRIBUTES_DAG_INDEX).when(inputContext).getDagIdentifier();
    doReturn(TEST_ATTRIBUTES_VERTEX_INDEX).when(inputContext).getTaskVertexIndex();
    doReturn(TEST_ATTRIBUTES_TASK_INDEX).when(inputContext).getTaskIndex();
    doReturn(TEST_ATTRIBUTES_TASK_ATTEMPT_INDEX).when(inputContext).getTaskAttemptNumber();
    doReturn(TEST_ATTRIBUTES_INPUT_INDEX).when(inputContext).getInputIndex();
    doReturn(TEST_ATTRIBUTES_DAG_ATTEMPT_NUMBER).when(inputContext).getDAGAttemptNumber();
    doReturn(TEST_ATTRIBUTES_DAG_NAME).when(inputContext).getDAGName();
    doReturn(TEST_ATTRIBUTES_VERTEX_NAME).when(inputContext).getTaskVertexName();
    doReturn(TEST_ATTRIBUTES_INPUT_NAME).when(inputContext).getSourceVertexName();
    doReturn(TEST_ATTRIBUTES_APPLICATION_ID).when(inputContext).getApplicationId();
    doReturn(TEST_ATTRIBUTES_UNIQUE_IDENTIFIER).when(inputContext).getUniqueIdentifier();


    DataSourceDescriptor dsd = MRInput.createConfigBuilder(new Configuration(false),
        TestInputFormat.class).groupSplits(false).build();

    doReturn(dsd.getInputDescriptor().getUserPayload()).when(inputContext).getUserPayload();
    doReturn(new TezCounters()).when(inputContext).getCounters();


    MRInput mrInput = new MRInput(inputContext, 1);
    mrInput.initialize();

    MRRuntimeProtos.MRSplitProto splitProto =
        MRRuntimeProtos.MRSplitProto.newBuilder().setSplitClassName(TestInputSplit.class.getName())
            .build();
    InputDataInformationEvent diEvent = InputDataInformationEvent
        .createWithSerializedPayload(0, splitProto.toByteString().asReadOnlyByteBuffer());

    List<Event> events = new LinkedList<>();
    events.add(diEvent);
    mrInput.handleEvents(events);
    assertTrue(TestInputFormat.invoked.get());
  }


  /**
   * Test class to verify
   */
  static class TestInputFormat implements InputFormat {

    private static final AtomicBoolean invoked = new AtomicBoolean(false);

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      return null;
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws
        IOException {
      assertEquals(TEST_ATTRIBUTES_DAG_NAME, MRInputHelpers.getDagName(job));
      assertEquals(TEST_ATTRIBUTES_VERTEX_NAME, MRInputHelpers.getVertexName(job));
      assertEquals(TEST_ATTRIBUTES_INPUT_NAME, MRInputHelpers.getInputName(job));
      assertEquals(TEST_ATTRIBUTES_DAG_INDEX, MRInputHelpers.getDagIndex(job));
      assertEquals(TEST_ATTRIBUTES_VERTEX_INDEX, MRInputHelpers.getVertexIndex(job));
      assertEquals(TEST_ATTRIBUTES_APPLICATION_ID.toString(), MRInputHelpers.getApplicationIdString(job));
      assertEquals(TEST_ATTRIBUTES_UNIQUE_IDENTIFIER, MRInputHelpers.getUniqueIdentifier(job));
      assertEquals(TEST_ATTRIBUTES_TASK_INDEX, MRInputHelpers.getTaskIndex(job));
      assertEquals(TEST_ATTRIBUTES_TASK_ATTEMPT_INDEX, MRInputHelpers.getTaskAttemptIndex(job));
      assertEquals(TEST_ATTRIBUTES_INPUT_INDEX, MRInputHelpers.getInputIndex(job));
      assertEquals(TEST_ATTRIBUTES_DAG_ATTEMPT_NUMBER, MRInputHelpers.getDagAttemptNumber(job));
      invoked.set(true);
      return new RecordReader() {
        @Override
        public boolean next(Object key, Object value) throws IOException {
          return false;
        }

        @Override
        public Object createKey() {
          return null;
        }

        @Override
        public Object createValue() {
          return null;
        }

        @Override
        public long getPos() throws IOException {
          return 0;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public float getProgress() throws IOException {
          return 0;
        }
      };
    }
  }

  public static class TestInputSplit implements InputSplit {

    @Override
    public long getLength() throws IOException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
  }
}
