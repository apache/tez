/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.readers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputCallback;
import org.apache.tez.runtime.library.common.shuffle.LocalDiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class TestUnorderedKVReader {

  private static final Logger LOG = LoggerFactory.getLogger(TestUnorderedKVReader.class);

  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;

  private String outputFileName = "ifile.out";
  private Path outputPath;
  private long rawLen;
  private long compLen;

  private UnorderedKVReader<Text, Text> unorderedKVReader;

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    try {
      localFs = FileSystem.getLocal(defaultConf);
      workDir = new Path(
          new Path(System.getProperty("test.build.data", "/tmp")),
          TestUnorderedKVReader.class.getName())
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
      LOG.info("Using workDir: " + workDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() throws Exception {
    outputPath = new Path(workDir, outputFileName);
    setupReader();
  }

  private void setupReader() throws IOException, InterruptedException {
    defaultConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    defaultConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());

    createIFile(outputPath, 1);

    final LinkedList<LocalDiskFetchedInput> inputs = new LinkedList<LocalDiskFetchedInput>();
    LocalDiskFetchedInput realFetchedInput = new LocalDiskFetchedInput(0, compLen, new
        InputAttemptIdentifier(0, 0), outputPath, defaultConf, new FetchedInputCallback() {
      @Override
      public void fetchComplete(FetchedInput fetchedInput) {
      }

      @Override
      public void fetchFailed(FetchedInput fetchedInput) {
      }

      @Override
      public void freeResources(FetchedInput fetchedInput) {
      }
    });
    LocalDiskFetchedInput fetchedInput = spy(realFetchedInput);
    doNothing().when(fetchedInput).free();

    inputs.add(fetchedInput);

    TezCounters counters = new TezCounters();
    TezCounter inputRecords = counters.findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);

    ShuffleManager manager = mock(ShuffleManager.class);
    doAnswer(new Answer() {
      @Override public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return (inputs.isEmpty()) ? null : inputs.remove();
      }
    }).when(manager).getNextInput();

    unorderedKVReader = new UnorderedKVReader<Text, Text>(manager,
        defaultConf, null, false, -1, -1, inputRecords, mock(InputContext.class));
  }

  private void createIFile(Path path, int recordCount) throws IOException {
    FSDataOutputStream out = localFs.create(path);
    IFile.Writer writer = new IFile.Writer(new WritableSerialization(), new WritableSerialization(),
        out, Text.class, Text.class, null, null, null, true);

    for (int i = 0; i < recordCount; i++) {
      writer.append(new Text("Key_" + i), new Text("Value_" + i));
    }
    writer.close();
    rawLen = writer.getRawLength();
    compLen = writer.getCompressedLength();
    out.close();
  }

  @Before
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test(timeout = 5000)
  public void testReadingMultipleTimes() throws Exception {
    int counter = 0;
    while (unorderedKVReader.next()) {
      unorderedKVReader.getCurrentKey();
      unorderedKVReader.getCurrentKey();
      counter++;
    }
    Assert.assertEquals(1, counter);

    //Check the reader again. This shouldn't throw EOF exception in IFile
    try {
      boolean next = unorderedKVReader.next();
      fail();
    } catch(IOException ioe) {
      Assert.assertTrue(ioe.getMessage().contains("For usage, please refer to"));
    }
  }

  @Test(timeout = 5000)
  public void testInterruptOnNext() throws IOException, InterruptedException {
    ShuffleManager shuffleManager = mock(ShuffleManager.class);

    // Simulate an interrupt while waiting for the next fetched input.
    doThrow(new InterruptedException()).when(shuffleManager).getNextInput();
    TezCounters counters = new TezCounters();
    TezCounter inputRecords = counters.findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);
    UnorderedKVReader<Text, Text> reader =
        new UnorderedKVReader<Text, Text>(shuffleManager, defaultConf, null, false, -1, -1,
            inputRecords, mock(InputContext.class));

    try {
      reader.next();
      fail("No data available to reader. Should not be able to access any record");
    } catch (IOInterruptedException e) {
      // Expected exception. Any other should fail the test.
    }
  }

}
