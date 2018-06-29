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
package org.apache.tez.dag.history.logging.proto;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.ManifestEntryProto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDagManifestFileScanner {
  private MockClock clock;
  private DatePartitionedLogger<ManifestEntryProto> manifestLogger;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setupTest() throws Exception {
    String basePath = tempFolder.newFolder().getAbsolutePath();
    clock = new MockClock();
    Configuration conf = new Configuration(false);
    conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR, basePath);
    // LocalFileSystem does not implement truncate.
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    TezProtoLoggers loggers = new TezProtoLoggers();
    loggers.setup(conf, clock);
    manifestLogger = loggers.getManifestEventsLogger();
  }

  @Test(timeout=5000)
  public void testNormal() throws Exception {
    clock.setTime(0); // 0th day.
    createManifestEvents(0, 8);
    clock.setTime((24 * 60 * 60 + 1) * 1000); // 1 day 1 sec.
    createManifestEvents(24 * 3600, 5);
    DagManifesFileScanner scanner = new DagManifesFileScanner(manifestLogger);
    int count = 0;
    while (scanner.getNext() != null) {
      ++count;
    }
    Assert.assertEquals(8, count);

    // Save offset for later use.
    String offset = scanner.getOffset();

    // Move time outside the window, no changes and it will give more events.
    clock.setTime((24 * 60 * 60 + 61) * 1000); // 1 day 61 sec.
    count = 0;
    while (scanner.getNext() != null) {
      ++count;
    }
    Assert.assertEquals(5, count);

    // Reset the offset
    scanner.setOffset(offset);
    count = 0;
    while (scanner.getNext() != null) {
      ++count;
    }
    Assert.assertEquals(5, count);

    scanner.close();

    // Not able to test append since the LocalFileSystem does not implement append.
  }

  private Path deleteFilePath = null;
  @Test(timeout=5000)
  public void testError() throws Exception {
    clock.setTime(0); // 0th day.
    createManifestEvents(0, 4);
    corruptFiles();
    clock.setTime((24 * 60 * 60 + 1) * 1000); // 1 day 1 sec.
    createManifestEvents(24 * 3600, 1);

    DagManifesFileScanner scanner = new DagManifesFileScanner(manifestLogger);
    Assert.assertNotNull(scanner.getNext());
    deleteFilePath.getFileSystem(manifestLogger.getConfig()).delete(deleteFilePath, false);
    // 4 files - 1 file deleted - 1 truncated - 1 corrupted => 1 remains.
    Assert.assertNull(scanner.getNext());

    // Save offset for later use.
    String offset = scanner.getOffset();

    // Move time outside the window, it should skip files with error and give more data for
    // next day.
    clock.setTime((24 * 60 * 60 + 61) * 1000); // 1 day 61 sec.
    Assert.assertNotNull(scanner.getNext());
    Assert.assertNull(scanner.getNext());

    // Reset the offset
    scanner.setOffset(offset);
    Assert.assertNotNull(scanner.getNext());
    Assert.assertNull(scanner.getNext());
    scanner.close();
  }

  private void createManifestEvents(long time, int numEvents) throws IOException {
    for (int i = 0; i < numEvents; ++i) {
      ApplicationId appId = ApplicationId.newInstance(1000l, i);
      ManifestEntryProto proto = ManifestEntryProto.newBuilder()
          .setAppId(appId.toString())
          .setDagFilePath("dummy_dag_path_" + i)
          .setDagSubmittedEventOffset(0)
          .setDagFinishedEventOffset(1)
          .setAppFilePath("dummp_app_path_" + i)
          .setAppLaunchedEventOffset(2)
          .setWriteTime(clock.getTime())
          .build();
      ProtoMessageWriter<ManifestEntryProto> writer = manifestLogger.getWriter(appId.toString());
      writer.writeProto(proto);
      writer.close();
    }
  }

  private void corruptFiles() throws IOException {
    int op = 0;
    Configuration conf = manifestLogger.getConfig();
    Path base = new Path(
        conf.get(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR) + "/dag_meta");
    FileSystem fs = base.getFileSystem(conf);
    for (FileStatus status : fs.listStatus(base)) {
      if (status.isDirectory()) {
        for (FileStatus file : fs.listStatus(status.getPath())) {
          if (!file.getPath().getName().startsWith("application_")) {
            continue;
          }
          switch (op) {
            case 0:
            case 1:
              fs.truncate(file.getPath(), op == 1 ? 0 : file.getLen() - 20);
              break;
            case 3:
              deleteFilePath = file.getPath();
              break;
          }
          op++;
        }
      }
    }
  }

  private static class MockClock implements Clock {
    private long time = 0;

    void setTime(long time) {
      this.time = time;
    }

    @Override
    public long getTime() {
      return time;
    }
  }
}
