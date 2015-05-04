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

package org.apache.tez.runtime.library.output;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.OutputContext;

public class OutputTestHelpers {
  static OutputContext createOutputContext(@Nullable Path workingDir) throws IOException {
    OutputContext outputContext = mock(OutputContext.class);
    Configuration conf = new TezConfiguration();
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    String workDirString = workingDir == null ? "workDir" : workingDir.toString();
    String[] workingDirs = new String[]{workDirString};
    TezCounters counters = new TezCounters();

    doReturn("destinationVertex").when(outputContext).getDestinationVertexName();
    doReturn(payLoad).when(outputContext).getUserPayload();
    doReturn(workingDirs).when(outputContext).getWorkDirs();
    doReturn(200 * 1024 * 1024l).when(outputContext).getTotalMemoryAvailableToTask();
    doReturn(counters).when(outputContext).getCounters();
    doReturn(UUID.randomUUID().toString()).when(outputContext).getUniqueIdentifier();
    return outputContext;
  }
}
