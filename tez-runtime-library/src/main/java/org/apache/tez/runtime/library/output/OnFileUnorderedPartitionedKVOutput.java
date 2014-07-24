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

package org.apache.tez.runtime.library.output;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter;

/**
 * <code>OnFileUnorderedPartitionedKVOutput</code> is a {@link LogicalOutput} which can be used to
 * write Key-Value pairs. The key-value pairs are written to the correct partition based on the
 * configured Partitioner.
 */
public class OnFileUnorderedPartitionedKVOutput extends AbstractLogicalOutput {

  private static final Log LOG = LogFactory.getLog(OnFileUnorderedPartitionedKVOutput.class);

  private TezOutputContext outputContext;
  private Configuration conf;
  private int numPhysicalOutputs;
  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private UnorderedPartitionedKVWriter kvWriter;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  @Override
  public synchronized List<Event> initialize(TezOutputContext outputContext) throws Exception {
    this.outputContext = outputContext;
    this.conf = TezUtils.createConfFromUserPayload(outputContext.getUserPayload());
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, outputContext.getWorkDirs());
    this.conf.setInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS,
        this.numPhysicalOutputs);
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    outputContext.requestInitialMemory(
        UnorderedPartitionedKVWriter.getInitialMemoryRequirement(conf,
            outputContext.getTotalMemoryAvailableToTask()), memoryUpdateCallbackHandler);
    return Collections.emptyList();
  }

  @Override
  public List<Event> initialize() throws Exception {
    return null;
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      this.kvWriter = new UnorderedPartitionedKVWriter(outputContext, conf, numPhysicalOutputs,
          memoryUpdateCallbackHandler.getMemoryAssigned());
      isStarted.set(true);
    }
  }

  @Override
  public synchronized Writer getWriter() throws Exception {
    Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
    return kvWriter;
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
  }

  @Override
  public synchronized List<Event> close() throws Exception {
    if (isStarted.get()) {
      return kvWriter.close();
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public synchronized void setNumPhysicalOutputs(int numOutputs) {
    this.numPhysicalOutputs = numOutputs;
  }

  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_MAX_KEYS);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_GROUP_NAME_MAX_KEYS);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_NAME_MAX_KEYS);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_GROUPS_MAX_KEYS);
  }

  // TODO Maybe add helper methods to extract keys
  // TODO Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }
}