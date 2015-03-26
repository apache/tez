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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link UnorderedKVOutput} is a {@link LogicalOutput} that writes key
 * value data without applying any ordering or grouping constraints. This can be
 * used to write raw key value data as is.
 */
@Public
public class UnorderedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedKVOutput.class);

  @VisibleForTesting
  UnorderedPartitionedKVWriter kvWriter;
  
  private Configuration conf;
  
  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public UnorderedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }


  @Override
  public synchronized List<Event> initialize()
      throws Exception {
    this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS,
        getContext().getWorkDirs());

    this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, CustomPartitioner.class
        .getName());

    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();

    boolean pipelinedShuffle = this.conf.getBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);

    long memRequestSize = (pipelinedShuffle) ?
        UnorderedPartitionedKVWriter.getInitialMemoryRequirement(conf, getContext()
            .getTotalMemoryAvailableToTask()) : 0;
    getContext().requestInitialMemory(memRequestSize, memoryUpdateCallbackHandler);
    
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      //This would have just a single partition
      this.kvWriter = new UnorderedPartitionedKVWriter(getContext(), conf, 1,
          memoryUpdateCallbackHandler.getMemoryAssigned());
      isStarted.set(true);
      LOG.info(this.getClass().getSimpleName() + " started. MemoryAssigned="
          + memoryUpdateCallbackHandler.getMemoryAssigned());
    }
  }

  @Override
  public synchronized KeyValuesWriter getWriter() throws Exception {
    // Eventually, disallow multiple invocations.
    return kvWriter;
  }

  @Override
  public synchronized void handleEvents(List<Event> outputEvents) {
    throw new TezUncheckedException("Not expecting any events");
  }

  @Override
  public synchronized List<Event> close() throws Exception {
    if (isStarted.get()) {
      //TODO: Do we need to support sending payloads via events?
      return kvWriter.close();
    } else {
      return Collections.emptyList();
    }
  }

  @VisibleForTesting
  @Private
  String getHost() {
    return getContext().getExecutionContext().getHostName();
  }

  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
  }

  // TODO Maybe add helper methods to extract keys
  // TODO Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }

  @Private
  public static class CustomPartitioner implements Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
      return 0;
    }
  }
}
