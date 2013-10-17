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

package org.apache.tez.mapreduce.common;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class MRInputAMSplitGenerator implements TezRootInputInitializer {

  private static final Log LOG = LogFactory
      .getLog(MRInputAMSplitGenerator.class);

  public MRInputAMSplitGenerator() {
  }

  @Override
  public List<Event> initialize(TezRootInputInitializerContext rootInputContext)
      throws Exception {
    Stopwatch sw = null;
    if (LOG.isDebugEnabled()) {
      sw = new Stopwatch().start();
    }
    MRInputUserPayloadProto userPayloadProto = MRHelpers
        .parseMRInputPayload(rootInputContext.getUserPayload());
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time to parse MRInput payload into prot: "
          + sw.elapsedMillis());
    }
    if (LOG.isDebugEnabled()) {
      sw.reset().start();
    }
    Configuration conf = MRHelpers.createConfFromByteString(userPayloadProto
        .getConfigurationBytes());
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time converting ByteString to configuration: " + sw.elapsedMillis());
    }

    if (LOG.isDebugEnabled()) {
      sw.reset().start();
    }

    InputSplitInfoMem inputSplitInfo = MRHelpers.generateInputSplitsToMem(conf,
        userPayloadProto.getInputFormatName(), rootInputContext.getNumTasks());
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time to create splits to mem: " + sw.elapsedMillis());
    }

    List<Event> events = Lists.newArrayListWithCapacity(inputSplitInfo
        .getNumTasks() + 1);
    
    RootInputConfigureVertexTasksEvent configureVertexEvent = new RootInputConfigureVertexTasksEvent(
        inputSplitInfo.getNumTasks(), inputSplitInfo.getTaskLocationHints());
    events.add(configureVertexEvent);

    MRSplitsProto splitsProto = inputSplitInfo.getSplitsProto();

    int count = 0;
    for (MRSplitProto mrSplit : splitsProto.getSplitsList()) {
      // Unnecessary array copy, can be avoided by using ByteBuffer instead of a
      // raw array.
      RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(
          count++, mrSplit.toByteArray());
      events.add(diEvent);
    }
    return events;
  }

}
