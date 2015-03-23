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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

/**
 * Implements an {@link InputInitializer} that generates Map Reduce 
 * splits in the App Master. This may utilizes the up to date cluster
 * information to create an optimal distribution of splits. This is the 
 * recommended {@link InputInitializer} to use when reading Map Reduce 
 * compatible data sources.
 */
@Public
@Evolving
public class MRInputAMSplitGenerator extends InputInitializer {

  private boolean sendSerializedEvents;
  
  private static final Logger LOG = LoggerFactory.getLogger(MRInputAMSplitGenerator.class);

  public MRInputAMSplitGenerator(
      InputInitializerContext initializerContext) {
    super(initializerContext);
  }

  @Override
  public List<Event> initialize() throws Exception {
    Stopwatch sw = null;
    if (LOG.isDebugEnabled()) {
      sw = new Stopwatch().start();
    }
    MRInputUserPayloadProto userPayloadProto = MRInputHelpers
        .parseMRInputPayload(getContext().getInputUserPayload());
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time to parse MRInput payload into prot: "
          + sw.elapsedMillis());
    }
    if (LOG.isDebugEnabled()) {
      sw.reset().start();
    }
    Configuration conf = TezUtils.createConfFromByteString(userPayloadProto
        .getConfigurationBytes());
    
    sendSerializedEvents = conf.getBoolean(
        MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD,
        MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD_DEFAULT);
    LOG.info("Emitting serialized splits: " + sendSerializedEvents);
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time converting ByteString to configuration: " + sw.elapsedMillis());
    }

    if (LOG.isDebugEnabled()) {
      sw.reset().start();
    }

    int totalResource = getContext().getTotalAvailableResource().getMemory();
    int taskResource = getContext().getVertexTaskResource().getMemory();
    float waves = conf.getFloat(
        TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES,
        TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES_DEFAULT);

    int numTasks = (int)((totalResource*waves)/taskResource);

    LOG.info("Input " + getContext().getInputName() + " asking for " + numTasks
        + " tasks. Headroom: " + totalResource + " Task Resource: "
        + taskResource + " waves: " + waves);

    // Read all credentials into the credentials instance stored in JobConf.
    JobConf jobConf = new JobConf(conf);
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());

    InputSplitInfoMem inputSplitInfo = null;
    boolean groupSplits = userPayloadProto.getGroupingEnabled();
    if (groupSplits) {
      LOG.info("Grouping input splits");
      inputSplitInfo = MRInputHelpers.generateInputSplitsToMem(jobConf, true, numTasks);
    } else {
      inputSplitInfo = MRInputHelpers.generateInputSplitsToMem(jobConf, false, 0);
    }
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time to create splits to mem: " + sw.elapsedMillis());
    }

    List<Event> events = Lists.newArrayListWithCapacity(inputSplitInfo
        .getNumTasks() + 1);
    
    InputConfigureVertexTasksEvent configureVertexEvent = InputConfigureVertexTasksEvent.create(
        inputSplitInfo.getNumTasks(),
        VertexLocationHint.create(inputSplitInfo.getTaskLocationHints()),
        InputSpecUpdate.getDefaultSinglePhysicalInputSpecUpdate());
    events.add(configureVertexEvent);

    if (sendSerializedEvents) {
      MRSplitsProto splitsProto = inputSplitInfo.getSplitsProto();
      int count = 0;
      for (MRSplitProto mrSplit : splitsProto.getSplitsList()) {
        // Unnecessary array copy, can be avoided by using ByteBuffer instead of a raw array.
        InputDataInformationEvent diEvent = InputDataInformationEvent.createWithSerializedPayload(
            count++,
            mrSplit.toByteString().asReadOnlyByteBuffer());
        events.add(diEvent);
      }
    } else {
      int count = 0;
      if (inputSplitInfo.holdsNewFormatSplits()) {
        for (org.apache.hadoop.mapreduce.InputSplit split : inputSplitInfo.getNewFormatSplits()) {
          InputDataInformationEvent diEvent = InputDataInformationEvent.createWithObjectPayload(
              count++, split);
          events.add(diEvent);
        }
      } else {
        for (org.apache.hadoop.mapred.InputSplit split : inputSplitInfo.getOldFormatSplits()) {
          InputDataInformationEvent diEvent = InputDataInformationEvent.createWithObjectPayload(
              count++, split);
          events.add(diEvent);
        }
      }
    }
    
    return events;
  }

  @Override
  public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
    throw new UnsupportedOperationException("Not expecting to handle any events");
  }

}
