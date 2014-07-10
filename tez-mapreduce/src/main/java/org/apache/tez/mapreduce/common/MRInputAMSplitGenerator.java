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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.RootInputSpecUpdate;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.tez.runtime.api.events.RootInputInitializerEvent;

public class MRInputAMSplitGenerator implements TezRootInputInitializer {

  private boolean sendSerializedEvents;
  
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

    int totalResource = rootInputContext.getTotalAvailableResource().getMemory();
    int taskResource = rootInputContext.getVertexTaskResource().getMemory();
    float waves = conf.getFloat(
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES,
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES_DEFAULT);

    int numTasks = (int)((totalResource*waves)/taskResource);

    LOG.info("Input " + rootInputContext.getInputName() + " asking for " + numTasks
        + " tasks. Headroom: " + totalResource + " Task Resource: "
        + taskResource + " waves: " + waves);

    // Read all credentials into the credentials instance stored in JobConf.
    JobConf jobConf = new JobConf(conf);
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());

    InputSplitInfoMem inputSplitInfo = null;
    String realInputFormatName = userPayloadProto.getInputFormatName(); 
    if ( realInputFormatName != null && !realInputFormatName.isEmpty()) {
      // split grouping on the AM
      if (jobConf.getUseNewMapper()) {
        LOG.info("Grouping mapreduce api input splits");
        Job job = Job.getInstance(jobConf);
        org.apache.hadoop.mapreduce.InputSplit[] splits = MRHelpers
            .generateNewSplits(job, realInputFormatName, numTasks);

        // Move all this into a function
        List<TaskLocationHint> locationHints = Lists
            .newArrayListWithCapacity(splits.length);
        for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
          String rack = 
              ((org.apache.hadoop.mapreduce.split.TezGroupedSplit) split).getRack();
          if (rack == null) {
            if (split.getLocations() != null) {
              locationHints.add(new TaskLocationHint(new HashSet<String>(Arrays
                  .asList(split.getLocations())), null));
            } else {
              locationHints.add(new TaskLocationHint(null, null));
            }
          } else {
            locationHints.add(new TaskLocationHint(null, 
                Collections.singleton(rack)));
          }
        }
        inputSplitInfo = new InputSplitInfoMem(splits, locationHints, splits.length, null, jobConf);
      } else {
        LOG.info("Grouping mapred api input splits");
        org.apache.hadoop.mapred.InputSplit[] splits = MRHelpers
            .generateOldSplits(jobConf, realInputFormatName, numTasks);
        List<TaskLocationHint> locationHints = Lists
            .newArrayListWithCapacity(splits.length);
        for (org.apache.hadoop.mapred.InputSplit split : splits) {
          String rack = 
              ((org.apache.hadoop.mapred.split.TezGroupedSplit) split).getRack();
          if (rack == null) {
            if (split.getLocations() != null) {
              locationHints.add(new TaskLocationHint(new HashSet<String>(Arrays
                  .asList(split.getLocations())), null));
            } else {
              locationHints.add(new TaskLocationHint(null, null));
            }
          } else {
            locationHints.add(new TaskLocationHint(null, 
                Collections.singleton(rack)));
          }
        }
        inputSplitInfo = new InputSplitInfoMem(splits, locationHints, splits.length, null, jobConf);
      }
    } else {
      inputSplitInfo = MRHelpers.generateInputSplitsToMem(jobConf);
    }
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time to create splits to mem: " + sw.elapsedMillis());
    }

    List<Event> events = Lists.newArrayListWithCapacity(inputSplitInfo
        .getNumTasks() + 1);
    
    RootInputConfigureVertexTasksEvent configureVertexEvent = new RootInputConfigureVertexTasksEvent(
        inputSplitInfo.getNumTasks(), inputSplitInfo.getTaskLocationHints(),
        RootInputSpecUpdate.getDefaultSinglePhysicalInputSpecUpdate());
    events.add(configureVertexEvent);

    if (sendSerializedEvents) {
      MRSplitsProto splitsProto = inputSplitInfo.getSplitsProto();
      int count = 0;
      for (MRSplitProto mrSplit : splitsProto.getSplitsList()) {
        // Unnecessary array copy, can be avoided by using ByteBuffer instead of a raw array.
        RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(count++,
            mrSplit.toByteArray());
        events.add(diEvent);
      }
    } else {
      int count = 0;
      if (inputSplitInfo.holdsNewFormatSplits()) {
        for (org.apache.hadoop.mapreduce.InputSplit split : inputSplitInfo.getNewFormatSplits()) {
          RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(count++, split);
          events.add(diEvent);
        }
      } else {
        for (org.apache.hadoop.mapred.InputSplit split : inputSplitInfo.getOldFormatSplits()) {
          RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(count++, split);
          events.add(diEvent);
        }
      }
    }
    
    return events;
  }

  @Override
  public void handleInputInitializerEvent(List<RootInputInitializerEvent> events) throws Exception {
    throw new UnsupportedOperationException("Not expecting to handle any events");
  }

}
