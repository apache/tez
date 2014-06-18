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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.lib.MRInputUtils;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.RootInputUpdatePayloadEvent;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class MRInputSplitDistributor implements TezRootInputInitializer {

  private static final Log LOG = LogFactory.getLog(MRInputSplitDistributor.class);
  
  private boolean sendSerializedEvents;

  public MRInputSplitDistributor() {
  }

  private MRSplitsProto splitsProto;

  @Override
  public List<Event> initialize(TezRootInputInitializerContext rootInputContext)
      throws IOException {
    Stopwatch sw = null;
    if (LOG.isDebugEnabled()) {
      sw = new Stopwatch().start();
    }
    MRInputUserPayloadProto userPayloadProto = MRHelpers.parseMRInputPayload(rootInputContext.getUserPayload());
    if (LOG.isDebugEnabled()) {
      sw.stop();
      LOG.debug("Time to parse MRInput payload into prot: "
          + sw.elapsedMillis());  
    }
    Configuration conf = MRHelpers.createConfFromByteString(userPayloadProto
        .getConfigurationBytes());
    JobConf jobConf = new JobConf(conf);
    boolean useNewApi = jobConf.getUseNewMapper();
    sendSerializedEvents = conf.getBoolean(
        MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD,
        MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD_DEFAULT);
    LOG.info("Emitting serialized splits: " + sendSerializedEvents);

    this.splitsProto = userPayloadProto.getSplits();
    
    MRInputUserPayloadProto.Builder updatedPayloadBuilder = MRInputUserPayloadProto.newBuilder(userPayloadProto);
    updatedPayloadBuilder.clearSplits();

    List<Event> events = Lists.newArrayListWithCapacity(this.splitsProto.getSplitsCount() + 1);
    RootInputUpdatePayloadEvent updatePayloadEvent = new RootInputUpdatePayloadEvent(
        updatedPayloadBuilder.build().toByteArray());

    events.add(updatePayloadEvent);
    int count = 0;

    for (MRSplitProto mrSplit : this.splitsProto.getSplitsList()) {

      RootInputDataInformationEvent diEvent;

      if (sendSerializedEvents) {
        // Unnecessary array copy, can be avoided by using ByteBuffer instead of
        // a raw array.
        diEvent = new RootInputDataInformationEvent(count++, mrSplit.toByteArray());
      } else {
        if (useNewApi) {
          org.apache.hadoop.mapreduce.InputSplit newInputSplit = MRInputUtils
              .getNewSplitDetailsFromEvent(mrSplit, conf);
          diEvent = new RootInputDataInformationEvent(count++, newInputSplit);
        } else {
          org.apache.hadoop.mapred.InputSplit oldInputSplit = MRInputUtils
              .getOldSplitDetailsFromEvent(mrSplit, conf);
          diEvent = new RootInputDataInformationEvent(count++, oldInputSplit);
        }
      }
      events.add(diEvent);
    }

    return events;
  }
}
