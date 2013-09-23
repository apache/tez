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
package org.apache.tez.engine.lib.output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.engine.api.KVWriter;
import org.apache.tez.engine.common.shuffle.newimpl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.engine.common.sort.impl.ExternalSorter;
import org.apache.tez.engine.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.LogicalOutput;
import org.apache.tez.engine.newapi.TezOutputContext;
import org.apache.tez.engine.newapi.events.DataMovementEvent;
import org.apache.tez.engine.shuffle.common.ShuffleUtils;

import com.google.common.collect.Lists;

/**
 * <code>OnFileSortedOutput</code> is an {@link LogicalOutput} which sorts key/value pairs 
 * written to it and persists it to a file.
 */
public class OnFileSortedOutput implements LogicalOutput {
  
  protected ExternalSorter sorter;
  protected Configuration conf;
  protected int numOutputs;
  protected TezOutputContext outputContext;
  private long startTime;
  private long endTime;
  
  
  @Override
  public List<Event> initialize(TezOutputContext outputContext)
      throws IOException {
    this.startTime = System.nanoTime();
    this.outputContext = outputContext;
    sorter = new DefaultSorter();
    this.conf = TezUtils.createConfFromUserPayload(outputContext.getUserPayload());
    // Initializing this parametr in this conf since it is used in multiple
    // places (wherever LocalDirAllocator is used) - TezTaskOutputFiles,
    // TezMerger, etc.
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS, outputContext.getWorkDirs());
    sorter.initialize(outputContext, conf, numOutputs);
    return Collections.emptyList();
  }

  @Override
  public KVWriter getWriter() throws IOException {
    return new KVWriter() {
      @Override
      public void write(Object key, Object value) throws IOException {
        sorter.write(key, value);
      }
    };
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
    // Not expecting any events.
  }

  @Override
  public void setNumPhysicalOutputs(int numOutputs) {
    this.numOutputs = numOutputs;
  }

  @Override
  public List<Event> close() throws IOException {
    sorter.flush();
    sorter.close();
    this.endTime = System.nanoTime();

    String host = System.getenv(ApplicationConstants.Environment.NM_HOST
        .toString());
    ByteBuffer shuffleMetadata = outputContext
        .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    int shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);

    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();
    payloadBuilder.setHost(host);
    payloadBuilder.setPort(shufflePort);
    payloadBuilder.setPathComponent(outputContext.getUniqueIdentifier());
    payloadBuilder.setRunDuration((int) ((endTime - startTime) / 1000));
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();
    byte[] payloadBytes = payloadProto.toByteArray();

    List<Event> events = Lists.newArrayListWithCapacity(numOutputs);

    for (int i = 0; i < numOutputs; i++) {
      DataMovementEvent event = new DataMovementEvent(i, payloadBytes);
      events.add(event);
    }
    return events;
  }
}
