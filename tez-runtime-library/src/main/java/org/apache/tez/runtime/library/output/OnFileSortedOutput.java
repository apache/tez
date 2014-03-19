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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.PipelinedSorter;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * <code>OnFileSortedOutput</code> is an {@link LogicalOutput} which sorts key/value pairs 
 * written to it and persists it to a file.
 */
public class OnFileSortedOutput implements LogicalOutput {

  private static final Log LOG = LogFactory.getLog(OnFileSortedOutput.class);

  protected ExternalSorter sorter;
  protected Configuration conf;
  protected int numOutputs;
  protected TezOutputContext outputContext;
  private long startTime;
  private long endTime;
  private boolean sendEmptyPartitionDetails;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  @Override
  public List<Event> initialize(TezOutputContext outputContext)
      throws IOException {
    this.startTime = System.nanoTime();
    this.outputContext = outputContext;
    this.conf = TezUtils.createConfFromUserPayload(outputContext.getUserPayload());
    // Initializing this parametr in this conf since it is used in multiple
    // places (wherever LocalDirAllocator is used) - TezTaskOutputFiles,
    // TezMerger, etc.
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS, outputContext.getWorkDirs());
    
    if (this.conf.getInt(TezJobConfig.TEZ_RUNTIME_SORT_THREADS,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SORT_THREADS) > 1) {
      sorter = new PipelinedSorter();
    } else {
      sorter = new DefaultSorter();
    }

    sendEmptyPartitionDetails = this.conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezJobConfig.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);
    sorter.initialize(outputContext, conf, numOutputs);
    return Collections.emptyList();
  }

  @Override
  public void start() throws Exception {
    if (!isStarted.getAndSet(true)) {
      sorter.start();
    }
  }

  @Override
  public KeyValueWriter getWriter() throws IOException {
    return new KeyValueWriter() {
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

   return generateEventsOnClose();
  }
  
  protected List<Event> generateEventsOnClose() throws IOException {
    String host = System.getenv(ApplicationConstants.Environment.NM_HOST
        .toString());
    ByteBuffer shuffleMetadata = outputContext
        .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    int shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);

    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();

    if (sendEmptyPartitionDetails) {
      Path indexFile = sorter.getMapOutput().getOutputIndexFile();
      TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
      //TODO: replace with BitSet in JDK 1.7 (no support for valueOf, toByteArray in 1.6)
      byte[] partitionDetails = new byte[numOutputs];
      int emptyPartitions = 0;
      for(int i=0;i<spillRecord.size();i++) {
        TezIndexRecord indexRecord = spillRecord.getIndex(i);
        if (!indexRecord.hasData()) {
          partitionDetails[i] = 1;
          emptyPartitions++;
        }
      }
      if (emptyPartitions > 0) {
        //compress the data
        ByteString.Output os = ByteString.newOutput();
        DeflaterOutputStream compressOs = new DeflaterOutputStream(os,
            new Deflater(Deflater.BEST_COMPRESSION));
        compressOs.write(partitionDetails);
        compressOs.finish();

        ShuffleUserPayloads.DataProto.Builder dataProtoBuilder = ShuffleUserPayloads.DataProto.newBuilder();
        dataProtoBuilder.setEmptyPartitions(os.toByteString());
        payloadBuilder.setData(dataProtoBuilder.build());

        LOG.info("EmptyPartition bitsetSize=" + partitionDetails.length + ", numOutputs="
                + numOutputs + ", emptyPartitions=" + emptyPartitions
              + ", compressedSize=" + os.toByteString().size());
      }
    }
    payloadBuilder.setHost(host);
    payloadBuilder.setPort(shufflePort);
    payloadBuilder.setPathComponent(outputContext.getUniqueIdentifier());
    payloadBuilder.setRunDuration((int) ((endTime - startTime) / 1000));
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();
    byte[] payloadBytes = payloadProto.toByteArray();

    long outputSize = outputContext.getCounters()
        .findCounter(TaskCounter.OUTPUT_BYTES).getValue();
    VertexManagerEventPayloadProto.Builder vmBuilder = VertexManagerEventPayloadProto
        .newBuilder();
    vmBuilder.setOutputSize(outputSize);
    VertexManagerEvent vmEvent = new VertexManagerEvent(
        outputContext.getDestinationVertexName(), vmBuilder.build().toByteArray());    

    List<Event> events = Lists.newArrayListWithCapacity(numOutputs+1);
    events.add(vmEvent);

    CompositeDataMovementEvent csdme = new CompositeDataMovementEvent(0, numOutputs, payloadBytes);
    events.add(csdme);

    return events;
  }
}
