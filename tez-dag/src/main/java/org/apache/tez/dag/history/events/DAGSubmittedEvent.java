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

package org.apache.tez.dag.history.events;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import org.apache.tez.dag.records.DAGIDAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DAGSubmittedProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;
import org.apache.tez.dag.utils.ProtoUtils;


public class DAGSubmittedEvent implements HistoryEvent, SummaryEvent, DAGIDAware {

  private static final Logger LOG = LoggerFactory.getLogger(DAGSubmittedEvent.class);

  private static final String CHARSET_NAME = "utf-8";

  private TezDAGID dagID;
  private String dagName;
  private long submitTime;
  private DAGProtos.DAGPlan dagPlan;
  private ApplicationAttemptId applicationAttemptId;
  private String user;
  private Map<String, LocalResource> cumulativeAdditionalLocalResources;
  private boolean historyLoggingEnabled = true;
  private Configuration conf;
  private String containerLogs;
  private String queueName;

  public DAGSubmittedEvent() {
  }

  public DAGSubmittedEvent(TezDAGID dagID, long submitTime,
      DAGProtos.DAGPlan dagPlan, ApplicationAttemptId applicationAttemptId,
      Map<String, LocalResource> cumulativeAdditionalLocalResources,
      String user, Configuration conf, String containerLogs, String queueName) {
    this.dagID = dagID;
    this.dagName = dagPlan.getName();
    this.submitTime = submitTime;
    this.dagPlan = dagPlan;
    this.applicationAttemptId = applicationAttemptId;
    this.cumulativeAdditionalLocalResources = cumulativeAdditionalLocalResources;
    this.user = user;
    this.conf = conf;
    this.containerLogs = containerLogs;
    this.queueName = queueName;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_SUBMITTED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public DAGSubmittedProto toProto() {
    DAGSubmittedProto.Builder builder =DAGSubmittedProto.newBuilder()
        .setDagId(dagID.toString())
        .setApplicationAttemptId(applicationAttemptId.toString())
        .setDagPlan(dagPlan)
        .setSubmitTime(submitTime);
    if (queueName != null) {
      builder.setQueueName(queueName);
    }
    if (cumulativeAdditionalLocalResources != null && !cumulativeAdditionalLocalResources.isEmpty()) {
      builder.setCumulativeAdditionalAmResources(DagTypeConverters
          .convertFromLocalResources(cumulativeAdditionalLocalResources));
    }
    return builder.build();
  }

  public void fromProto(DAGSubmittedProto proto) {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.dagPlan = proto.getDagPlan();
    this.dagName = this.dagPlan.getName();
    this.submitTime = proto.getSubmitTime();
    this.applicationAttemptId = ConverterUtils.toApplicationAttemptId(
        proto.getApplicationAttemptId());
    if (proto.hasQueueName()) {
      this.queueName = proto.getQueueName();
    }
    if (proto.hasCumulativeAdditionalAmResources()) {
      this.cumulativeAdditionalLocalResources = DagTypeConverters.convertFromPlanLocalResources(proto
          .getCumulativeAdditionalAmResources());
    }
  }

  @Override
  public void toProtoStream(CodedOutputStream outputStream) throws IOException {
    outputStream.writeMessageNoTag(toProto());
  }

  @Override
  public void fromProtoStream(CodedInputStream inputStream) throws IOException {
    DAGSubmittedProto proto = inputStream.readMessage(DAGSubmittedProto.PARSER, ExtensionRegistry.getEmptyRegistry());
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "dagID=" + dagID
        + ", submitTime=" + submitTime
        + ", queueName=" + queueName;
  }

  @Override
  public void toSummaryProtoStream(OutputStream outputStream) throws IOException {
    ProtoUtils.toSummaryEventProto(dagID, submitTime,
        HistoryEventType.DAG_SUBMITTED, dagName.getBytes(CHARSET_NAME))
        .writeDelimitedTo(outputStream);
  }

  @Override
  public void fromSummaryProtoStream(SummaryEventProto proto) throws IOException {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.submitTime = proto.getTimestamp();
    this.dagName = new String(proto.getEventPayload().toByteArray(), CHARSET_NAME);
  }

  @Override
  public boolean writeToRecoveryImmediately() {
    return true;
  }

  public String getDAGName() {
    return this.dagName;
  }

  public DAGProtos.DAGPlan getDAGPlan() {
    return this.dagPlan;
  }

  @Override
  public TezDAGID getDAGID() {
    return dagID;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public Map<String, LocalResource> getCumulativeAdditionalLocalResources() {
    return cumulativeAdditionalLocalResources;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public DAGPlan getDagPlan() {
    return dagPlan;
  }

  public String getUser() {
    return user;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setHistoryLoggingEnabled(boolean loggingEnabled) {
    historyLoggingEnabled = loggingEnabled;
  }

  public boolean isHistoryLoggingEnabled() {
    return historyLoggingEnabled;
  }

  public String getContainerLogs() {
    return containerLogs;
  }

  public String getQueueName() {
    return queueName;
  }
}
