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
package org.apache.tez.dag.api;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TezSessionStatusProto;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataMovementType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataSourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeSchedulingType;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResource;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourcesProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourceVisibility;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.api.records.DAGProtos.TezCounterGroupProto;
import org.apache.tez.dag.api.records.DAGProtos.TezCounterProto;
import org.apache.tez.dag.api.records.DAGProtos.TezCountersProto;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexLocationHintProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;

@Private
public class DagTypeConverters {

  public static PlanLocalResourceVisibility convertToDAGPlan(LocalResourceVisibility visibility){
    switch(visibility){
      case PUBLIC : return PlanLocalResourceVisibility.PUBLIC;
      case PRIVATE : return PlanLocalResourceVisibility.PRIVATE;
      case APPLICATION : return PlanLocalResourceVisibility.APPLICATION;
      default : throw new RuntimeException("unknown 'visibility': " + visibility);
    }
  }

  public static LocalResourceVisibility convertFromDAGPlan(PlanLocalResourceVisibility visibility){
    switch(visibility){
      case PUBLIC : return LocalResourceVisibility.PUBLIC;
      case PRIVATE : return LocalResourceVisibility.PRIVATE;
      case APPLICATION : return LocalResourceVisibility.APPLICATION;
      default : throw new RuntimeException("unknown 'visibility': " + visibility);
    }
  }

  public static PlanEdgeDataSourceType convertToDAGPlan(DataSourceType sourceType){
    switch(sourceType){
      case PERSISTED : return PlanEdgeDataSourceType.PERSISTED;
      case PERSISTED_RELIABLE : return PlanEdgeDataSourceType.PERSISTED_RELIABLE;
      case EPHEMERAL :  return PlanEdgeDataSourceType.EPHEMERAL;
      default : throw new RuntimeException("unknown 'dataSourceType': " + sourceType);
    }
  }

  public static DataSourceType convertFromDAGPlan(PlanEdgeDataSourceType sourceType){
    switch(sourceType){
      case PERSISTED : return DataSourceType.PERSISTED;
      case PERSISTED_RELIABLE : return DataSourceType.PERSISTED_RELIABLE;
      case EPHEMERAL :  return DataSourceType.EPHEMERAL;
      default : throw new RuntimeException("unknown 'dataSourceType': " + sourceType);
    }
  }

  public static PlanEdgeDataMovementType convertToDAGPlan(DataMovementType type){
    switch(type){
      case ONE_TO_ONE : return PlanEdgeDataMovementType.ONE_TO_ONE;
      case BROADCAST : return PlanEdgeDataMovementType.BROADCAST;
      case SCATTER_GATHER : return PlanEdgeDataMovementType.SCATTER_GATHER;
      case CUSTOM: return PlanEdgeDataMovementType.CUSTOM;
      default : throw new RuntimeException("unknown 'dataMovementType': " + type);
    }
  }

  public static DataMovementType convertFromDAGPlan(PlanEdgeDataMovementType type){
    switch(type){
      case ONE_TO_ONE : return DataMovementType.ONE_TO_ONE;
      case BROADCAST : return DataMovementType.BROADCAST;
      case SCATTER_GATHER : return DataMovementType.SCATTER_GATHER;
      default : throw new IllegalArgumentException("unknown 'dataMovementType': " + type);
    }
  }

  public static PlanEdgeSchedulingType convertToDAGPlan(SchedulingType type){
    switch(type){
      case SEQUENTIAL : return PlanEdgeSchedulingType.SEQUENTIAL;
      case CONCURRENT : return PlanEdgeSchedulingType.CONCURRENT;
      default : throw new RuntimeException("unknown 'SchedulingType': " + type);
    }
  }

  public static SchedulingType convertFromDAGPlan(PlanEdgeSchedulingType type){
    switch(type){
      case SEQUENTIAL : return SchedulingType.SEQUENTIAL;
      case CONCURRENT : return SchedulingType.CONCURRENT;
      default : throw new IllegalArgumentException("unknown 'SchedulingType': " + type);
    }
  }

  public static PlanLocalResourceType convertToDAGPlan(LocalResourceType type) {
    switch(type){
    case ARCHIVE : return PlanLocalResourceType.ARCHIVE;
    case FILE : return PlanLocalResourceType.FILE;
    case PATTERN : return PlanLocalResourceType.PATTERN;
    default : throw new IllegalArgumentException("unknown 'type': " + type);
    }
  }

  public static LocalResourceType convertFromDAGPlan(PlanLocalResourceType type) {
    switch(type){
    case ARCHIVE : return LocalResourceType.ARCHIVE;
    case FILE : return LocalResourceType.FILE;
    case PATTERN : return LocalResourceType.PATTERN;
    default : throw new IllegalArgumentException("unknown 'type': " + type);
    }
  }

  public static VertexLocationHint convertFromDAGPlan(
      List<PlanTaskLocationHint> locationHints) {

    List<TaskLocationHint> outputList = new ArrayList<TaskLocationHint>();

    for(PlanTaskLocationHint inputHint : locationHints){
      TaskLocationHint outputHint = TaskLocationHint.createTaskLocationHint(
          new HashSet<String>(inputHint.getHostList()),
          new HashSet<String>(inputHint.getRackList()));
      outputList.add(outputHint);
    }
    return VertexLocationHint.create(outputList);
  }
  
  // notes re HDFS URL handling:
  //   Resource URLs in the protobuf message are strings of the form hdfs://host:port/path
  //   org.apache.hadoop.fs.Path.Path  is actually a URI type that allows any scheme
  //   org.apache.hadoop.yarn.api.records.URL is a URL type used by YARN.
  //   java.net.URL cannot be used out of the box as it rejects unknown schemes such as HDFS.

  public static String convertToDAGPlan(URL resource) {
    // see above notes on HDFS URL handling
    return resource.getScheme() + "://" + resource.getHost()
        + ":" + resource.getPort() + resource.getFile();
  }

  public static Map<String, LocalResource> createLocalResourceMapFromDAGPlan(
      List<PlanLocalResource> localResourcesList) {
    Map<String, LocalResource> map = new HashMap<String, LocalResource>();
    for(PlanLocalResource res : localResourcesList){
      LocalResource r = new LocalResourcePBImpl();

      //NOTE: have to check every optional field in protobuf generated classes for existence before accessing
      //else we will receive a default value back, eg ""
      if(res.hasPattern()){
        r.setPattern(res.getPattern());
      }
      r.setResource(ConverterUtils.getYarnUrlFromPath(new Path(res.getUri())));  // see above notes on HDFS URL handling
      r.setSize(res.getSize());
      r.setTimestamp(res.getTimeStamp());
      r.setType(DagTypeConverters.convertFromDAGPlan(res.getType()));
      r.setVisibility(DagTypeConverters.convertFromDAGPlan(res.getVisibility()));
      map.put(res.getName(), r);
    }
    return map;
  }

  public static Map<String, String> createEnvironmentMapFromDAGPlan(
      List<PlanKeyValuePair> environmentSettingList) {

    Map<String, String> map = new HashMap<String, String>();
    for(PlanKeyValuePair setting : environmentSettingList){
      map.put(setting.getKey(), setting.getValue());
    }

    return map;
  }

  public static Map<String, EdgePlan> createEdgePlanMapFromDAGPlan(List<EdgePlan> edgeList){
    Map<String, EdgePlan> edgePlanMap =
        new HashMap<String, EdgePlan>();
    for(EdgePlan edgePlanItem : edgeList){
      edgePlanMap.put(edgePlanItem.getId(), edgePlanItem);
    }
    return edgePlanMap;
  }

  public static EdgeProperty createEdgePropertyMapFromDAGPlan(EdgePlan edge) {
    if (edge.getDataMovementType() == PlanEdgeDataMovementType.CUSTOM) {
      return EdgeProperty.create(
          (edge.hasEdgeManager() ?
              convertEdgeManagerPluginDescriptorFromDAGPlan(edge.getEdgeManager()) : null),
          convertFromDAGPlan(edge.getDataSourceType()),
          convertFromDAGPlan(edge.getSchedulingType()),
          convertOutputDescriptorFromDAGPlan(edge.getEdgeSource()),
          convertInputDescriptorFromDAGPlan(edge.getEdgeDestination())
      );
    } else {
      return EdgeProperty.create(
          convertFromDAGPlan(edge.getDataMovementType()),
          convertFromDAGPlan(edge.getDataSourceType()),
          convertFromDAGPlan(edge.getSchedulingType()),
          convertOutputDescriptorFromDAGPlan(edge.getEdgeSource()),
          convertInputDescriptorFromDAGPlan(edge.getEdgeDestination())
      );
    }
  }

  public static Resource createResourceRequestFromTaskConfig(
      PlanTaskConfiguration taskConfig) {
    return Resource.newInstance(taskConfig.getMemoryMb(), taskConfig.getVirtualCores());
  }

  public static Map<String, String> convertConfFromProto(
      ConfigurationProto confProto) {
    List<PlanKeyValuePair> settingList = confProto.getConfKeyValuesList();
    Map<String, String> map = new HashMap<String, String>();
    for(PlanKeyValuePair setting: settingList){
      map.put(setting.getKey(), setting.getValue());
    }
    return map;
  }

  public static TezEntityDescriptorProto convertToDAGPlan(
      EntityDescriptor<?> descriptor) {
    TezEntityDescriptorProto.Builder builder = TezEntityDescriptorProto
        .newBuilder();
    builder.setClassName(descriptor.getClassName());

    UserPayload userPayload = descriptor.getUserPayload();
    if (userPayload != null && userPayload.hasPayload()) {
      builder.setUserPayload(ByteString.copyFrom(descriptor.getUserPayload().getPayload()));
      builder.setVersion(userPayload.getVersion());
    }
    if (descriptor.getHistoryText() != null) {
      try {
        builder.setHistoryText(TezCommonUtils.compressByteArrayToByteString(
            descriptor.getHistoryText().getBytes()));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
    return builder.build();
  }

  public static String getHistoryTextFromProto(TezEntityDescriptorProto proto) {
    if (!proto.hasHistoryText()) {
      return null;
    }
    try {
      return new String(TezCommonUtils.decompressByteStringToByteArray(proto.getHistoryText()));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }



  public static RootInputLeafOutputProto convertToDAGPlan(
      RootInputLeafOutput<? extends EntityDescriptor<?>, ? extends EntityDescriptor<?>> rootIO) {
    RootInputLeafOutputProto.Builder builder = RootInputLeafOutputProto.newBuilder();
    builder.setName(rootIO.getName());
    builder.setIODescriptor(convertToDAGPlan(rootIO.getIODescriptor()));
    if (rootIO.getControllerDescriptor() != null) {
      builder.setControllerDescriptor(convertToDAGPlan(rootIO.getControllerDescriptor()));
    }
    return builder.build();
  }

  private static UserPayload convertTezUserPayloadFromDAGPlan(
      TezEntityDescriptorProto proto) {
    UserPayload userPayload = null;
    if (proto.hasUserPayload()) {
      userPayload =
          UserPayload.create(proto.getUserPayload().asReadOnlyByteBuffer(), proto.getVersion());
    } else {
      userPayload = UserPayload.create(null, -1);
    }
    return userPayload;
  }

  public static InputDescriptor convertInputDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertTezUserPayloadFromDAGPlan(proto);
    return InputDescriptor.create(className).setUserPayload(payload);
  }

  public static OutputDescriptor convertOutputDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertTezUserPayloadFromDAGPlan(proto);
    return OutputDescriptor.create(className).setUserPayload(payload);
  }

  public static InputInitializerDescriptor convertInputInitializerDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertTezUserPayloadFromDAGPlan(proto);
    return InputInitializerDescriptor.create(className).setUserPayload(payload);
  }

  public static OutputCommitterDescriptor convertOutputCommitterDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertTezUserPayloadFromDAGPlan(proto);
    return OutputCommitterDescriptor.create(className).setUserPayload(payload);
  }

  public static VertexManagerPluginDescriptor convertVertexManagerPluginDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertTezUserPayloadFromDAGPlan(proto);
    return VertexManagerPluginDescriptor.create(className).setUserPayload(payload);
  }

  public static EdgeManagerPluginDescriptor convertEdgeManagerPluginDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertTezUserPayloadFromDAGPlan(proto);
    return EdgeManagerPluginDescriptor.create(className).setUserPayload(payload);
  }

  public static ProcessorDescriptor convertProcessorDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertTezUserPayloadFromDAGPlan(proto);
    return ProcessorDescriptor.create(className).setUserPayload(payload);
  }

  public static TezAppMasterStatus convertTezSessionStatusFromProto(
      TezSessionStatusProto proto) {
    switch (proto) {
    case INITIALIZING:
      return TezAppMasterStatus.INITIALIZING;
    case READY:
      return TezAppMasterStatus.READY;
    case RUNNING:
      return TezAppMasterStatus.RUNNING;
    case SHUTDOWN:
      return TezAppMasterStatus.SHUTDOWN;
    }
    throw new TezUncheckedException("Could not convert to TezSessionStatus from"
        + " proto");
  }

  public static TezSessionStatusProto convertTezSessionStatusToProto(
      TezAppMasterStatus status) {
    switch (status) {
    case INITIALIZING:
      return TezSessionStatusProto.INITIALIZING;
    case READY:
      return TezSessionStatusProto.READY;
    case RUNNING:
      return TezSessionStatusProto.RUNNING;
    case SHUTDOWN:
      return TezSessionStatusProto.SHUTDOWN;
    }
    throw new TezUncheckedException("Could not convert TezSessionStatus to"
        + " proto");
  }


  public static PlanLocalResourcesProto convertFromLocalResources(
    Map<String, LocalResource> localResources) {
    PlanLocalResourcesProto.Builder builder =
      PlanLocalResourcesProto.newBuilder();
    for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {
      PlanLocalResource plr = convertLocalResourceToPlanLocalResource(
        entry.getKey(), entry.getValue());
      builder.addLocalResources(plr);
    }
    return builder.build();
  }

  public static Map<String, LocalResource> convertFromPlanLocalResources(
    PlanLocalResourcesProto proto) {
    Map<String, LocalResource> localResources =
      new HashMap<String, LocalResource>(proto.getLocalResourcesCount());
    for (PlanLocalResource plr : proto.getLocalResourcesList()) {
      String name = plr.getName();
      LocalResource lr = convertPlanLocalResourceToLocalResource(plr);
      localResources.put(name, lr);
    }
    return localResources;
  }

  public static PlanLocalResource convertLocalResourceToPlanLocalResource(
    String name, LocalResource lr) {
    PlanLocalResource.Builder localResourcesBuilder = PlanLocalResource.newBuilder();
    localResourcesBuilder.setName(name);
    localResourcesBuilder.setUri(
      DagTypeConverters.convertToDAGPlan(lr.getResource()));
    localResourcesBuilder.setSize(lr.getSize());
    localResourcesBuilder.setTimeStamp(lr.getTimestamp());
    localResourcesBuilder.setType(
      DagTypeConverters.convertToDAGPlan(lr.getType()));
    localResourcesBuilder.setVisibility(
      DagTypeConverters.convertToDAGPlan(lr.getVisibility()));
    if (lr.getType() == LocalResourceType.PATTERN) {
      if (lr.getPattern() == null || lr.getPattern().isEmpty()) {
        throw new TezUncheckedException("LocalResource type set to pattern"
          + " but pattern is null or empty");
      }
      localResourcesBuilder.setPattern(lr.getPattern());
    }
    return localResourcesBuilder.build();
  }

  public static LocalResource convertPlanLocalResourceToLocalResource(
      PlanLocalResource plr) {
    return LocalResource.newInstance(
      ConverterUtils.getYarnUrlFromPath(new Path(plr.getUri())),
      DagTypeConverters.convertFromDAGPlan(plr.getType()),
      DagTypeConverters.convertFromDAGPlan(plr.getVisibility()),
      plr.getSize(), plr.getTimeStamp(),
      plr.hasPattern() ? plr.getPattern() : null);
  }

  public static TezCounters convertTezCountersFromProto(TezCountersProto proto) {
    TezCounters counters = new TezCounters();
    for (TezCounterGroupProto counterGroupProto : proto.getCounterGroupsList()) {
      CounterGroup group = counters.addGroup(counterGroupProto.getName(),
        counterGroupProto.getDisplayName());
      for (TezCounterProto counterProto :
        counterGroupProto.getCountersList()) {
        TezCounter counter = group.findCounter(
          counterProto.getName(),
          counterProto.getDisplayName());
        counter.setValue(counterProto.getValue());
      }
    }
    return counters;
  }

  public static TezCountersProto convertTezCountersToProto(
      TezCounters counters) {
    TezCountersProto.Builder builder = TezCountersProto.newBuilder();
    Iterator<CounterGroup> groupIterator = counters.iterator();
    int groupIndex = 0;
    while (groupIterator.hasNext()) {
      CounterGroup counterGroup = groupIterator.next();
      TezCounterGroupProto.Builder groupBuilder =
        TezCounterGroupProto.newBuilder();
      groupBuilder.setName(counterGroup.getName());
      groupBuilder.setDisplayName(counterGroup.getDisplayName());
      Iterator<TezCounter> counterIterator = counterGroup.iterator();
      int counterIndex = 0;
      while (counterIterator.hasNext()) {
        TezCounter counter = counterIterator.next();
        TezCounterProto tezCounterProto = TezCounterProto.newBuilder()
          .setName(counter.getName())
          .setDisplayName(counter.getDisplayName())
          .setValue(counter.getValue())
          .build();
        groupBuilder.addCounters(counterIndex, tezCounterProto);
        ++counterIndex;
      }
      builder.addCounterGroups(groupIndex, groupBuilder.build());
      ++groupIndex;
    }
    return builder.build();
  }

  public static DAGProtos.StatusGetOptsProto convertStatusGetOptsToProto(
    StatusGetOpts statusGetOpts) {
    switch (statusGetOpts) {
      case GET_COUNTERS:
        return DAGProtos.StatusGetOptsProto.GET_COUNTERS;
    }
    throw new TezUncheckedException("Could not convert StatusGetOpts to"
      + " proto");
  }

  public static StatusGetOpts convertStatusGetOptsFromProto(
    DAGProtos.StatusGetOptsProto proto) {
    switch (proto) {
      case GET_COUNTERS:
        return StatusGetOpts.GET_COUNTERS;
    }
    throw new TezUncheckedException("Could not convert to StatusGetOpts from"
      + " proto");
  }

  public static List<DAGProtos.StatusGetOptsProto> convertStatusGetOptsToProto(
    Set<StatusGetOpts> statusGetOpts) {
    List<DAGProtos.StatusGetOptsProto> protos =
      new ArrayList<DAGProtos.StatusGetOptsProto>(statusGetOpts.size());
    for (StatusGetOpts opt : statusGetOpts) {
      protos.add(convertStatusGetOptsToProto(opt));
    }
    return protos;
  }

  public static Set<StatusGetOpts> convertStatusGetOptsFromProto(
      List<DAGProtos.StatusGetOptsProto> protoList) {
    Set<StatusGetOpts> opts = new TreeSet<StatusGetOpts>();
    for (DAGProtos.StatusGetOptsProto proto : protoList) {
      opts.add(convertStatusGetOptsFromProto(proto));
    }
    return opts;
  }

  public static ByteString convertCredentialsToProto(Credentials credentials) {
    if (credentials == null) {
      return null;
    }
    Output output = ByteString.newOutput();
    DataOutputStream dos = new DataOutputStream(output);
    try {
      credentials.writeTokenStorageToStream(dos);
      return output.toByteString();
    } catch (IOException e) {
      throw new TezUncheckedException("Failed to serialize Credentials", e);
    }
  }

  public static Credentials convertByteStringToCredentials(ByteString byteString) {
    if (byteString == null) {
      return null;
    }
    DataInputByteBuffer dib = new DataInputByteBuffer();
    dib.reset(byteString.asReadOnlyByteBuffer());
    Credentials credentials = new Credentials();
    try {
      credentials.readTokenStorageStream(dib);
      return credentials;
    } catch (IOException e) {
      throw new TezUncheckedException("Failed to deserialize Credentials", e);
    }
  }

  public static VertexLocationHint convertVertexLocationHintFromProto(
    VertexLocationHintProto proto) {
    List<TaskLocationHint> outputList = new ArrayList<TaskLocationHint>(
      proto.getTaskLocationHintsCount());
    for(PlanTaskLocationHint inputHint : proto.getTaskLocationHintsList()){
      TaskLocationHint outputHint = TaskLocationHint.createTaskLocationHint(
          new HashSet<String>(inputHint.getHostList()),
          new HashSet<String>(inputHint.getRackList()));
      outputList.add(outputHint);
    }

    return VertexLocationHint.create(outputList);
  }

  public static VertexLocationHintProto convertVertexLocationHintToProto(
      VertexLocationHint vertexLocationHint) {
    VertexLocationHintProto.Builder builder =
      VertexLocationHintProto.newBuilder();
    if (vertexLocationHint.getTaskLocationHints() != null) {
      for (TaskLocationHint taskLocationHint :
        vertexLocationHint.getTaskLocationHints()) {
        PlanTaskLocationHint.Builder taskLHBuilder =
          PlanTaskLocationHint.newBuilder();
        if (taskLocationHint.getHosts() != null) {
          taskLHBuilder.addAllHost(taskLocationHint.getHosts());
        }
        if (taskLocationHint.getRacks() != null) {
          taskLHBuilder.addAllRack(taskLocationHint.getRacks());
        }
        builder.addTaskLocationHints(taskLHBuilder.build());
      }
    }
    return builder.build();
  }

  public static UserPayload convertToTezUserPayload(@Nullable ByteBuffer payload, int version) {
    return UserPayload.create(payload, version);
  }

  @Nullable
  public static ByteBuffer convertFromTezUserPayload(@Nullable UserPayload payload) {
    if (payload == null) {
      return null;
    }
    return payload.getPayload();
  }

}
