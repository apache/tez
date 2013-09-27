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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.NamedDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataMovementType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataSourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeSchedulingType;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResource;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourceVisibility;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;

import com.google.protobuf.ByteString;


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
      TaskLocationHint outputHint = new TaskLocationHint(
          new HashSet<String>(inputHint.getHostList()),
          new HashSet<String>(inputHint.getRackList()));
      outputList.add(outputHint);
    }
    return new VertexLocationHint(outputList.size(), outputList);
  }

  // notes re HDFS URL handling:
  //   Resource URLs in the protobuf message are strings of the form hdfs://host:port/path 
  //   org.apache.hadoop.fs.Path.Path  is actually a URI type that allows any scheme
  //   org.apache.hadoop.yarn.api.records.URL is a URL type used by YARN.
  //   java.net.URL cannot be used out of the box as it rejects unknown schemes such as HDFS.
  
  public static String convertToDAGPlan(URL resource) {
    // see above notes on HDFS URL handling
    String out = resource.getScheme() + "://" + resource.getHost() + ":" + resource.getPort() 
        + resource.getFile();
    return out;
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
    return new EdgeProperty(
        convertFromDAGPlan(edge.getDataMovementType()),
        convertFromDAGPlan(edge.getDataSourceType()),
        convertFromDAGPlan(edge.getSchedulingType()),
        convertOutputDescriptorFromDAGPlan(edge.getEdgeSource()),
        convertInputDescriptorFromDAGPlan(edge.getEdgeDestination())
    );
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
      TezEntityDescriptor descriptor) {
    TezEntityDescriptorProto.Builder builder = TezEntityDescriptorProto
        .newBuilder();
    builder.setClassName(descriptor.getClassName());
    if (descriptor.getUserPayload() != null) {
      builder
          .setUserPayload(ByteString.copyFrom(descriptor.getUserPayload()));
    }
    return builder.build();
  }
  
  public static NamedDescriptorProto convertToDAGPlan(
      NamedDescriptor<? extends TezEntityDescriptor> descriptor) {
    NamedDescriptorProto.Builder builder = NamedDescriptorProto.newBuilder();
    builder.setName(descriptor.getName());
    builder.setEntityDescriptor(convertToDAGPlan(descriptor.getDescriptor()));
    return builder.build();
  }

  public static InputDescriptor convertInputDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    byte[] bb = null;
    if (proto.hasUserPayload()) {
      bb = proto.getUserPayload().toByteArray();
    }
    return new InputDescriptor(className).setUserPayload(bb);
  }

  public static OutputDescriptor convertOutputDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    byte[] bb = null;
    if (proto.hasUserPayload()) {
      bb =  proto.getUserPayload().toByteArray();
    }
    return new OutputDescriptor(className).setUserPayload(bb);
  }

  public static ProcessorDescriptor convertProcessorDescriptorFromDAGPlan(
      TezEntityDescriptorProto proto) {
    String className = proto.getClassName();
    byte[] bb = null;
    if (proto.hasUserPayload()) {
      bb = proto.getUserPayload().toByteArray();
    }
    return new ProcessorDescriptor(className).setUserPayload(bb);
  }
}
