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

public class EdgeProperty { // FIXME rename to ChannelProperty
  
  public enum ConnectionPattern {
    ONE_TO_ONE,
    ONE_TO_ALL,
    BIPARTITE // FIXME rename to SHUFFLE
  }
  
  public enum SourceType {
    STABLE,
    STABLE_PERSISTED,
    STREAMING
  }
  
  ConnectionPattern connectionPattern;
  SourceType sourceType;
  String inputClass;
  String outputClass;
  
  public EdgeProperty(ConnectionPattern connectionPattern, 
                       SourceType sourceType,
                       String inputClass,
                       String outputClass) {
    this.connectionPattern = connectionPattern;
    this.sourceType = sourceType;
    this.inputClass = inputClass;
    this.outputClass = outputClass;
  }
  
  // default constructor for mapreduce case
  public EdgeProperty() {
    this.connectionPattern = ConnectionPattern.BIPARTITE;
    this.sourceType = SourceType.STABLE;
    this.inputClass = "ShuffleInput.class"; //FIXME
    this.outputClass = "SortedMapOutputBuffer.class"; //FIXME
  }
  
  public ConnectionPattern getConnectionPattern() {
    return connectionPattern;
  }
  
  public SourceType getSourceType() {
    return sourceType;
  }
  
  public String getInputClass() {
    return inputClass;
  }
  
  public String getOutputClass() {
    return outputClass;
  }
  
  @Override
  public String toString() {
    return "{ " + connectionPattern + " : " 
            + inputClass + " >> " + sourceType + " >> " + outputClass + " }";
  }
  
}
