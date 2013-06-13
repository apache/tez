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

package org.apache.tez.dag.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.yarn.api.records.ApplicationId;


/**
 * TezDAGID represents the immutable and unique identifier for
 * a Tez DAG.
 *
 * TezDAGID consists of 2 parts. The first part is the {@link ApplicationId},
 * that is the YARN Application ID that this DAG belongs to. The second part is
 * the DAG number.
 *
 * @see ApplicationId
 */
public class TezDAGID extends TezID {
  
  private ApplicationId applicationId;

  public TezDAGID() {
  }

  /**
   * Constructs a DAGID object from given {@link ApplicationId}.
   * @param applicationId Application that this dag belongs to
   * @param id the dag number
   */
  public TezDAGID(ApplicationId applicationId, int id) {
    super(id);
    if(applicationId == null) {
      throw new IllegalArgumentException("applicationId cannot be null");
    }
    this.applicationId = applicationId;
  }

  /**
   * Constructs a DAGID object from given parts.
   * @param yarnRMIdentifier YARN RM identifier
   * @param applicationId application number
   * @param id the dag number
   */
  public TezDAGID(String yarnRMIdentifier, int appId, int id) {
    this(ApplicationId.newInstance(Long.valueOf(yarnRMIdentifier),
        appId), id);
  }

  /** Returns the {@link ApplicationId} object that this dag belongs to */
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TezDAGID that = (TezDAGID)o;
    return this.applicationId.equals(that.applicationId);
  }

  /**Compare TaskInProgressIds by first jobIds, then by tip numbers and type.*/
  @Override
  public int compareTo(TezID o) {
    TezDAGID that = (TezDAGID)o;
    return this.applicationId.compareTo(that.applicationId);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    applicationId = ApplicationId.newInstance(in.readLong(), in.readInt());
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(applicationId.getClusterTimestamp());
    out.writeInt(applicationId.getId());
    super.write(out);
  }

  // DO NOT CHANGE THIS. DAGClient replicates this code to create DAG id string
  public static final String DAG = "dag";
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(DAG)).toString();
  }
  
  public static TezDAGID fromString(String dagId) {
    try {
      String[] split = dagId.split("_");
      String rmId = split[1];
      int appId = Integer.parseInt(split[2]);
      int id;
      id = idFormat.parse(split[3]).intValue();
      return new TezDAGID(rmId, appId, id);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Add the unique string to the given builder.
   * @param builder the builder to append to
   * @return the builder that was passed in
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return builder.append(SEPARATOR).
                 append(applicationId.getClusterTimestamp()).
                 append(SEPARATOR).
                 append(applicationId.getId()).
                 append(SEPARATOR).
                 append(idFormat.format(id));
  }

  @Override
  public int hashCode() {
    return applicationId.hashCode() * 524287 + id;
  }

}
