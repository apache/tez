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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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

  private static LoadingCache<TezDAGID, TezDAGID> dagIdCache = CacheBuilder.newBuilder().softValues().
      build(
          new CacheLoader<TezDAGID, TezDAGID>() {
            @Override
            public TezDAGID load(TezDAGID key) throws Exception {
              return key;
            }
          }
      );
  
  private ApplicationId applicationId;

  /**
   * Get a DAGID object from given {@link ApplicationId}.
   * @param applicationId Application that this dag belongs to
   * @param id the dag number
   */
  public static TezDAGID getInstance(ApplicationId applicationId, int id) {
    // The newly created TezDAGIds are primarily for their hashCode method, and
    // will be short-lived.
    // Alternately the cache can be keyed by the hash of the incoming paramters.
    Preconditions.checkArgument(applicationId != null, "ApplicationID cannot be null");
    return dagIdCache.getUnchecked(new TezDAGID(applicationId, id));
  }

  @InterfaceAudience.Private
  public static void clearCache() {
    dagIdCache.invalidateAll();
    dagIdCache.cleanUp();
  }
  
  /**
   * Get a DAGID object from given parts.
   * @param yarnRMIdentifier YARN RM identifier
   * @param appId application number
   * @param id the dag number
   */
  public static TezDAGID getInstance(String yarnRMIdentifier, int appId, int id) {
    // The newly created TezDAGIds are primarily for their hashCode method, and
    // will be short-lived.
    // Alternately the cache can be keyed by the hash of the incoming paramters.
    Preconditions.checkArgument(yarnRMIdentifier != null, "yarnRMIdentifier cannot be null");
    return dagIdCache.getUnchecked(new TezDAGID(yarnRMIdentifier, appId, id));
  }
  
  // Public for Writable serialization. Verify if this is actually required.
  public TezDAGID() {
  }

  private TezDAGID(ApplicationId applicationId, int id) {
    super(id);
    this.applicationId = applicationId;
  }

  
  private TezDAGID(String yarnRMIdentifier, int appId, int id) {
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
  // Can't do much about this instance if used via the RPC layer. Any downstream
  // users can however avoid using this method.
  public void readFields(DataInput in) throws IOException {
    // ApplicationId could be cached in this case. All of this will change for Protobuf RPC.
    applicationId = ApplicationId.newInstance(in.readLong(), in.readInt());
    super.readFields(in);
  }

  public static TezDAGID readTezDAGID(DataInput in) throws IOException {
    long clusterId = in.readLong();
    int appId = in.readInt();
    int dagIdInt = TezID.readID(in);
    TezDAGID dagID = getInstance(ApplicationId.newInstance(clusterId, appId), dagIdInt);
    return dagID;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(applicationId.getClusterTimestamp());
    out.writeInt(applicationId.getId());
    super.write(out);
  }

  // DO NOT CHANGE THIS. DAGClient replicates this code to create DAG id string
  public static final String DAG = "dag";
  static final ThreadLocal<NumberFormat> tezAppIdFormat = new ThreadLocal<NumberFormat>() {
    @Override
    public NumberFormat initialValue() {
      NumberFormat fmt = NumberFormat.getInstance();
      fmt.setGroupingUsed(false);
      fmt.setMinimumIntegerDigits(4);
      return fmt;
    }
  };
  
  static final ThreadLocal<NumberFormat> tezDagIdFormat = new ThreadLocal<NumberFormat>() {
    @Override
    public NumberFormat initialValue() {
      NumberFormat fmt = NumberFormat.getInstance();
      fmt.setGroupingUsed(false);
      fmt.setMinimumIntegerDigits(1);
      return fmt;
    }
  };

  @Override
  public String toString() {
    return appendTo(new StringBuilder(DAG)).toString();
  }

  public static TezDAGID fromString(String dagId) {
    try {
      String[] split = dagId.split("_");
      String rmId = split[1];
      int appId = tezAppIdFormat.get().parse(split[2]).intValue();
      int id;
      id = tezDagIdFormat.get().parse(split[3]).intValue();
      return TezDAGID.getInstance(rmId, appId, id);
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
                 append(tezAppIdFormat.get().format(applicationId.getId())).
                 append(SEPARATOR).
                 append(tezDagIdFormat.get().format(id));
  }

  @Override
  public int hashCode() {
    return applicationId.hashCode() * 524287 + id;
  }

}
