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

package org.apache.tez.client.registry;

import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.zookeeper.ZkConfig;


/**
 * Record representing an Application Master (AM) instance within Tez.
 * <p>
 * This class can be serialized to and from a {@link ServiceRecord}, enabling
 * storage and retrieval of AM metadata in external systems. Some constructors
 * and methods are not necessarily used within the Tez codebase itself, but
 * are part of the Tez API and intended for Tez clients that manage or interact
 * with Tez unmanaged sessions.
 */
@InterfaceAudience.Public
public class AMRecord {
  private static final String APP_ID_RECORD_KEY = "appId";
  private static final String HOST_NAME_RECORD_KEY = "hostName";
  private static final String HOST_IP_RECORD_KEY = "hostIp";
  private static final String PORT_RECORD_KEY = "port";
  private static final String EXTERNAL_ID_KEY = "externalId";
  private static final String COMPUTE_GROUP_NAME_KEY = "computeName";

  private final ApplicationId appId;
  private final String hostName;
  private final String hostIp;
  private final int port;
  private final String externalId;
  private final String computeName;

  /**
   * Creates a new {@code AMRecord} with the given application ID, host, port, and identifier.
   * <p>
   * If the provided identifier is {@code null}, it will be converted to an empty string.
   * <p>
   * Although this constructor may not be used directly within Tez internals,
   * it is part of the public API for Tez clients that handle unmanaged sessions.
   *
   * @param appId the {@link ApplicationId} of the Tez application
   * @param hostName the hostname where the Application Master is running
   * @param hostIp the IP address of the Application Master host
   * @param port the RPC port number on which the Application Master is listening
   * @param externalId an optional external identifier for the record; if {@code null}, defaults to an empty string
   * @param computeName the compute group or cluster name; if {@code null}, defaults to {@link ZkConfig#DEFAULT_COMPUTE_GROUP_NAME}
   */
  public AMRecord(ApplicationId appId, String hostName, String hostIp, int port, String externalId, String computeName) {
    this.appId = appId;
    this.hostName = hostName;
    this.hostIp = hostIp;
    this.port = port;
    //externalId is optional, if not provided, convert to empty string
    this.externalId = (externalId == null) ? "" : externalId;
    this.computeName = (computeName == null) ? ZkConfig.DEFAULT_COMPUTE_GROUP_NAME : computeName;
  }

  /**
   * Copy constructor.
   * <p>
   * Creates a new {@code AMRecord} by copying the fields of another instance.
   * <p>
   * This constructor is mainly useful for client-side logic and session handling,
   * and may not be invoked directly within the Tez codebase.
   *
   * @param other the {@code AMRecord} instance to copy
   */
  public AMRecord(AMRecord other) {
    this.appId = other.getApplicationId();
    this.hostName = other.getHost();
    this.hostIp = other.getHostIp();
    this.port = other.getPort();
    this.externalId = other.getExternalId();
    this.computeName = other.getComputeName();
  }

  /**
   * Constructs a new {@code AMRecord} from a {@link ServiceRecord}.
   * <p>
   * This allows conversion from serialized metadata back into an in-memory {@code AMRecord}.
   * <p>
   * While not always used in Tez internals, it exists in the Tez API so
   * clients can reconstruct AM information when working with unmanaged sessions.
   *
   * @param serviceRecord the {@link ServiceRecord} containing AM metadata
   * @throws IllegalArgumentException if required keys are missing or invalid
   */
  public AMRecord(ServiceRecord serviceRecord) {
    this.appId = ApplicationId.fromString(serviceRecord.get(APP_ID_RECORD_KEY));
    this.hostName = serviceRecord.get(HOST_NAME_RECORD_KEY);
    this.hostIp = serviceRecord.get(HOST_IP_RECORD_KEY);
    this.port = Integer.parseInt(serviceRecord.get(PORT_RECORD_KEY));
    this.externalId = serviceRecord.get(EXTERNAL_ID_KEY);
    this.computeName = serviceRecord.get(COMPUTE_GROUP_NAME_KEY);
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public String getHost() {
    return hostName;
  }

  public String getHostName() {
    return hostName;
  }

  public String getHostIp() {
    return hostIp;
  }

  public int getPort() {
    return port;
  }

  public String getExternalId() {
    return externalId;
  }

  public String getComputeName() {
    return computeName;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof AMRecord) {
      AMRecord otherRecord = (AMRecord) other;
      return appId.equals(otherRecord.appId)
          && hostName.equals(otherRecord.hostName)
          && hostIp.equals(otherRecord.hostIp)
          && port == otherRecord.port
          && externalId.equals(otherRecord.externalId)
          && computeName.equals(otherRecord.computeName);
    } else {
      return false;
    }
  }

  /**
   * Converts this {@code AMRecord} into a {@link ServiceRecord}.
   * <p>
   * The returned {@link ServiceRecord} contains the Application Master metadata
   * (application ID, host, port, and opaque identifier) so that it can be stored
   * in an external registry or retrieved later.
   * <p>
   * While this method may not be directly used within Tez internals,
   * it is part of the Tez public API and is intended for Tez clients
   * that interact with unmanaged sessions or otherwise need to
   * persist/reconstruct Application Master information.
   *
   * @return a {@link ServiceRecord} populated with the values of this {@code AMRecord}
   */
  public ServiceRecord toServiceRecord() {
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.set(APP_ID_RECORD_KEY, appId);
    serviceRecord.set(HOST_NAME_RECORD_KEY, hostName);
    serviceRecord.set(HOST_IP_RECORD_KEY, hostIp);
    serviceRecord.set(PORT_RECORD_KEY, port);
    serviceRecord.set(EXTERNAL_ID_KEY, externalId);
    serviceRecord.set(COMPUTE_GROUP_NAME_KEY, computeName);
    return serviceRecord;
  }

  @Override
  public String toString() {
    return toServiceRecord().attributes().toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, hostName, hostIp, externalId, computeName, port);
  }
}
