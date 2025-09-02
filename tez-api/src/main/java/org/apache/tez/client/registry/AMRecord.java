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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.base.Preconditions;

/**
 * Represents an instance of an AM (DAGClientServer) in the AM registry
 */
@InterfaceAudience.Public
public class AMRecord {
  private ApplicationId appId;
  private String host;
  private int port;
  private String id;
  private final static String APP_ID_RECORD_KEY = "appId";
  private final static String HOST_RECORD_KEY = "host";
  private final static String PORT_RECORD_KEY = "port";
  private final static String OPAQUE_ID_KEY = "id";

  public AMRecord(ApplicationId appId, String host, int port, String id) {
    Preconditions.checkNotNull(appId);
    Preconditions.checkNotNull(host);
    this.appId = appId;
    this.host = host;
    this.port = port;
    //If id is not provided, convert to empty string
    this.id = (id == null) ? "" : id;
  }

  public AMRecord(AMRecord other) {
    Preconditions.checkNotNull(other);
    this.appId = other.getApplicationId();
    this.host = other.getHost();
    this.port = other.getPort();
    this.id = other.getId();
  }

  public AMRecord(ServiceRecord serviceRecord) {
    String serviceAppId = serviceRecord.get(APP_ID_RECORD_KEY);
    Preconditions.checkNotNull(serviceAppId);
    this.appId = ApplicationId.fromString(serviceAppId);
    String serviceHost = serviceRecord.get(HOST_RECORD_KEY);
    Preconditions.checkNotNull(serviceHost);
    this.host = serviceHost;
    String servicePort = serviceRecord.get(PORT_RECORD_KEY);
    this.port = Integer.parseInt(servicePort);
    String serviceId = serviceRecord.get(OPAQUE_ID_KEY);
    Preconditions.checkNotNull(serviceId);
    this.id = serviceId;
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getId() { return id; }

  @Override
  public boolean equals(Object other) {
    if(other instanceof AMRecord) {
      AMRecord otherRecord = (AMRecord) other;
      return appId.equals(otherRecord.appId)
          && host.equals(otherRecord.host)
          && port == otherRecord.port
          && id.equals(otherRecord.id);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return appId.hashCode() * host.hashCode() * id.hashCode() + port;
  }

  public ServiceRecord toServiceRecord() {
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.set(APP_ID_RECORD_KEY, appId);
    serviceRecord.set(HOST_RECORD_KEY, host);
    serviceRecord.set(PORT_RECORD_KEY, port);
    serviceRecord.set(OPAQUE_ID_KEY, id);
    return serviceRecord;
  }

}
