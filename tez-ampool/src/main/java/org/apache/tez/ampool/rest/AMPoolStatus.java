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

package org.apache.tez.ampool.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.annotate.JsonProperty;

public class AMPoolStatus {

  @JsonProperty("hostname")
  private String hostname;

  @JsonProperty("pendingLaunches")
  private int pendingLaunches;

  @JsonProperty("failedLaunches")
  private int failedLaunches;

  @JsonProperty("applications")
  private Map<String, String> applications;

  @JsonProperty("unassignedApplications")
  private List<String> unassignedApplications;

  public AMPoolStatus() {
    this.applications = new TreeMap<String, String>();
    this.setUnassignedApplications(new ArrayList<String>());
  }

  /**
   * @return the applications
   */
  public Map<String, String> getApplications() {
    return applications;
  }

  /**
   * @param applications the applications to set
   */
  public void setApplications(Map<String, String> applications) {
    this.applications = applications;
  }

  /**
   * @return the pendingLaunches
   */
  public int getPendingLaunches() {
    return pendingLaunches;
  }

  /**
   * @param pendingLaunches the pendingLaunches to set
   */
  public void setPendingLaunches(int pendingLaunches) {
    this.pendingLaunches = pendingLaunches;
  }

  /**
   * @return the failedLaunches
   */
  public int getFailedLaunches() {
    return failedLaunches;
  }

  /**
   * @param failedLaunches the failedLaunches to set
   */
  public void setFailedLaunches(int failedLaunches) {
    this.failedLaunches = failedLaunches;
  }

  /**
   * @return the unassignedApplications
   */
  public List<String> getUnassignedApplications() {
    return unassignedApplications;
  }

  /**
   * @param unassignedApplications the unassignedApplications to set
   */
  public void setUnassignedApplications(List<String> unassignedApplications) {
    this.unassignedApplications = unassignedApplications;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }


}
