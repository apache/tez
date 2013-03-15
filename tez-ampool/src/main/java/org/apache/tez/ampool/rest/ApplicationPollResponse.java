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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ApplicationPollResponse {

  private String applicationIdStr;

  private String configurationFileLocation;

  private String applicationJarLocation;

  private long applicationSubmissionTime;

  public ApplicationPollResponse() {
    this.applicationIdStr = null;
  }

  public ApplicationPollResponse(ApplicationId applicationId,
      long applicationSubmissionTime) {
    this.setApplicationIdStr(ConverterUtils.toString(applicationId));
    this.applicationSubmissionTime = applicationSubmissionTime;
  }

  public String getApplicationIdStr() {
    return applicationIdStr;
  }

  public void setApplicationIdStr(String applicationIdStr) {
    this.applicationIdStr = applicationIdStr;
  }

  public String getApplicationJarLocation() {
    return applicationJarLocation;
  }

  public void setApplicationJarLocation(String applicationJarLocation) {
    this.applicationJarLocation = applicationJarLocation;
  }

  public String getConfigurationFileLocation() {
    return configurationFileLocation;
  }

  public void setConfigurationFileLocation(String configurationFileLocation) {
    this.configurationFileLocation = configurationFileLocation;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("applicationId=" + applicationIdStr
        + ", configurationFileLocation=" + configurationFileLocation
        + ", applicationJarLocation=" + applicationJarLocation
        + ", applicationSubmissionTime=" + applicationSubmissionTime);
    return sb.toString();
  }

  public long getApplicationSubmissionTime() {
    return applicationSubmissionTime;
  }

  public void setApplicationSubmissionTime(long applicationSubmissionTime) {
    this.applicationSubmissionTime = applicationSubmissionTime;
  }

}
