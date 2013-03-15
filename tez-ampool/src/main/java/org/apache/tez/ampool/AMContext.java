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

package org.apache.tez.ampool;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

public class AMContext {

  private final ApplicationId applicationId;
  private ApplicationSubmissionContext submissionContext;
  private ApplicationAttemptId currentApplicationAttemptId;
  private long applicationSubmissionTime;
  private long jobPickUpTime = -1;

  public AMContext(ApplicationId applicationId) {
    this.applicationId = applicationId;
    this.submissionContext = null;
    this.currentApplicationAttemptId = null;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public synchronized ApplicationSubmissionContext getSubmissionContext() {
    return submissionContext;
  }

  public synchronized void setSubmissionContext(ApplicationSubmissionContext submissionContext) {
    this.submissionContext = submissionContext;
  }

  public synchronized ApplicationAttemptId getCurrentApplicationAttemptId() {
    return currentApplicationAttemptId;
  }

  public synchronized void setCurrentApplicationAttemptId(
      ApplicationAttemptId currentApplicationAttemptId) {
    this.currentApplicationAttemptId = currentApplicationAttemptId;
  }

  public synchronized long getApplicationSubmissionTime() {
    return applicationSubmissionTime;
  }

  public synchronized void setApplicationSubmissionTime(long applicationSubmissionTime) {
    this.applicationSubmissionTime = applicationSubmissionTime;
  }

  public synchronized long getJobPickUpTime() {
    return jobPickUpTime;
  }

  public synchronized void setJobPickUpTime(long jobPickUpTime) {
    this.jobPickUpTime = jobPickUpTime;
  }

  @Override
  public String toString() {
    return "applicationId=" + applicationId
        + ", applicationSubmissionContextIsNull="
        + (getSubmissionContext() == null)
        + ", jobPickupTime=" + jobPickUpTime
        + ", applicationSubmissionTime=" + getApplicationSubmissionTime()
        + ", applicationAttemptIdIsNull=" +
        (getCurrentApplicationAttemptId() == null);
  }

}
