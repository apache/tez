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

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.ampool.AMContext;
import org.apache.tez.ampool.manager.AMPoolManager;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

@Path("/applications")
public class ApplicationPollService {

  private static final Log LOG =
      LogFactory.getLog(ApplicationPollService.class);

  private static AMPoolManager manager;

  public static void init(AMPoolManager manager) {
    ApplicationPollService.manager = manager;
  }

  @Path("poll/{applicationAttemptId}")
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON})
  public ApplicationPollResponse pollForApplication(
      @PathParam("applicationAttemptId") String applicationAttemptIdStr,
      @Context HttpServletRequest req) {
    ApplicationAttemptId applicationAttemptId;
    LOG.info("Received a poll from launchedAM"
        + ", appAttemptId=" + applicationAttemptIdStr);
    try {
      applicationAttemptId =
          ConverterUtils.toApplicationAttemptId(applicationAttemptIdStr);
    } catch (IllegalArgumentException e) {
      LOG.warn("Received an invalid attempt id in the poll request"
          + ", appAttemptId=" + applicationAttemptIdStr);
      throw new WebApplicationException(Status.BAD_REQUEST);
    }

    AMContext amContext = null;
    try {
      amContext =
          manager.getAMContext(applicationAttemptId);
    } catch (Exception e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }

    if (amContext == null
        || amContext.getSubmissionContext() == null) {
      throw new WebApplicationException(Status.NO_CONTENT);
    }
    try {
      return convertAMContext(amContext);
    } catch (Exception e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }
  }

  private ApplicationPollResponse convertAMContext(
      AMContext amContext) throws Exception {
    ApplicationSubmissionContext context =
        amContext.getSubmissionContext();
    Map<String, LocalResource> resources =
        context.getAMContainerSpec().getLocalResources();
    ApplicationPollResponse pollResponse =
        new ApplicationPollResponse(context.getApplicationId(),
            amContext.getApplicationSubmissionTime());
    if (resources.containsKey(MRJobConfig.JOB_CONF_FILE)) {
      LocalResource rsrc = resources.get(MRJobConfig.JOB_CONF_FILE);
      pollResponse.setConfigurationFileLocation(
          ConverterUtils.getPathFromYarnURL(rsrc.getResource()).toString());
    }
    if (resources.containsKey(MRJobConfig.JOB_JAR)) {
      LocalResource rsrc = resources.get(MRJobConfig.JOB_JAR);
      pollResponse.setApplicationJarLocation(
          ConverterUtils.getPathFromYarnURL(rsrc.getResource()).toString());
    }
    return pollResponse;
  }

}
