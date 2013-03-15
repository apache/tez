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

package org.apache.hadoop.mapreduce.v2.app2.lazy;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app2.lazy.LazyMRAppMaster.LazyStartAppContext;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

public class LazyMRClientService extends MRClientService {

  public LazyMRClientService(AppContext appContext) {
    super(LazyMRClientService.class.getName(),
        appContext);
    super.setProtocolHandler(new LazyMRClientProtocolHandler(appContext));
  }

  class LazyMRClientProtocolHandler extends MRClientProtocolHandler {

    private LazyStartAppContext lazyStartAppContext;
    
    public LazyMRClientProtocolHandler(AppContext appContext) {
      super();
      if (appContext instanceof LazyStartAppContext) {
        this.lazyStartAppContext = (LazyStartAppContext) appContext;
      } else {
        this.lazyStartAppContext = null;
      }
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request) 
      throws YarnRemoteException {
      if (lazyStartAppContext != null
        && lazyStartAppContext.getJob(request.getJobId()) == null) {
        // create dummy job report as client will start polling before AM is 
        // fully up
        List<AMInfo> amInfos = new ArrayList<AMInfo>();
        JobReport jobReport = MRBuilderUtils.newJobReport(request.getJobId(),
            "", "", JobState.NEW,
            -1, -1, -1, 0, 0, 0, 0, "", amInfos , false, "");
        GetJobReportResponse response =
            Records.newRecord(GetJobReportResponse.class);
        response.setJobReport(jobReport);
        return response;
      }
      return super.getJobReport(request);
    }
  }

}
