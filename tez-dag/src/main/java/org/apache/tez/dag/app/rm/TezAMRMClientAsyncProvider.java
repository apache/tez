/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import java.util.List;


public class TezAMRMClientAsyncProvider /*implements AMRMClientAsync.CallbackHandler*/ {

  private static TezAMRMClientAsync INSTANCE;
  //   private static AMRMClient INSTANCE;

  public static synchronized TezAMRMClientAsync createAMRMClientAsync(
    int intervalMs, AMRMClientAsync.CallbackHandler callbackHandler) {
    if (INSTANCE == null) {
      INSTANCE =
        TezAMRMClientAsync.createAMRMClientAsync(1000, callbackHandler);
    }
    return INSTANCE;
  }

  public static synchronized TezAMRMClientAsync getAMRMClientAsync() {
    return INSTANCE;
  }


   /* public static synchronized AMRMClient createAMRMClientAsync(){

	    if(INSTANCE == null){

		   INSTANCE= AMRMClient.createAMRMClient();

	    }

	    return INSTANCE;
    }*/



          /* @Override
            public void onContainersCompleted(List statuses) {
            }

            @Override
            public void onContainersAllocated(List containers) {
            }

            @Override
            public void onShutdownRequest() {
               // LOG.warn("Shutting down");
               // end.set(true);
	       System.out.println("onshutdownrequest");
            }

            @Override
            public void onNodesUpdated(List updatedNodes) {
            }

            @Override
            public float getProgress() {
                return 0;
	    }
            @Override
            public void onError(Throwable e) {
              //  LOG.error("Unexpected error", e);
               // end.set(true);
	       System.out.println("error");
            }*/

}
