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

module.exports = { // Tez App configurations
  hosts: {
    timeline: 'localhost:8188',
    rm: 'localhost:8088',
  },
  namespaces: {
    webService: {
      timeline: 'ws/v1/timeline',
      history: 'ws/v1/applicationhistory',
      am: {
        v1: 'proxy/__app_id__/ws/v1/tez',
        v2: 'proxy/__app_id__/ws/v2/tez',
      },
      cluster: 'ws/v1/cluster',
    },
    web: {
      cluster: 'cluster'
    },
  },
};
