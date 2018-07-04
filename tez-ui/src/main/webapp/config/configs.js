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

ENV = {
  hosts: {
    /*
     * Timeline Server Address:
     * By default TEZ UI looks for timeline server at http://localhost:8188, uncomment and change
     * the following value for pointing to a different address.
     */
    //timeline: "http://localhost:8188",

    /*
     * Resource Manager Address:
     * By default RM REST APIs are expected to be at http://localhost:8088, uncomment and change
     * the following value to point to a different address.
     */
    //rm: "http://localhost:8088",

    /*
     * Resource Manager Web Proxy Address:
     * Optional - By default, value configured as RM host will be taken as proxy address
     * Use this configuration when RM web proxy is configured at a different address than RM.
     */
    //rmProxy: "http://localhost:8088",
  },

  /*
   * Time Zone in which dates are displayed in the UI:
   * If not set, local time zone will be used.
   * Refer http://momentjs.com/timezone/docs/ for valid entries.
   */
  //timeZone: "UTC",

  /*
   * yarnProtocol:
   * If specified, this protocol would be used to construct node manager log links.
   * Possible values: http, https
   * Default value: If not specified, protocol of hosts.rm will be used
   */
  //yarnProtocol: "<value>",
};
