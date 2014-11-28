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

$.extend(true, App.Configs, {

  /* Environment configurations */
  envDefaults: {
    /*
     * By default TEZ UI looks for timeline server at http://localhost:8188, uncomment and change
     * the following value for pointing to a different domain.
     */
    // timelineBaseUrl: 'http://localhost:8188',

    /*
     * By default RM web interface is expected to be at http://localhost:8088, uncomment and change
     * the following value to point to a different domain.
     */
    // RMWebUrl: 'http://localhost:8088',
  },

  /*
   * By default TEZ UI displays 10 file system counters in all tables. 'tables' object
   * gives you the option to configure more columns. Latest release(0.6.0) only supports addition
   * of counter columns.
   * Counters must be added as configuration objects into the respective array(sharedColumns, entity.dag,
   * entity.vertex etc). Configuration object must be of the following format.
   *     {
   *       counterId: '<Counter ID>',
   *       groupId: '<Group ID>',
   *       headerText: '<Display text>'
   *     },
   */
  tables: {
    /*
     * Entity specific columns must be added into the respective array.
     */
    entity: {
      dag: [],
      vertex: [],
      task: [],
      taskAttempt: [],
      tezApp: [],
    },
    /*
     * User sharedColumns to add counters that must be displayed in all tables.
     */
    sharedColumns:[]
  }

});

App.advanceReadiness();