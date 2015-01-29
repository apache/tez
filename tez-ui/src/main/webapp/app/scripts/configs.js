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

App.setConfigs({

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
   * Visibility of table columns can be controlled using the column selector. Also an optional set of
   * file system counters can be enabled as columns for most of the tables. For adding more counters
   * as columns edit the following 'tables' object. Counters must be added as configuration objects
   * of the following format.
   *    {
   *      counterName: '<Counter ID>',
   *      counterGroupName: '<Group ID>',
   *    }
   *
   * Note: Till 0.6.0 the properties were counterId and groupId, their use is deprecated now.
   */
  tables: {
    /*
     * Entity specific columns must be added into the respective array.
     */
    entity: {
      dag: [
        // { // Following is a sample configuration object.
        //   counterName: 'FILE_BYTES_READ',
        //   counterGroupName: 'org.apache.tez.common.counters.FileSystemCounter',
        // }
      ],
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

