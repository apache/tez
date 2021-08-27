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

import EmberObject, { action } from '@ember/object';
import { assign } from '@ember/polyfills';

import SingleAmPollsterRoute from '../single-am-pollster';
import downloadDAGZip from '../../utils/download-dag-zip';

export default SingleAmPollsterRoute.extend({
  get title() {
    var dag = this.modelFor("dag"),
      name = dag.get("name"),
      entityID = dag.get("entityID");
    return `DAG: ${name} (${entityID})`;
  },

  loaderNamespace: "dag",

  setupController: function () {
    this._super(...arguments);
    this.startCrumbBubble();
  },

  load: function (value, query, options) {
    options = assign({
      demandNeeds: ["info"]
    }, options);
    return this.loader.queryRecord('dag', this.modelFor("dag").get("id"), options);
  },

  getCallerInfo: function (dag) {
    var dagName = dag.get("name") || "",
        callerType = dag.get("callerType"),
        callerID = dag.get("callerID");

    if(!callerID || !callerType) {
      let hiveQueryID = dagName.substr(0, dagName.indexOf(":"));
      if(hiveQueryID && dagName !== hiveQueryID) {
        callerType = "HIVE_QUERY_ID";
        callerID = hiveQueryID;
      }
    }

    return {
      type: callerType,
      id: callerID
    };
  },

  downloadDagRoute: action(function () {
    var dag = this.loadedValue,
      downloader = downloadDAGZip(dag, {
        batchSize: 500,
        timelineHost: this.get("hosts.timeline"),
        timelineNamespace: this.get("env.app.namespaces.webService.timeline"),
        callerInfo: this.getCallerInfo(dag)
      }),
      modalContent = EmberObject.create({
        dag: dag,
        downloader: downloader
      });

    this.send("openModal", "zip-download-modal", {
      title: "Download data",
      targetObject: this,
      content: modalContent
    });
  })
});
