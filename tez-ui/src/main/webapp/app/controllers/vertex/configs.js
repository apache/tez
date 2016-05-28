/*global more*/
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

import Ember from 'ember';

import TableController from '../table';
import ColumnDefinition from 'em-table/utils/column-definition';

var MoreObject = more.Object;

// Better fits in more-js
function arrayfy(object) {
  var array = [];
  MoreObject.forEach(object, function (key, value) {
    array.push({
      key: key,
      value: value
    });
  });
  return array;
}

export default TableController.extend({
  searchText: "tez",

  queryParams: ["configType", "configID"],
  configType: null,
  configID: null,

  breadcrumbs: Ember.computed("configDetails", "configID", "configType", function () {
    var crumbs = [{
      text: "Configurations",
      routeName: "vertex.configs",
      queryParams: {
        configType: null,
        configID: null,
      }
    }],
    type = this.get("configType"),
    name;

    if(this.get("configType")) {
      name = this.get("configDetails.name") || this.get("configDetails.desc");
    }

    if(type && name) {
      type = type.capitalize();
      crumbs.push({
        text: `${type} [ ${name} ]`,
        routeName: "vertex.configs",
      });
    }

    return crumbs;
  }),

  setBreadcrumbs: function() {
    this._super();
    Ember.run.later(this, "send", "bubbleBreadcrumbs", []);
  },

  columns: ColumnDefinition.make([{
    id: 'configName',
    headerTitle: 'Configuration Name',
    contentPath: 'configName',
  }, {
    id: 'configValue',
    headerTitle: 'Configuration Value',
    contentPath: 'configValue',
  }]),

  normalizeConfig: function (config) {
    var userPayload = config.userPayloadAsText ? JSON.parse(config.userPayloadAsText) : {};
    return {
      id: config.name || null,
      name: config.name,
      desc: userPayload.desc,
      class: config.class || config.processorClass,
      initializer: config.initializer,
      configs: arrayfy(userPayload.config || {})
    };
  },

  configsHash: Ember.computed("model.name", "model.dag.vertices", function () {
    var vertexName = this.get("model.name"),

        inputConfigs = [],
        outputConfigs = [],
        vertexDetails;

    if(!this.get("model")) {
      return {};
    }

    vertexDetails = this.get("model.dag.vertices").findBy("vertexName", vertexName);

    (this.get("model.dag.edges") || []).forEach(function (edge) {
      if(edge.outputVertexName === vertexName) {
        let payload = edge.outputUserPayloadAsText;
        inputConfigs.push({
          id: edge.edgeId,
          desc: `From ${edge.inputVertexName}`,
          class: edge.edgeDestinationClass,
          configs: arrayfy(payload ? Ember.get(JSON.parse(payload), "config") : {})
        });
      }
      else if(edge.inputVertexName === vertexName) {
        let payload = edge.inputUserPayloadAsText;
        outputConfigs.push({
          id: edge.edgeId,
          desc: `To ${edge.outputVertexName}`,
          class: edge.edgeSourceClass,
          configs: arrayfy(payload ? Ember.get(JSON.parse(payload), "config") : {})
        });
      }
    });

    return {
      processor: this.normalizeConfig(vertexDetails),

      sources: (vertexDetails.additionalInputs || []).map(this.normalizeConfig),
      sinks: (vertexDetails.additionalOutputs || []).map(this.normalizeConfig),

      inputs: inputConfigs,
      outputs: outputConfigs
    };
  }),

  configDetails: Ember.computed("configsHash", "configType", "configID", function () {
    var configType = this.get("configType"),
        details;

    if(configType) {
      details = Ember.get(this.get("configsHash"), configType);
    }

    if(Array.isArray(details)) {
      details = details.findBy("id", this.get("configID"));
    }

    return details;
  }),

  configs: Ember.computed("configDetails", function () {
    var configs = this.get("configDetails.configs");

    if(Array.isArray(configs)) {
      return Ember.A(configs.map(function (config) {
        return Ember.Object.create({
          configName: config.key,
          configValue: config.value
        });
      }));
    }
  }),

  actions: {
    showConf: function (type, details) {
      this.setProperties({
        configType: type,
        configID: details.id
      });
      Ember.run.later(this, "send", "bubbleBreadcrumbs", []);
    }
  }

});
