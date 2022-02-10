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

import { A } from '@ember/array';
import EmberObject, { action, computed, get } from '@ember/object';
import { later } from '@ember/runloop';
import { capitalize } from '@ember/string';
import MoreObject from '../../utils/more-object';

import TableController from '../table';
import ColumnDefinition from '../../utils/column-definition';

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

  breadcrumbs: computed('configDetails.{desc,name}', 'configID', 'configType', function () {
    var crumbs = [{
      text: "Configurations",
      routeName: "vertex.configs",
      queryParams: {
        configType: null,
        configID: null,
      }
    }],
    type = this.configType,
    name;

    if(this.configType) {
      name = this.get("configDetails.name") || this.get("configDetails.desc");
    }

    if(type && name) {
      type = capitalize(type);
      crumbs.push({
        text: `${type} [ ${name} ]`,
        routeName: "vertex.configs",
      });
    }

    return crumbs;
  }),

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

  configsHash: computed('model.dag.{edges,vertices}', 'model.name', 'normalizeConfig', function () {
    var vertexName = this.get("model.name"),

        inputConfigs = [],
        outputConfigs = [],
        vertexDetails;

    if(!this.model) {
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
          configs: arrayfy(payload ? (JSON.parse(payload)).config : {})
        });
      }
      else if(edge.inputVertexName === vertexName) {
        let payload = edge.inputUserPayloadAsText;
        outputConfigs.push({
          id: edge.edgeId,
          desc: `To ${edge.outputVertexName}`,
          class: edge.edgeSourceClass,
          configs: arrayfy(payload ? (JSON.parse(payload)).config : {})
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

  configDetails: computed("configsHash", "configType", "configID", function () {
    var configType = this.configType,
        details;

    if(configType) {
      details = get(this.configsHash, configType);
    }

    if(Array.isArray(details)) {
      details = details.findBy("id", this.configID);
    }

    return details;
  }),

  configs: computed('configDetails.configs', function () {
    var configs = this.get("configDetails.configs");

    if(Array.isArray(configs)) {
      return A(configs.map(function (config) {
        return EmberObject.create({
          configName: config.key,
          configValue: config.value
        });
      }));
    }
  }),

  showConf: action(function (type, details) {
    this.setProperties({
      configType: type,
      configID: details.id
    });
  })
});
