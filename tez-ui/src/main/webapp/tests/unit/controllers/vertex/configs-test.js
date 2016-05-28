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

import { moduleFor, test } from 'ember-qunit';

moduleFor('controller:vertex/configs', 'Unit | Controller | vertex/configs', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  });

  assert.ok(controller);

  assert.ok(controller.breadcrumbs);
  assert.ok(controller.setBreadcrumbs);

  assert.ok(controller.columns);
  assert.equal(controller.columns.length, 2);

  assert.ok(controller.normalizeConfig);
  assert.ok(controller.configsHash);
  assert.ok(controller.configDetails);
  assert.ok(controller.configs);

  assert.ok(controller.actions.showConf);

  assert.equal(controller.searchText, "tez");
  assert.notEqual(controller.queryParams.indexOf("configType"), -1);
  assert.notEqual(controller.queryParams.indexOf("configID"), -1);
});

test('Breadcrumbs test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K,
    configDetails: {
      name: "name"
    }
  });

  assert.equal(controller.get("breadcrumbs").length, 1);
  assert.equal(controller.get("breadcrumbs")[0].text, "Configurations");
  assert.equal(controller.get("breadcrumbs")[0].queryParams.configType, null);
  assert.equal(controller.get("breadcrumbs")[0].queryParams.configID, null);

  controller.setProperties({
    configType: "TestType",
    configID: "ID",
  });
  assert.equal(controller.get("breadcrumbs").length, 2);
  assert.equal(controller.get("breadcrumbs")[1].text, "TestType [ name ]");
});

test('normalizeConfig test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K,
  }),
  testName = "name",
  testClass = "TestClass",
  testInit = "TestInit",
  payload = {
    desc: 'abc',
    config: {
      x:1,
      y:2
    }
  },
  config;

  // Processor
  config = controller.normalizeConfig({
    processorClass: testClass,
    userPayloadAsText: JSON.stringify(payload)
  });
  assert.equal(config.id, null);
  assert.equal(config.class, testClass);
  assert.equal(config.desc, payload.desc);
  assert.deepEqual(config.configs, [{key: "x", value: 1}, {key: "y", value: 2}]);

  // Inputs & outputs
  config = controller.normalizeConfig({
    name: testName,
    class: testClass,
    initializer: testInit,
    userPayloadAsText: JSON.stringify(payload)
  });
  assert.equal(config.id, testName);
  assert.equal(config.class, testClass);
  assert.equal(config.initializer, testInit);
  assert.equal(config.desc, payload.desc);
  assert.deepEqual(config.configs, [{key: "x", value: 1}, {key: "y", value: 2}]);
});

test('configsHash test', function(assert) {
  let controller = this.subject({
        send: Ember.K,
        initVisibleColumns: Ember.K,
      });

  assert.deepEqual(controller.get("configsHash"), {});

  controller.set("model", {
    dag: {
      vertices: [
        {
          "vertexName": "v1",
          "processorClass": "org.apache.tez.mapreduce.processor.map.MapProcessor",
          "userPayloadAsText": "{\"desc\":\"Tokenizer Vertex\",\"config\":{\"config.key\":\"11\"}}",
          "additionalInputs": [
            {
              "name": "MRInput",
              "class": "org.apache.tez.mapreduce.input.MRInputLegacy",
              "initializer": "org.apache.tez.mapreduce.common.MRInputAMSplitGenerator",
              "userPayloadAsText": "{\"desc\":\"HDFS Input\",\"config\":{\"config.key\":\"22\"}}"
            }
          ]
        },
        {
          "vertexName": "v2",
          "processorClass": "org.apache.tez.mapreduce.processor.reduce.ReduceProcessor",
          "userPayloadAsText": "{\"desc\":\"Summation Vertex\",\"config\":{\"config.key\":\"33\"}}"
        },
        {
          "vertexName": "v3",
          "processorClass": "org.apache.tez.mapreduce.processor.reduce.ReduceProcessor",
          "userPayloadAsText": "{\"desc\":\"Sorter Vertex\",\"config\":{\"config.key1\":\"44\", \"config.key2\":\"444\"}}",
          "additionalOutputs": [
            {
              "name": "MROutput",
              "class": "org.apache.tez.mapreduce.output.MROutputLegacy",
              "initializer": "org.apache.tez.mapreduce.committer.MROutputCommitter",
              "userPayloadAsText": "{\"desc\":\"HDFS Output\",\"config\":{\"config.key\":\"55\"}}"
            }
          ]
        }
      ],
      edges: [
        {
          "edgeId": "edg1",
          "inputVertexName": "v2",
          "outputVertexName": "v3",
          "edgeSourceClass": "org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput",
          "edgeDestinationClass": "org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy",
          "outputUserPayloadAsText": "{\"config\":{\"config.key\":\"66\"}}",
          "inputUserPayloadAsText": "{\"config\":{\"config.key\":\"77\"}}",
        },
        {
          "edgeId": "edg2",
          "inputVertexName": "v1",
          "outputVertexName": "v2",
          "edgeSourceClass": "org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput",
          "edgeDestinationClass": "org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy",
          "outputUserPayloadAsText": "{\"config\":{\"config.key\":\"88\"}}",
          "inputUserPayloadAsText": "{\"config\":{\"config.key\":\"99\"}}",
        }
      ]
    }
  });

  // Test for vertex v1
  controller.set("model.name", "v1");

  assert.ok(controller.get("configsHash.processor"));
  assert.equal(controller.get("configsHash.processor.name"), null);
  assert.equal(controller.get("configsHash.processor.desc"), "Tokenizer Vertex");
  assert.equal(controller.get("configsHash.processor.class"), "org.apache.tez.mapreduce.processor.map.MapProcessor");
  assert.equal(controller.get("configsHash.processor.configs.length"), 1);
  assert.equal(controller.get("configsHash.processor.configs.0.key"), "config.key");
  assert.equal(controller.get("configsHash.processor.configs.0.value"), 11);

  assert.ok(controller.get("configsHash.sources"));
  assert.equal(controller.get("configsHash.sources.length"), 1);
  assert.equal(controller.get("configsHash.sources.0.name"), "MRInput");
  assert.equal(controller.get("configsHash.sources.0.desc"), "HDFS Input");
  assert.equal(controller.get("configsHash.sources.0.class"), "org.apache.tez.mapreduce.input.MRInputLegacy");
  assert.equal(controller.get("configsHash.sources.0.initializer"), "org.apache.tez.mapreduce.common.MRInputAMSplitGenerator");
  assert.equal(controller.get("configsHash.sources.0.configs.length"), 1);
  assert.equal(controller.get("configsHash.sources.0.configs.0.key"), "config.key");
  assert.equal(controller.get("configsHash.sources.0.configs.0.value"), 22);

  assert.ok(controller.get("configsHash.sinks"));
  assert.equal(controller.get("configsHash.sinks.length"), 0);

  assert.ok(controller.get("configsHash.inputs"));
  assert.equal(controller.get("configsHash.inputs.length"), 0);

  assert.ok(controller.get("configsHash.outputs"));
  assert.equal(controller.get("configsHash.outputs.length"), 1);
  assert.equal(controller.get("configsHash.outputs.0.name"), null);
  assert.equal(controller.get("configsHash.outputs.0.desc"), "To v2");
  assert.equal(controller.get("configsHash.outputs.0.class"), "org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput");
  assert.equal(controller.get("configsHash.outputs.0.configs.length"), 1);
  assert.equal(controller.get("configsHash.outputs.0.configs.0.key"), "config.key");
  assert.equal(controller.get("configsHash.outputs.0.configs.0.value"), 99);

  // Test for vertex v3
  controller.set("model.name", "v3");

  assert.ok(controller.get("configsHash.processor"));
  assert.equal(controller.get("configsHash.processor.name"), null);
  assert.equal(controller.get("configsHash.processor.desc"), "Sorter Vertex");
  assert.equal(controller.get("configsHash.processor.class"), "org.apache.tez.mapreduce.processor.reduce.ReduceProcessor");
  assert.equal(controller.get("configsHash.processor.configs.length"), 2);
  assert.equal(controller.get("configsHash.processor.configs.0.key"), "config.key1");
  assert.equal(controller.get("configsHash.processor.configs.0.value"), 44);
  assert.equal(controller.get("configsHash.processor.configs.1.key"), "config.key2");
  assert.equal(controller.get("configsHash.processor.configs.1.value"), 444);

  assert.ok(controller.get("configsHash.sources"));
  assert.equal(controller.get("configsHash.sources.length"), 0);

  assert.ok(controller.get("configsHash.sinks"));
  assert.equal(controller.get("configsHash.sinks.length"), 1);
  assert.equal(controller.get("configsHash.sinks.0.name"), "MROutput");
  assert.equal(controller.get("configsHash.sinks.0.desc"), "HDFS Output");
  assert.equal(controller.get("configsHash.sinks.0.class"), "org.apache.tez.mapreduce.output.MROutputLegacy");
  assert.equal(controller.get("configsHash.sinks.0.initializer"), "org.apache.tez.mapreduce.committer.MROutputCommitter");
  assert.equal(controller.get("configsHash.sinks.0.configs.length"), 1);
  assert.equal(controller.get("configsHash.sinks.0.configs.0.key"), "config.key");
  assert.equal(controller.get("configsHash.sinks.0.configs.0.value"), 55);

  assert.ok(controller.get("configsHash.inputs"));
  assert.equal(controller.get("configsHash.inputs.length"), 1);
  assert.equal(controller.get("configsHash.inputs.0.name"), null);
  assert.equal(controller.get("configsHash.inputs.0.desc"), "From v2");
  assert.equal(controller.get("configsHash.inputs.0.class"), "org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy");
  assert.equal(controller.get("configsHash.inputs.0.configs.length"), 1);
  assert.equal(controller.get("configsHash.inputs.0.configs.0.key"), "config.key");
  assert.equal(controller.get("configsHash.inputs.0.configs.0.value"), 66);

  assert.ok(controller.get("configsHash.outputs"));
  assert.equal(controller.get("configsHash.outputs.length"), 0);

});

test('configDetails test', function(assert) {
  let configsHash = {
        type: [{
          id: "id1"
        },{
          id: "id2"
        }]
      },
      controller = this.subject({
        send: Ember.K,
        initVisibleColumns: Ember.K,
        configsHash: configsHash
      });

  assert.equal(controller.get("configDetails"), undefined);

  controller.set("configType", "random");
  assert.equal(controller.get("configDetails"), undefined);

  controller.set("configType", "type");
  assert.equal(controller.get("configDetails"), undefined);

  controller.set("configID", "id1");
  assert.equal(controller.get("configDetails"), configsHash.type[0]);

  controller.set("configID", "id2");
  assert.equal(controller.get("configDetails"), configsHash.type[1]);
});

test('configs test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K,
    configDetails: {
      configs: [{
        key: "x",
        value: 1
      }, {
        key: "y",
        value: 2
      }]
    }
  });

  assert.equal(controller.get("configs").length, 2);
  assert.ok(controller.get("configs.0") instanceof Ember.Object);

  assert.equal(controller.get("configs.0.configName"), "x");
  assert.equal(controller.get("configs.0.configValue"), 1);
  assert.equal(controller.get("configs.1.configName"), "y");
  assert.equal(controller.get("configs.1.configValue"), 2);
});
