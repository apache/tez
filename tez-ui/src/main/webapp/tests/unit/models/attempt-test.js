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
import { moduleForModel, test } from 'ember-qunit';

moduleForModel('attempt', 'Unit | Model | attempt', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(model);

  assert.ok(model.needs.dag);
  assert.ok(model.needs.am);

  assert.ok(model.taskID);
  assert.ok(model.taskIndex);

  assert.ok(model.vertexID);
  assert.ok(model.vertexIndex);
  assert.ok(model.vertexName);

  assert.ok(model.dagID);
  assert.ok(model.dag);

  assert.ok(model.containerID);
  assert.ok(model.nodeID);

  assert.ok(model.inProgressLogsURL);
  assert.ok(model.completedLogsURL);
  assert.ok(model.logURL);
  assert.ok(model.containerLogURL);
});

test('index test', function(assert) {
  let model = this.subject({
    entityID: "1_2_3"
  });

  assert.equal(model.get("index"), "3");
});

test('taskIndex test', function(assert) {
  let model = this.subject({
        taskID: "1_2_3",
      });

  assert.equal(model.get("taskIndex"), "3");
});

test('vertexName test', function(assert) {
  let testVertexName = "Test Vertex",
      model = this.subject({
        vertexID: "1_2",
        dag: {
          vertexIdNameMap: {
            "1_2": testVertexName
          }
        }
      });

  assert.equal(model.get("vertexName"), testVertexName);
});

test('logURL test', function(assert) {
  let model = this.subject({
        entityID: "id_1",
        dag: Ember.Object.create(),
        env: {
          app: {
            yarnProtocol: "ptcl"
          }
        },
        completedLogsURL: "http://abc.com/completed/link.log.done"
      });

  Ember.run(function () {
    // Normal Tez log link
    model.set("inProgressLogsURL", "abc.com/test/link");
    assert.equal(model.get("logURL"), "ptcl://abc.com/test/link/syslog_id_1");

    // LLAP log link - In Progress
    model.set("inProgressLogsURL", "http://abc.com/in-progress/link.log");
    assert.equal(model.get("logURL"), "http://abc.com/in-progress/link.log");

    // LLAP log link - Completed
    model.set("dag.isComplete", true);
    assert.equal(model.get("logURL"), "http://abc.com/completed/link.log.done");
  });
});