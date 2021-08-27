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

import EmberObject from '@ember/object';
import { run } from '@ember/runloop';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Model | attempt', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('attempt'));

    assert.ok(model);
  });

  test('index test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('attempt', {
      entityID: "1_2_3"
    }));

    assert.equal(model.get("index"), "3");
  });

  test('taskIndex test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('attempt', {
          taskID: "1_2_3",
        }));

    assert.equal(model.get("taskIndex"), "3");
  });

  test('vertexName test', function(assert) {
    let testVertexName = "Test Vertex",
        model = run(() => this.owner.lookup('service:store').createRecord('attempt', {
          vertexID: "1_2",
          dag: {
            vertexIdNameMap: {
              "1_2": testVertexName
            }
          }
        }));

    assert.equal(model.get("vertexName"), testVertexName);
  });

  test('logURL test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('attempt', {
          entityID: "id_1",
          dag: EmberObject.create(),
          env: {
            app: {
              yarnProtocol: "ptcl"
            }
          },
          completedLogsURL: "http://abc.com/completed/link.log.done"
        }));

    run(function () {
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
});
