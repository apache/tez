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
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Route | dag/index', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let route = this.owner.lookup('route:dag/index');

    assert.ok(route);
  });

  test('setupController test', function(assert) {
    assert.expect(1);

    let route = this.owner.factoryFor('route:dag/index').create({
      startCrumbBubble: function () {
        assert.ok(true);
      }
    });

    route.setupController({}, {});
  });

  test('getCallerInfo test', function(assert) {
    let route = this.owner.factoryFor('route:dag/index').create({
        startCrumbBubble() {}
        }),

        testID = "id",
        testType = "entity",

        dag = EmberObject.create({
          name: "hive_query_id:1",
        }),
        callerInfo;

    // callerID computed - No callerType
    callerInfo = route.getCallerInfo(dag);
    assert.equal(callerInfo.id, "hive_query_id");
    assert.equal(callerInfo.type, "HIVE_QUERY_ID");

    // callerID computed - No callerID
    dag.set("callerType", testType);
    callerInfo = route.getCallerInfo(dag);
    assert.equal(callerInfo.id, "hive_query_id");
    assert.equal(callerInfo.type, "HIVE_QUERY_ID");

    // callerID & callerType available
    dag.set("callerID", testID);
    callerInfo = route.getCallerInfo(dag);
    assert.equal(callerInfo.id, testID);
    assert.equal(callerInfo.type, testType);
  });
});
