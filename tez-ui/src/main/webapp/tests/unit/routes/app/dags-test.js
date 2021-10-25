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

module('Unit | Route | app/dags', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let route = this.owner.lookup('route:app/dags');

    assert.ok(route);
  });

  test('setupController test', function(assert) {
    assert.expect(1);

    let route = this.owner.factoryFor('route:app/dags').create({
      startCrumbBubble: function () {
        assert.ok(true);
      }
    });

    route.setupController({}, {});
  });

  test('Test load', function(assert) {
    let testID = "123",
        testOptions = {},
        testData = {},
        route = this.owner.factoryFor('route:app/dags').create({
          modelFor: function (type) {
            assert.equal(type, "app");
            return EmberObject.create({
              entityID: testID
            });
          },
          get: function () {
            return {
              query: function (type, query, options) {
                assert.equal(type, "dag");
                assert.equal(query.appID, testID);
                assert.equal(options, testOptions);
                return testData;
              }
            };
          }
        }),
        data;

    assert.expect(1 + 3 + 1);

    data = route.load(null, null, testOptions);
    assert.equal(data, testData);
  });
});
