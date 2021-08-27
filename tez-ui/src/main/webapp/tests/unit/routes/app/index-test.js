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

module('Unit | Route | app/index', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let route = this.owner.lookup('route:app/index');

    assert.ok(route);
  });

  test('setupController test', function(assert) {
    assert.expect(1);

    let route = this.owner.factoryFor('route:app/index').create({
      startCrumbBubble: function () {
        assert.ok(true);
      }
    });

    route.setupController({}, {});
  });

  test('Test load', function(assert) {
    let testID = "123",
        testOptions = {},
        testErr = {},
        route = this.owner.factoryFor('route:app/index').create({
          modelFor: function (type) {
            assert.equal(type, "app");
            return EmberObject.create({
              entityID: testID
            });
          },
          get: function (name) {
            return { // injecting our own mock loader by overriding get
              queryRecord: function (type, id, options) {
                assert.equal(type, "app");
                assert.equal(id, "tez_123");
                assert.equal(options, testOptions);
                return {
                  catch: function (callback) {
                    return callback(testErr);
                  }
                };
              },
              query: function (type, query, options) {
                assert.equal(type, "dag");
                assert.equal(query.appID, testID);
                assert.equal(query.limit, 1);
                assert.equal(options, testOptions);
                return {
                  then: function (callback) {
                    return callback([]);
                  }
                };
              }
            };
          }
        });

    assert.expect(1 + 3 + 4 + 1);

    assert.throws(function () {
      route.load(null, null, testOptions);
    });
  });
});
