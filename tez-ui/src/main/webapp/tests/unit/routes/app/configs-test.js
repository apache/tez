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
import { resolve, reject } from 'rsvp';

module('Unit | Route | app/configs', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let route = this.owner.lookup('route:app/configs');

    assert.ok(route);
  });

  test('setupController test', function(assert) {
    assert.expect(1);

    let route = this.owner.factoryFor('route:app/configs').create({
      startCrumbBubble: function () {
        assert.ok(true);
      }
    });

    route.setupController({}, {});
  });

  test('load test', function(assert) {
    let entityID = "123",
        testOptions = {},
        testData = {},
        route = this.owner.factoryFor('route:app/configs').create({
          modelFor: function (type) {
            assert.equal(type, "app");
            return EmberObject.create({
              entityID: entityID
            });
          }
        });
    route.loader = {
      queryRecord: function (type, id, options) {
        assert.equal(type, "app");
        assert.equal(id, "tez_123");
        assert.equal(options, testOptions);
        return resolve(testData);
      }
    };

    route.load(null, null, testOptions).then(function (data) {
      assert.equal(data, testData);
    });

    assert.expect(1 + 3 + 1);
  });

  test('load failure test', function(assert) {
    let route = this.owner.factoryFor('route:app/configs').create({
          modelFor: function (type) {
            assert.equal(type, "app");
            return EmberObject.create();
          },
        });
    route.loader = {
      queryRecord: function () {
        return reject(new Error());
      }
    };

    route.load(null, null, {}).then(function (data) {
      assert.ok(Array.isArray(data));
      assert.equal(data.length, 0);
    });

    assert.expect(1 + 2);
  });
});
