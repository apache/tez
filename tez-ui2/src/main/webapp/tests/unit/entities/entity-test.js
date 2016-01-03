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

moduleFor('entitie:entity', 'Unit | Entity | entity', {
  // Specify the other units that are required for this test.
  // needs: ['entitie:foo']
});

test('Basic creation', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.ok(adapter.loadRelations);
  assert.ok(adapter.normalizeNeed);
  assert.ok(adapter.loadNeeds);
});

test('loadRelations creation', function(assert) {
  let adapter = this.subject(),
      testLoader = {},
      testModel = {},
      relationsPromise;

  assert.expect(2 + 1 + 2 + 2);

  // Test model without needs
  adapter.loadNeeds = function (loader, model) {
    assert.equal(loader, testLoader);
    assert.equal(model, testModel);

    return null;
  };
  relationsPromise = adapter.loadRelations(testLoader, testModel);

  assert.equal(relationsPromise, testModel, "Model without needs");

  // Test model with needs
  adapter.loadNeeds = function (loader, model) {
    assert.equal(loader, testLoader);
    assert.equal(model, testModel);

    return Ember.RSVP.resolve();
  };
  relationsPromise = adapter.loadRelations(testLoader, testModel);

  assert.notEqual(relationsPromise, testModel);
  relationsPromise.then(function (model) {
    assert.equal(model, testModel);
  });

});

test('normalizeNeed creation', function(assert) {
  let adapter = this.subject();

  assert.deepEqual(adapter.normalizeNeed("app", "appKey"), {
    name: "app",
    type: "app",
    idKey: "appKey",
    lazy: false
  }, "Test 1");

  assert.deepEqual(adapter.normalizeNeed( "app", { idKey: "appKey" }), {
    name: "app",
    type: "app",
    idKey: "appKey",
    lazy: false
  }, "Test 2");

  assert.deepEqual(adapter.normalizeNeed( "app", { type: "application", idKey: "appKey" }), {
    name: "app",
    type: "application",
    idKey: "appKey",
    lazy: false
  }, "Test 3");

  assert.deepEqual(adapter.normalizeNeed( "app", { lazy: true, idKey: "appKey" }), {
    name: "app",
    type: "app",
    idKey: "appKey",
    lazy: true
  }, "Test 4");
});

test('loadNeeds creation', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        needs: {
          app: "appID",
          foo: "fooID"
        },
        appID: 1,
        fooID: 2
      });

  assert.expect(1 + 2 + 1);

  assert.equal(adapter.loadNeeds(loader, Ember.Object.create()), null, "Model without needs");

  loader = {
    queryRecord: function (type, id) {

      // Must be called twice, once for each record
      switch(type) {
        case "app":
          assert.equal(id, testModel.get("appID"));
        break;
        case "foo":
          assert.equal(id, testModel.get("fooID"));
        break;
      }

      return Ember.RSVP.resolve();
    }
  };
  adapter.loadNeeds(loader, testModel).then(function () {
    assert.ok(true);
  });
});
