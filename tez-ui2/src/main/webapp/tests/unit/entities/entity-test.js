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

test('Basic creation test', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.ok(adapter.loadRelations);
  assert.ok(adapter.normalizeNeed);
  assert.ok(adapter.loadNeeds);
});

test('loadRelations test', function(assert) {
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

test('normalizeNeed test', function(assert) {
  let adapter = this.subject(),
      expectedProperties = ["name", "type", "idKey", "lazy", "silent"];

  assert.deepEqual(adapter.normalizeNeed("app", "appKey").getProperties(expectedProperties), {
    name: "app",
    type: "app",
    idKey: "appKey",
    lazy: false,
    silent: false
  }, "Test 1");

  assert.deepEqual(adapter.normalizeNeed( "app", { idKey: "appKey" }).getProperties(expectedProperties), {
    name: "app",
    type: "app",
    idKey: "appKey",
    lazy: false,
    silent: false
  }, "Test 2");

  assert.deepEqual(adapter.normalizeNeed( "app", { type: "application", idKey: "appKey" }).getProperties(expectedProperties), {
    name: "app",
    type: "application",
    idKey: "appKey",
    lazy: false,
    silent: false
  }, "Test 3");

  assert.deepEqual(adapter.normalizeNeed( "app", { lazy: true, idKey: "appKey" }).getProperties(expectedProperties), {
    name: "app",
    type: "app",
    idKey: "appKey",
    lazy: true,
    silent: false
  }, "Test 4");

  assert.deepEqual(adapter.normalizeNeed( "app", { silent: true, idKey: "appKey" }).getProperties(expectedProperties), {
    name: "app",
    type: "app",
    idKey: "appKey",
    lazy: false,
    silent: true
  }, "Test 5");
});

test('loadNeeds basic test', function(assert) {
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

  assert.equal(adapter.loadNeeds(loader, Ember.Object.create()), undefined, "Model without needs");

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

test('loadNeeds silent=false test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        needs: {
          app: {
            idKey: "appID",
            // silent: false - By default it's false
          },
        },
        appID: 1,
      }),
      testErr = {};

  assert.expect(1 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      return Ember.RSVP.reject(testErr);
    }
  };
  adapter.loadNeeds(loader, testModel).catch(function (err) {
    assert.equal(err, testErr);
  });
});

test('loadNeeds silent=true test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        needs: {
          app: {
            idKey: "appID",
            silent: true
          },
        },
        appID: 1,
      });

  assert.expect(1 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      return Ember.RSVP.resolve();
    }
  };
  adapter.loadNeeds(loader, testModel).then(function (val) {
    assert.ok(val);
  });
});

test('loadNeeds lazy=true test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        needs: {
          app: {
            idKey: "appID",
            lazy: true
          },
        },
        appID: 1,
      });

  assert.expect(1 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      return Ember.RSVP.resolve();
    }
  };
  assert.equal(adapter.loadNeeds(loader, testModel), undefined, "Model without needs");
});
