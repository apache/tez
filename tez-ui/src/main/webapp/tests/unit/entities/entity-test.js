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

  assert.ok(adapter.queryRecord);
  assert.ok(adapter.query);

  assert.ok(adapter.normalizeNeed);
  assert.ok(adapter._loadNeed);
  assert.ok(adapter.loadNeed);

  assert.ok(adapter._loadAllNeeds);
  assert.ok(adapter.loadAllNeeds);
});

test('normalizeNeed test', function(assert) {
  let adapter = this.subject(),
      expectedProperties = ["name", "type", "idKey", "silent"];

  assert.deepEqual(adapter.normalizeNeed("app", "appKey").getProperties(expectedProperties), {
    name: "app",
    type: "app",
    idKey: "appKey",
    silent: false
  }, "Test 1");

  assert.deepEqual(adapter.normalizeNeed( "app", { idKey: "appKey" }).getProperties(expectedProperties), {
    name: "app",
    type: "app",
    idKey: "appKey",
    silent: false
  }, "Test 2");

  assert.deepEqual(adapter.normalizeNeed( "app", { type: "application", idKey: "appKey" }).getProperties(expectedProperties), {
    name: "app",
    type: "application",
    idKey: "appKey",
    silent: false
  }, "Test 3");

  assert.deepEqual(adapter.normalizeNeed( "app", { silent: true, idKey: "appKey" }).getProperties(expectedProperties), {
    name: "app",
    type: "app",
    idKey: "appKey",
    silent: true
  }, "Test 4");
});

test('loadAllNeeds basic test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: "appID",
          foo: "fooID"
        },
        appID: 1,
        fooID: 2
      });

  assert.expect(1 + 2 + 1);

  assert.equal(adapter.loadAllNeeds(loader, Ember.Object.create()), undefined, "Model without needs");

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
  adapter.loadAllNeeds(loader, testModel).then(function () {
    assert.ok(true);
  });
});

test('loadAllNeeds silent=false test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
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
  adapter.loadAllNeeds(loader, testModel).catch(function (err) {
    assert.equal(err, testErr);
  });
});

test('loadAllNeeds silent=true test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
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
  adapter.loadAllNeeds(loader, testModel).then(function (val) {
    assert.ok(val);
  });
});

test('_loadNeed single string type test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            type: "appRm",
            idKey: "appID",
            silent: true
          },
        },
        appID: 1,
      });

  assert.expect(2 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      assert.equal(type, "appRm");
      return Ember.RSVP.resolve();
    }
  };
  adapter.loadAllNeeds(loader, testModel).then(function (val) {
    assert.ok(val);
  });
});

test('_loadNeed multiple type test', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            type: ["AhsApp", "appRm"],
            idKey: "appID",
            silent: true
          },
        },
        appID: 1,
      });

  assert.expect(2 * 2 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));

      if(type === "AhsApp") {
        assert.ok(true);
        return Ember.RSVP.reject();
      }
      else {
        assert.equal(type, "appRm");
        return Ember.RSVP.resolve();
      }
    }
  };
  adapter.loadAllNeeds(loader, testModel).then(function (val) {
    assert.ok(val);
  });
});

test('_loadNeed test with silent false', function(assert) {
  let adapter = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            type: ["AhsApp"],
            idKey: "appID",
            silent: false
          },
        },
        appID: 1,
      }),
      testErr = {};

  assert.expect(2 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      assert.equal(type, "AhsApp");
      return Ember.RSVP.reject(testErr);
    }
  };
  adapter.loadAllNeeds(loader, testModel).catch(function (err) {
    assert.equal(err, testErr);
  });
});
