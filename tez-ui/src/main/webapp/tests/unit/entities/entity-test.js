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

module('Unit | Entity | entity', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let entity = this.owner.lookup('entitie:entity');

    assert.ok(entity);

    assert.ok(entity.queryRecord);
    assert.ok(entity.query);

    assert.ok(entity.normalizeNeed);
    assert.ok(entity.setNeed);
    assert.ok(entity._loadNeed);
    assert.ok(entity.loadNeed);

    assert.ok(entity._loadAllNeeds);
    assert.ok(entity.loadAllNeeds);
  });

  test('normalizeNeed test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        expectedProperties = ["id", "name", "type", "idKey", "silent", "queryParams", "urlParams"],
        testParentModel = EmberObject.create({
          appKey: "id_1"
        }),
        testQueryParams = { x: 1 },
        testUrlParams = { y: 2 };
    let actualProperties = entity.normalizeNeed("app", "appKey", testParentModel, testQueryParams, testUrlParams);
    assert.equal(actualProperties.id, "id_1");
    assert.equal(actualProperties.name, "app");
    assert.equal(actualProperties.type, "app");
    assert.equal(actualProperties.idKey, "appKey");
    assert.equal(actualProperties.silent, false);
    assert.deepEqual(actualProperties.queryParams, testQueryParams);
    assert.deepEqual(actualProperties.urlParams, testUrlParams);

    actualProperties = entity.normalizeNeed( "app", {
      idKey: "appKey",
      queryParams: { x: 3 },
      urlParams: { y: 4 }
    }, testParentModel);
    assert.equal(actualProperties.id, "id_1");
    assert.equal(actualProperties.name, "app");
    assert.equal(actualProperties.type, "app");
    assert.equal(actualProperties.idKey, "appKey");
    assert.equal(actualProperties.silent, false);
    assert.deepEqual(actualProperties.queryParams, { x: 3 });
    assert.deepEqual(actualProperties.urlParams, { y: 4 });

    actualProperties = entity.normalizeNeed( "app", {
      type: "application",
      idKey: "appKey",
      queryParams: { x: 3 },
      urlParams: { y: 4 }
    }, testParentModel, testQueryParams, testUrlParams);
    assert.equal(actualProperties.id, "id_1");
    assert.equal(actualProperties.name, "app");
    assert.equal(actualProperties.type, "application");
    assert.equal(actualProperties.idKey, "appKey");
    assert.equal(actualProperties.silent, false);
    assert.deepEqual(actualProperties.queryParams, testQueryParams);
    assert.deepEqual(actualProperties.urlParams, testUrlParams);

    actualProperties = entity.normalizeNeed( "app", {
      silent: true,
      idKey: "appKey",
      queryParams: function () {
        return { x: 5 };
      },
      urlParams: function () {
        return { y: 6 };
      },
    }, testParentModel);
    assert.equal(actualProperties.id, "id_1");
    assert.equal(actualProperties.name, "app");
    assert.equal(actualProperties.type, "app");
    assert.equal(actualProperties.idKey, "appKey");
    assert.equal(actualProperties.silent, true);
    assert.deepEqual(actualProperties.queryParams, { x: 5 });
    assert.deepEqual(actualProperties.urlParams, { y: 6 });
  });

  test('loadAllNeeds basic test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        loader,
        testModel = EmberObject.create({
          refreshLoadTime() {},
          needs: {
            app: "appID",
            foo: "fooID"
          },
          appID: 1,
          fooID: 2
        });

    assert.expect(1 + 2 + 1);

    assert.equal(entity.loadAllNeeds(loader, EmberObject.create({refreshLoadTime() {}})), undefined, "Model without needs");

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

        return resolve();
      }
    };
    entity.loadAllNeeds(loader, testModel).then(function () {
      assert.ok(true);
    });
  });

  test('loadAllNeeds silent=false test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        loader,
        testModel = EmberObject.create({
          refreshLoadTime() {},
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
        return reject(testErr);
      }
    };
    entity.loadAllNeeds(loader, testModel).catch(function (err) {
      assert.equal(err, testErr);
    });
  });

  test('loadAllNeeds silent=true test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        loader,
        testModel = EmberObject.create({
          refreshLoadTime() {},
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
        return resolve();
      }
    };
    entity.loadAllNeeds(loader, testModel).then(function (val) {
      assert.ok(val);
    });
  });

  test('setNeed test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        parentModel = EmberObject.create(),
        testModel = {},
        testName = "name";

    assert.expect(2);

    entity.setNeed(parentModel, testName, testModel);
    assert.equal(parentModel.get(testName), testModel);

    parentModel.set("isDeleted", true);
    parentModel.set(testName, undefined);
    entity.setNeed(parentModel, testName, testModel);
    assert.equal(parentModel.get(testName), undefined);
  });

  test('loadAllNeeds loadType=function test', function(assert) {
    var entity = this.owner.lookup('entitie:entity'),
        loader = {},
        testRecord = EmberObject.create({
          refreshLoadTime() {},
          needs: {
            app: {
              idKey: "appID",
              loadType: function (record) {
                assert.strictEqual(testRecord, record);
                return "demand";
              }
            },
          },
          appID: 1,
        });

    entity._loadNeed = function () {
      assert.ok(true); // Shouldn't be called
    };

    assert.expect(1 + 1);
    assert.equal(entity.loadAllNeeds(loader, testRecord), undefined);
  });

  test('_loadNeed single string type test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        loader,
        testModel = EmberObject.create({
          refreshLoadTime() {},
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
        return resolve();
      }
    };
    entity.loadAllNeeds(loader, testModel).then(function (val) {
      assert.ok(val);
    });
  });

  test('_loadNeed multiple type test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        loader,
        testModel = EmberObject.create({
          refreshLoadTime() {},
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
          return reject();
        }
        else {
          assert.equal(type, "appRm");
          return resolve();
        }
      }
    };
    entity.loadAllNeeds(loader, testModel).then(function (val) {
      assert.ok(val);
    });
  });

  test('_loadNeed test with silent false', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
        loader,
        testModel = EmberObject.create({
          refreshLoadTime() {},
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
        return reject(testErr);
      }
    };
    entity.loadAllNeeds(loader, testModel).catch(function (err) {
      assert.equal(err, testErr);
    });
  });

  test('resetAllNeeds test', function(assert) {
    let entity = this.owner.lookup('entitie:entity'),
      loader = {},
        parentModel = EmberObject.create({
          needs : {
            foo: {
              loadType: 'demand',
            },
            bar: {
              loadType: 'demand',
            },
          },
          foo: 1,
          bar: 2,
          refreshLoadTime: function () {
            assert.ok(true);
          }
        });

    assert.expect(2 + 2 + 1);

    assert.equal(parentModel.get("foo"), 1);
    assert.equal(parentModel.get("bar"), 2);

    entity.loadAllNeeds(loader, parentModel);

    assert.equal(parentModel.get("foo"), null);
    assert.equal(parentModel.get("bar"), null);
  });
});
