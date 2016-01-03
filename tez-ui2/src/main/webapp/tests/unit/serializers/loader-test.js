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

moduleFor('serializer:loader', 'Unit | Serializer | loader', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:loader']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer._isLoader);

  assert.ok(serializer.extractId);
  assert.ok(serializer.extractAttributes);
  assert.ok(serializer.extractRelationships);

  assert.ok(serializer.extractSinglePayload);
  assert.ok(serializer.extractArrayPayload);

  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.normalizeArrayResponse);
});

test('extractId test', function(assert) {
  let serializer = this.subject(),
    modelClass = {},
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        entityID: 3
      }
    };

  assert.equal(serializer.extractId(modelClass, resourceHash), "ns:1", "With name-space");
  assert.equal(serializer.extractId(modelClass, { data: {id: 2} }), 2, "Without name-space");

  serializer.primaryKey = "entityID";
  assert.equal(serializer.extractId(modelClass, resourceHash), "ns:3", "Different primary key");
});

test('extractAttributes test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: function (callback) {
        callback("id", {type: "string"});
        callback("appID", {type: "string"});
        callback("status", {type: "string"});
      }
    },
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        appID: 2,
        applicationID: 3,
        info: {
          status: "SUCCESS"
        }
      }
    };

  assert.deepEqual(serializer.extractAttributes(modelClass, resourceHash), {
    id: 1,
    appID: 2
  });

  serializer.maps = {
    id: "id",
    appID: "applicationID",
    status: "info.status"
  };

  assert.deepEqual(serializer.extractAttributes(modelClass, resourceHash), {
    id: 1,
    appID: 3,
    status: "SUCCESS"
  });
});

test('extractRelationships test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: Ember.K,
      eachRelationship: function (callback) {
        callback("app", {
          key: "app",
          kind: "belongsTo",
          options: {},
          parentType: "parent",
          type: "app"
        });
      },
      eachTransformedAttribute: Ember.K
    },
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        app: "",
      }
    };

  assert.deepEqual(serializer.extractRelationships(modelClass, resourceHash), {
    app: {
      data: {
        id: null,
        type:"app"
      }
    }
  });

});

test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: function (callback) {
        callback("id", {type: "string"});
        callback("appID", {type: "string"});
        callback("status", {type: "string"});
      },
      eachRelationship: Ember.K,
      eachTransformedAttribute: Ember.K
    },
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        appID: 2,
        applicationID: 3,
        info: {
          status: "SUCCESS"
        }
      }
    };

  var response = serializer.normalizeSingleResponse({}, modelClass, resourceHash, null, null);

  assert.equal(response.data.id, "ns:1");
  assert.equal(response.data.attributes.id, 1);
  assert.equal(response.data.attributes.appID, 2);
});

test('normalizeArrayResponse test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: function (callback) {
        callback("id", {type: "string"});
        callback("appID", {type: "string"});
        callback("status", {type: "string"});
      },
      eachRelationship: Ember.K,
      eachTransformedAttribute: Ember.K
    },
    resourceHash = {
      nameSpace: "ns",
      data: [{
        id: 1,
        appID: 2,
      },{
        id: 2,
        appID: 4,
      }]
    };

  var response = serializer.normalizeArrayResponse({}, modelClass, resourceHash, null, null);

  assert.equal(response.data.length, 2);
  assert.deepEqual(response.data[0].id, "ns:1");
  assert.deepEqual(response.data[1].id, "ns:2");
});
