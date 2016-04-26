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

moduleFor('adapter:loader', 'Unit | Adapter | loader', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:foo']
});

test('Basic creation', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.ok(adapter._isLoader);
  assert.ok(adapter.buildURL);
  assert.ok(adapter._loaderAjax);
  assert.ok(adapter.queryRecord);
  assert.ok(adapter.query);
});

test('buildURL test', function(assert) {
  let adapter = this.subject();

  assert.equal(adapter.buildURL("dag"), "/dags");
  assert.equal(adapter.buildURL("dag", "dag1"), "/dags/dag1");
  assert.equal(adapter.buildURL("{x}dag", "dag1", null, null, null, {x: "x_x"}), "/x_xdags/dag1", "Test for substitution");
});

test('_loaderAjax test', function(assert) {
  let adapter = this.subject(),
      testURL = "/dags",
      testQueryParams = { x:1 },
      testRecord = {},
      testNameSpace = "ns";

  assert.expect(2 + 1 + 2);

  adapter.ajax = function (url, method/*, options*/) {

    assert.equal(url, testURL);
    assert.equal(method, "GET");

    return Ember.RSVP.resolve(testRecord);
  };

  adapter.sortQueryParams = function (queryParams) {
    assert.ok(queryParams, "sortQueryParams was called with query params");
  };

  adapter._loaderAjax(testURL, testQueryParams, testNameSpace).then(function (data) {
    assert.equal(data.nameSpace, testNameSpace, "Namespace returned");
    assert.equal(data.data, testRecord, "Test record returned");
  });
});

test('queryRecord test', function(assert) {
  let adapter = this.subject(),
      testURL = "/dags",
      testModel = { modelName: "testModel" },
      testStore = {},
      testQuery = {
        id: "test1",
        params: {},
        urlParams: {},
        nameSpace: "ns"
      };

  assert.expect(4 + 3);

  adapter.buildURL = function (modelName, id, snapshot, requestType, query, params) {
    assert.equal(modelName, testModel.modelName);
    assert.equal(id, testQuery.id);
    assert.equal(query, testQuery.params);
    assert.equal(params, testQuery.urlParams);

    return testURL;
  };

  adapter._loaderAjax = function (url, queryParams, nameSpace) {
    assert.equal(url, testURL);
    assert.equal(queryParams, testQuery.params);
    assert.equal(nameSpace, testQuery.nameSpace);
  };

  adapter.queryRecord(testStore, testModel, testQuery);
});

test('query test', function(assert) {
  let adapter = this.subject(),
      testURL = "/dags",
      testModel = { modelName: "testModel" },
      testStore = {},
      testQuery = {
        id: "test1",
        params: {},
        urlParams: {},
        nameSpace: "ns"
      };

  assert.expect(5 + 3);

  adapter.buildURL = function (modelName, id, snapshot, requestType, query, params) {
    assert.equal(modelName, testModel.modelName);
    assert.equal(id, null);
    assert.equal(requestType, "query");
    assert.equal(query, testQuery.params);
    assert.equal(params, testQuery.urlParams);

    return testURL;
  };

  adapter._loaderAjax = function (url, queryParams, nameSpace) {
    assert.equal(url, testURL);
    assert.equal(queryParams, testQuery.params);
    assert.equal(nameSpace, testQuery.nameSpace);
  };

  adapter.query(testStore, testModel, testQuery);
});
