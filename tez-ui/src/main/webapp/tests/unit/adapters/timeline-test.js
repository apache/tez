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

import { moduleFor, test } from 'ember-qunit';

moduleFor('adapter:timeline', 'Unit | Adapter | timeline', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:foo']
});

test('Basic creation test', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.ok(adapter.filters);
  assert.ok(adapter.stringifyFilters);
  assert.ok(adapter.normalizeQuery);
  assert.ok(adapter.query);

  assert.equal(adapter.serverName, "timeline");
});

test('stringifyFilters test', function(assert) {
  let adapter = this.subject();

  assert.equal(adapter.stringifyFilters({a: 1, b: 2}), 'a:"1",b:"2"');
  assert.throws(function () {
    adapter.stringifyFilters();
  });

  assert.equal(adapter.stringifyFilters({a: "123", b: "abc"}), 'a:"123",b:"abc"');
  assert.equal(adapter.stringifyFilters({a: '123', b: 'abc'}), 'a:"123",b:"abc"');
  assert.equal(adapter.stringifyFilters({a: '123"abc'}), 'a:"123\\"abc"');
});

test('normalizeQuery test', function(assert) {
  let adapter = this.subject(),
      normalQuery;

  adapter.set("filters", {
    a: "A_ID",
    b: "B_ID",
  });

  normalQuery = adapter.normalizeQuery({a: 1, b: 2, c: 3, d: 4});

  assert.deepEqual(normalQuery.primaryFilter, 'A_ID:"1"');
  assert.deepEqual(normalQuery.secondaryFilter, 'B_ID:"2"');
  assert.deepEqual(normalQuery.c, 3);
  assert.deepEqual(normalQuery.d, 4);
});

test('query test', function(assert) {
  let adapter = this.subject(),
      normalQuery = {},
      testStore = {},
      testType = "ts",
      testQuery = {};

  assert.expect(1 + 1);

  adapter.normalizeQuery = function (params) {
    assert.equal(params, testQuery);
    return normalQuery;
  };
  adapter._loaderAjax = function (url, queryParams) {
    assert.equal(queryParams, normalQuery);
  };

  adapter.query(testStore, testType, {
    params: testQuery
  });
});
