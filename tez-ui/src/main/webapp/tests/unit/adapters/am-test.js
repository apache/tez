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

moduleFor('adapter:am', 'Unit | Adapter | am', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:foo']
});

test('Basic creation test', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.equal(adapter.serverName, "am");

  assert.ok(adapter.queryRecord);
});

test('queryRecord test', function(assert) {
  let testStore = {},
      testType = {},
      testQuery = {},

      adapter = this.subject({
        query: function (store, type, query) {
          assert.equal(store, testStore);
          assert.equal(type, testType);
          assert.equal(query, testQuery);
        }
      });

  assert.expect(3);
  adapter.queryRecord(testStore, testType, testQuery);
});
