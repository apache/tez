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

moduleFor('serializer:attempt', 'Unit | Serializer | attempt', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:attempt']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.equal(Object.keys(serializer.maps).length, 8 + 8);
});

test('containerLogURL test', function(assert) {
  let serializer = this.subject({
    env: {
      app: {
        yarnProtocol: "ptcl"
      }
    }
  });

  assert.equal(serializer.maps.containerLogURL.call(serializer, {
    otherinfo: {
      inProgressLogsURL: "abc.com/test/link",
    }
  }), "ptcl://abc.com/test/link");
});
