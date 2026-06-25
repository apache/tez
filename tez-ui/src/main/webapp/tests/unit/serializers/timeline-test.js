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

moduleFor('serializer:timeline', 'Unit | Serializer | timeline', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:timeline']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer.extractArrayPayload);
  assert.ok(serializer.maps);

  assert.equal(Object.keys(serializer.get("maps")).length, 7);
  assert.equal(serializer.primaryKey, 'entityId');
});

test('extractArrayPayload test', function(assert) {
  let serializer = this.subject(),
      testPayload = {
        entities: []
      };

  assert.equal(serializer.extractArrayPayload(testPayload), testPayload.entities);
});

test('getDiagnostics escapes HTML', function(assert) {
  let serializer = this.subject(),
      mapper = serializer.maps.diagnostics;

  // Empty/missing diagnostics
  assert.equal(mapper({}), "");
  assert.equal(mapper({otherInfo: {}}), "");

  // Script injection must be escaped
  assert.equal(
    mapper({otherInfo: {diagnostics: "<script>alert(1)</script>"}}),
    "&lt;script&gt;alert(1)&lt;/script&gt;"
  );

  // IMG onerror injection must be escaped
  assert.equal(
    mapper({otherInfo: {diagnostics: "<img src=x onerror=alert(1)>"}}),
    "&lt;img src=x onerror=alert(1)&gt;"
  );

  // Tab formatting still works after escaping
  assert.equal(
    mapper({otherInfo: {diagnostics: "\tindented"}}),
    "&emsp;&emsp;indented"
  );

  // Bracket formatting still works after escaping
  assert.equal(
    mapper({otherInfo: {diagnostics: "[section]"}}),
    "<div>&#187; section</div>"
  );

  // Ampersands in diagnostics are escaped
  assert.equal(
    mapper({otherInfo: {diagnostics: "a & b"}}),
    "a &amp; b"
  );
});
