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

import { txt } from '../../../helpers/txt';
import { module, test } from 'qunit';

module('Unit | Helper | txt');

test('txt: created', function(assert) {
  assert.ok(txt);
});

test('txt: String', function(assert) {
  assert.equal(txt(["Abc"], {}), "Abc");
  assert.equal(txt(null, {}).string, '<span class="txt-message"> Not Available! </span>');
});

test('txt: String - success', function(assert) {
  assert.equal(txt(["Abc"], {}), "Abc");
  assert.equal(txt(null, {}).string, '<span class="txt-message"> Not Available! </span>');
  assert.equal(txt([null], {}).string, '<span class="txt-message"> Not Available! </span>');
});

test('txt: String - error', function(assert) {
  var obj = {};

  obj.toString = null;
  assert.equal(txt([obj], {}).string, '<span class="txt-message"> Invalid Data! </span>');
});

test('txt: json', function(assert) {
  var obj = {
    x: 1,
    y: 2
  };
  assert.equal(txt([obj], {
    type: "json",
  }).string, '{\n    &quot;x&quot;: 1,\n    &quot;y&quot;: 2\n}');
});

test('txt: error', function(assert) {
  var err = new Error("testError");
  assert.equal(txt([err], {}).string, '<span title="testError" class="txt-message"> testError </span>');
});
