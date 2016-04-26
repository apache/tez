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

moduleFor('service:hosts', 'Unit | Service | hosts', {
  // Specify the other units that are required for this test.
  needs: ['service:env']
});

test('Test creation', function(assert) {
  let service = this.subject();
  assert.ok(service);
});

test('Test correctProtocol', function(assert) {
  let service = this.subject();

  //No correction
  assert.equal(service.correctProtocol("http://localhost:8088"), "http://localhost:8088");

  // Correction
  assert.equal(service.correctProtocol("localhost:8088"), "http://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088"), "http://localhost:8088");
  assert.equal(service.correctProtocol("file://localhost:8088"), "http://localhost:8088");

  assert.equal(service.correctProtocol("localhost:8088", "http:"), "http://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088", "http:"), "http://localhost:8088");
  assert.equal(service.correctProtocol("file://localhost:8088", "http:"), "http://localhost:8088");

  assert.equal(service.correctProtocol("localhost:8088", "https:"), "https://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088", "https:"), "https://localhost:8088");
  assert.equal(service.correctProtocol("file://localhost:8088", "https:"), "https://localhost:8088");
});

test('Test correctProtocol with protocol=file:', function(assert) {
  let service = this.subject();

  assert.equal(service.correctProtocol("file://localhost:8088", "file:"), "file://localhost:8088");
  assert.equal(service.correctProtocol("http://localhost:8088", "file:"), "http://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088", "file:"), "https://localhost:8088");
});

test('Test host URLs', function(assert) {
  let service = this.subject();

  assert.equal(service.get("timeline"), "http://localhost:8188");
  assert.equal(service.get("rm"), "http://localhost:8088");
});

test('Test host URLs with ENV set', function(assert) {
  let service = this.subject();

  window.ENV = {
    hosts: {
      timeline: "https://localhost:3333",
      rm: "https://localhost:4444"
    }
  };
  assert.equal(service.get("timeline"), "http://localhost:3333");
  assert.equal(service.get("rm"), "http://localhost:4444");
});
