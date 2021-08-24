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

import fmts from '../../../utils/formatters';
import { module, test } from 'qunit';

import Ember from 'ember';

module('Unit | Utility | formatters');

test('Formatter functions created', function(assert) {
  assert.ok(fmts);

  assert.ok(fmts.date);
  assert.ok(fmts.duration);
  assert.ok(fmts.number);
  assert.ok(fmts.memory);
});

test('duration', function(assert) {
  var options = {
    format: "long"
  };
  assert.equal(fmts.duration(0, options), "0 millisecond");
  assert.equal(fmts.duration(1, options), "1 millisecond");
  assert.equal(fmts.duration(60, options), "60 milliseconds");
  assert.equal(fmts.duration(6000, options), "6 seconds");
  assert.equal(fmts.duration(66000, options), "1 minute 6 seconds");
  assert.equal(fmts.duration(666000, options), "11 minutes 6 seconds");
  assert.equal(fmts.duration(6666000, options), "1 hour 51 minutes 6 seconds");
  assert.equal(fmts.duration(66666000, options), "18 hours 31 minutes 6 seconds");

  options = {
    format: "short"
  }; // By default format = short
  assert.equal(fmts.duration(0, options), "0 msec");
  assert.equal(fmts.duration(60, options), "60 msecs");
  assert.equal(fmts.duration(6000, options), "6 secs");
  assert.equal(fmts.duration(66000, options), "1 min 6 secs");
  assert.equal(fmts.duration(666000, options), "11 mins 6 secs");
  assert.equal(fmts.duration(6666000, options), "1 hr 51 mins 6 secs");
  assert.equal(fmts.duration(66666000, options), "18 hrs 31 mins 6 secs");

  assert.equal(fmts.duration(60.4, options), "60 msecs");
  assert.equal(fmts.duration(60.6, options), "61 msecs");

  options = {}; // By default format = xshort
  assert.equal(fmts.duration(0, options), "0ms");
  assert.equal(fmts.duration(60, options), "60ms");
  assert.equal(fmts.duration(6000, options), "6s");
  assert.equal(fmts.duration(66000, options), "1m 6s");
  assert.equal(fmts.duration(666000, options), "11m 6s");
  assert.equal(fmts.duration(6666000, options), "1h 51m 6s");
  assert.equal(fmts.duration(66666000, options), "18h 31m 6s");
});

test('number', function(assert) {
  assert.equal(fmts.number(6000, {}), "6,000");
  assert.equal(fmts.number(6000000, {}), "6,000,000");
});

test('memory', function(assert) {
  assert.equal(fmts.memory(0, {}), "0 B");
  assert.equal(fmts.memory(600, {}), "600 B");
  assert.equal(fmts.memory(1024, {}), "1 KB");
  assert.equal(fmts.memory(1024 * 1024, {}), "1 MB");
  assert.equal(fmts.memory(1024 * 1024 * 1024, {}), "1 GB");
  assert.equal(fmts.memory(1024 * 1024 * 1024 * 1024, {}), "1 TB");
});

test('json', function(assert) {
  var str = "testString",
      complexObj = Ember.Object.create();

  assert.equal(fmts.json(str, {}), str);
  assert.equal(fmts.json(complexObj, {}), complexObj);

  assert.equal(fmts.json(null, {}), null);
  assert.equal(fmts.json(undefined, {}), undefined);

  assert.equal(fmts.json({x: 1}, {}), '{\n    "x": 1\n}');
  assert.equal(fmts.json({x: 1, y: 2}, {space: 1}), '{\n "x": 1,\n "y": 2\n}');
  assert.equal(fmts.json({x: 1, y: {z: 3}}, {space: 1}), '{\n "x": 1,\n "y": {\n  "z": 3\n }\n}');
});
