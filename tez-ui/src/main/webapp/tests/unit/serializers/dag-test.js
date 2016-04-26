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

moduleFor('serializer:dag', 'Unit | Serializer | dag', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:dag']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer.maps.atsStatus);
  assert.ok(serializer.maps.startTime);
  assert.ok(serializer.maps.endTime);
  assert.ok(serializer.maps.containerLogs);
  assert.ok(serializer.maps.vertexIdNameMap);
});

test('atsStatus test', function(assert) {
  let serializer = this.subject(),
      mapper = serializer.maps.atsStatus;

  assert.equal(mapper({
    events: [{eventtype: "SOME_EVENT"}]
  }), undefined);

  assert.equal(mapper({
    events: [{eventtype: "DAG_STARTED"}]
  }), "RUNNING");

  assert.equal(mapper({
    otherinfo: {status: "STATUS1"},
    primaryfilters: {status: ["STATUS2"]},
    events: [{eventtype: "DAG_STARTED"}]
  }), "STATUS1");

  assert.equal(mapper({
    primaryfilters: {status: ["STATUS2"]},
    events: [{eventtype: "DAG_STARTED"}]
  }), "STATUS2");
});

test('startTime test', function(assert) {
  let serializer = this.subject(),
      mapper = serializer.maps.startTime,
      testTimestamp = Date.now();

  assert.equal(mapper({
    events: [{eventtype: "SOME_EVENT"}]
  }), undefined);

  assert.equal(mapper({
    events: [{eventtype: "DAG_STARTED", timestamp: testTimestamp}]
  }), testTimestamp);

  assert.equal(mapper({
    otherinfo: {startTime: testTimestamp},
    events: [{eventtype: "DAG_STARTED"}]
  }), testTimestamp);
});

test('endTime test', function(assert) {
  let serializer = this.subject(),
      mapper = serializer.maps.endTime,
      testTimestamp = Date.now();

  assert.equal(mapper({
    events: [{eventtype: "SOME_EVENT"}]
  }), undefined);

  assert.equal(mapper({
    events: [{eventtype: "DAG_FINISHED", timestamp: testTimestamp}]
  }), testTimestamp);

  assert.equal(mapper({
    otherinfo: {endTime: testTimestamp},
    events: [{eventtype: "DAG_FINISHED"}]
  }), testTimestamp);
});

test('containerLogs test', function(assert) {
  let serializer = this.subject(),
      mapper = serializer.maps.containerLogs;

  assert.deepEqual(mapper({
    otherinfo: {},
  }), [], "No logs");

  assert.deepEqual(mapper({
    otherinfo: {inProgressLogsURL_1: "foo", inProgressLogsURL_2: "bar"},
  }), [{text: "1", href: "http://foo"}, {text: "2", href: "http://bar"}], "2 logs");
});

test('vertexIdNameMap test', function(assert) {
  let serializer = this.subject(),
      mapper = serializer.maps.vertexIdNameMap;

  let nameIdMap = {
    otherinfo: {
      vertexNameIdMapping: {
        name1: "ID1",
        name2: "ID2",
        name3: "ID3",
      }
    }
  };

  assert.deepEqual(mapper(nameIdMap), {
    ID1: "name1",
    ID2: "name2",
    ID3: "name3",
  });
});
