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

moduleFor('serializer:hive-query', 'Unit | Serializer | hive query', {
  // Specify the other units that are required for this test.
  needs: ['model:hive-query']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();
  assert.equal(Object.keys(serializer.get("maps")).length, 6 + 20);
  assert.ok(serializer.get("extractAttributes"));
});

test('getStatus test', function(assert) {
  let serializer = this.subject(),
      getStatus = serializer.get("maps.status");

  assert.equal(getStatus({}), "RUNNING");
  assert.equal(getStatus({
    otherinfo: {
      STATUS: true
    }
  }), "SUCCEEDED");
  assert.equal(getStatus({
    otherinfo: {
      STATUS: false
    }
  }), "FAILED");
});

test('getEndTime test', function(assert) {
  let serializer = this.subject(),
      getEndTime = serializer.get("maps.endTime"),
      endTime = 23;

  assert.equal(getEndTime({}), undefined);

  assert.equal(getEndTime({
    otherinfo: {
      endTime: endTime
    }
  }), endTime);

  assert.equal(getEndTime({
    events: [{
      eventtype: 'X',
    }, {
      eventtype: 'QUERY_COMPLETED',
      timestamp: endTime
    }, {
      eventtype: 'Y',
    }]
  }), endTime);
});

test('extractAttributes test', function(assert) {
  let serializer = this.subject(),
      testQuery = {
        abc: 1,
        xyz: 2
      },
      testHiveAddress = "1.2.3.4",
      testData = {
        otherinfo: {
          QUERY: JSON.stringify(testQuery),
          HIVE_ADDRESS: testHiveAddress
        }
      };

  serializer.extractAttributes(Ember.Object.create({
    eachAttribute: Ember.K
  }), {
    data: testData
  });
  assert.deepEqual(testData.otherinfo.QUERY, testQuery);

  assert.equal(testHiveAddress, testData.otherinfo.CLIENT_IP_ADDRESS);
});
