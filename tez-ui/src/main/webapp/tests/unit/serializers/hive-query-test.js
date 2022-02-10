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

import EmberObject from '@ember/object';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Serializer | hive query', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let serializer = this.owner.lookup('serializer:hive-query');
    assert.equal(Object.keys(serializer.get("maps")).length, 23 + 6); // 23 own and 6 inherited
    assert.ok(serializer.get("extractAttributes"));
  });

  test('getStatus test', function(assert) {
    let serializer = this.owner.lookup('serializer:hive-query'),
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
    let serializer = this.owner.lookup('serializer:hive-query'),
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
    let serializer = this.owner.lookup('serializer:hive-query'),
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

    serializer.extractAttributes(EmberObject.create({
      eachAttribute() {}
    }), {
      data: testData
    });
    assert.deepEqual(testData.otherinfo.QUERY, testQuery);

    //CLIENT_IP_ADDRESS set
    assert.equal(testHiveAddress, testData.otherinfo.CLIENT_IP_ADDRESS);

    // Tables read & tables written
    assert.ok(testData.primaryfilters);
    assert.ok(testData.primaryfilters.tablesread instanceof Error);
    assert.ok(testData.primaryfilters.tableswritten instanceof Error);
    assert.equal(testData.primaryfilters.tablesread.message, "None");
    assert.equal(testData.primaryfilters.tableswritten.message, "None");
  });
});
