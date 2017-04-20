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

moduleFor('serializer:dag-info', 'Unit | Serializer | dag info', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:dag-info']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);

  assert.ok(serializer.normalizeResourceHash);

  assert.ok(serializer.maps.dagPlan);
  assert.ok(serializer.maps.callerData);

  assert.equal(Object.keys(serializer.get("maps")).length, 2 + 7); //2 own & 7 inherited
});

test('normalizeResourceHash test', function(assert) {
  let serializer = this.subject(),

      callerInfo = {
        callerId: "id_1",
        callerType: "HIVE_QUERY_ID",
        context: "Hive",
        description: "hive query"
      },

      data;

  // dagContext test
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagContext: callerInfo
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.equal(data.callerData.callerType, callerInfo.callerType);

  // dagInfo test
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagInfo: `{"context": "${callerInfo.context}", "description": "${callerInfo.description}"}`
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.notOk(data.callerData.callerType);

  // dagInfo.blob test
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagInfo: {
            context: callerInfo.context,
            blob: callerInfo.description
          }
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.notOk(data.callerData.callerType);

  // dagContext have presidence over dagInfo
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagContext: callerInfo,
          dagInfo: `{"context": "RandomContext", "description": "RandomDesc"}`
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.equal(data.callerData.callerType, callerInfo.callerType);
});
