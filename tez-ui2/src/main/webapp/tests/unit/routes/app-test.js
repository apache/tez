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

moduleFor('route:app', 'Unit | Route | app', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});

test('Test model - Without app data', function(assert) {
  let testID = "123",
      route = this.subject({
        loader: {
          queryRecord: function (type, id) {
            assert.ok(type === 'AhsApp' || type === 'appRm');
            assert.equal(id, testID);
            return {
              catch: function (callBack) {
                return callBack();
              }
            };
          }
        }
      }),
      data;

  assert.expect(2 + 2 + 1);

  data = route.model({
    "app_id": testID
  });
  assert.equal(data.get("entityID"), testID);
});

test('Test model - With app data', function(assert) {
  let testID1 = "123",
      testData1 = {},
      testID2 = "456",
      testData2 = {},
      route = this.subject({
        loader: {
          queryRecord: function (type, id) {
            if(id === "123"){
              assert.equal(type, 'AhsApp');
              return {
                catch: function () {
                  return testData1;
                }
              };
            }
            else if(id === "456") {
              if(type === "AhsApp") {
                return {
                  catch: function (callBack) {
                    return callBack();
                  }
                };
              }
              assert.equal(type, 'appRm');
              return {
                catch: function () {
                  return testData2;
                }
              };
            }
          }
        }
      }),
      data;

  assert.expect(2 + 2);

  data = route.model({
    "app_id": testID1
  });
  assert.equal(data, testData1);

  data = route.model({
    "app_id": testID2
  });
  assert.equal(data, testData2);
});
