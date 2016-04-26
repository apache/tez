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

import CounterColumnDefinition from '../../../utils/counter-column-definition';
import { module, test } from 'qunit';

module('Unit | Utility | counter column definition');

test('Basic creation test', function(assert) {
  let definition = CounterColumnDefinition.create();

  assert.ok(definition);

  assert.ok(definition.getCellContent);
  assert.ok(definition.getSearchValue);
  assert.ok(definition.getSortValue);

  assert.ok(definition.id);
  assert.ok(definition.groupDisplayName);
  assert.ok(definition.headerTitle);

  assert.ok(CounterColumnDefinition.make);

  assert.equal(definition.observePath, true);
  assert.equal(definition.contentPath, "counterGroupsHash");
});

test('getCellContent, getSearchValue & getSortValue test', function(assert) {
  let testGroupName = "t.gn",
      testCounterName = "cn",
      testCounterValue = "val",
      testContent = {},
      testRow = {
        counterGroupsHash: testContent
      };

  testContent[testGroupName] = {};
  testContent[testGroupName][testCounterName] = testCounterValue;
  testContent[testGroupName]["anotherName"] = "anotherValue";

  let definition = CounterColumnDefinition.create({
    counterGroupName: testGroupName,
    counterName: testCounterName,
  });

  assert.equal(definition.getCellContent(testRow), testCounterValue);
  assert.equal(definition.getSearchValue(testRow), testCounterValue);
  assert.equal(definition.getSortValue(testRow), testCounterValue);
});

test('id test', function(assert) {
  let testGroupName = "t.gn",
      testCounterName = "cn";

  let definition = CounterColumnDefinition.create({
    counterGroupName: testGroupName,
    counterName: testCounterName,
  });

  assert.equal(definition.get("id"), `${testGroupName}/${testCounterName}`);
});

test('groupDisplayName test', function(assert) {
  let definition = CounterColumnDefinition.create();

  definition.set("counterGroupName", "foo");
  assert.equal(definition.get("groupDisplayName"), "foo");

  definition.set("counterGroupName", "foo.bar");
  assert.equal(definition.get("groupDisplayName"), "bar");

  definition.set("counterGroupName", "org.apache.tez.common.counters.DAGCounter");
  assert.equal(definition.get("groupDisplayName"), "DAG");

  definition.set("counterGroupName", "org.apache.tez.common.counters.FileSystemCounter");
  assert.equal(definition.get("groupDisplayName"), "FileSystem");

  definition.set("counterGroupName", "TaskCounter_ireduce1_INPUT_map");
  assert.equal(definition.get("groupDisplayName"), "Task - ireduce1 to Input-map");

  definition.set("counterGroupName", "TaskCounter_ireduce1_OUTPUT_reduce");
  assert.equal(definition.get("groupDisplayName"), "Task - ireduce1 to Output-reduce");
});

test('headerTitle test', function(assert) {
  let testGroupName = "t.gn",
      testCounterName = "cn";

  let definition = CounterColumnDefinition.create({
    counterGroupName: testGroupName,
    counterName: testCounterName,
  });

  assert.equal(definition.get("headerTitle"), "gn - cn");
});

test('CounterColumnDefinition.make test', function(assert) {
  var definitions = CounterColumnDefinition.make([{
    counterGroupName: "gn1",
    counterName: "nm1",
  }, {
    counterGroupName: "gn2",
    counterName: "nm2",
  }]);

  assert.equal(definitions.length, 2);
  assert.equal(definitions[0].get("headerTitle"), "gn1 - nm1");
  assert.equal(definitions[1].get("headerTitle"), "gn2 - nm2");
});
