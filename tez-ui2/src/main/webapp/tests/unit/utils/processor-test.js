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

import Processor from '../../../utils/processor';
import { module, test } from 'qunit';

module('Unit | Utility | processor');

test('Basic creation test', function(assert) {
  let processor = Processor.create();

  assert.ok(processor);

  assert.ok(processor.timeWindow);
  assert.ok(processor.createProcessColor);
  assert.ok(processor.timeToPositionPercent);
});

test('timeWindow test', function(assert) {
  let processor = Processor.create({
    startTime: 50,
    endTime: 80
  });

  assert.equal(processor.get("timeWindow"), 30);

  processor = Processor.create({
    startTime: 80,
    endTime: 50
  });

  assert.equal(processor.get("timeWindow"), 0);
});

test('timeWindow test', function(assert) {
  let processor = Processor.create({
    processCount: 10
  }),
  color = processor.createProcessColor(3);

  assert.equal(color.h, 108);
  assert.equal(color.s, 70);
  assert.equal(color.l, 40);
});

test('timeToPositionPercent test', function(assert) {
  let processor = Processor.create({
    startTime: 0,
    endTime: 10
  });

  assert.equal(processor.timeToPositionPercent(5), 50);
});
