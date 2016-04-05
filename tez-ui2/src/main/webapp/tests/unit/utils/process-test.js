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

import Process from '../../../utils/process';
import { module, test } from 'qunit';

module('Unit | Utility | process');

test('Basic creation test', function(assert) {
  let process = Process.create();

  assert.ok(process);

  assert.ok(process.consolidateStartTime);
  assert.ok(process.consolidateEndTime);

  assert.ok(process.init);

  assert.ok(process.getBarColor);
  assert.ok(process.getConsolidateColor);

  assert.ok(process.getColor);
  assert.ok(process.startEvent);
  assert.ok(process.endEvent);
  assert.ok(process.getAllBlockers);
  assert.ok(process.getTooltipContents);
});

test('_id test', function(assert) {
  let nextID = parseInt(Process.create().get("_id").split("-")[2]) + 1;

  let process = Process.create();
  assert.equal(process.get("_id"), "process-id-" + nextID);
});


test('getColor test', function(assert) {
  let process = Process.create();

  assert.equal(process.getColor(), "#0");

  process.set("color", {
    h: 10,
    s: 20,
    l: 30
  });
  assert.equal(process.getColor(), "hsl( 10, 20%, 30% )");
  assert.equal(process.getColor(0.2), "hsl( 10, 20%, 40% )");
});

test('startEvent test', function(assert) {
  let process = Process.create();

  assert.equal(process.get("startEvent"), undefined);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 20,
  }, {
    time: 80,
  }]);
  assert.equal(process.get("startEvent").time, 20);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 80,
  }]);
  assert.equal(process.get("startEvent").time, 50);
});

test('endEvent test', function(assert) {
  let process = Process.create();

  assert.equal(process.get("endEvent"), undefined);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 20,
  }, {
    time: 80,
  }]);
  assert.equal(process.get("endEvent").time, 80);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 20,
  }]);
  assert.equal(process.get("endEvent").time, 70);
});

test('getAllBlockers test', function(assert) {
  var cyclicProcess = Process.create({
    name: "p3",
  });
  cyclicProcess.blockers = [cyclicProcess];

  var multiLevelCycle1 = Process.create({
    name: "p5",
  });
  var multiLevelCycle2 = Process.create({
    name: "p6",
  });
  multiLevelCycle1.blockers = [multiLevelCycle2];
  multiLevelCycle2.blockers = [multiLevelCycle1];

  var process = Process.create({
    blockers: [Process.create({
      name: "p1"
    }), Process.create({
      name: "p2",
      blockers: [Process.create({
        name: "p21"
      }), Process.create({
        name: "p22",
        blockers: [Process.create({
          name: "p221"
        })]
      })]
    }), cyclicProcess, Process.create({
      name: "p4"
    }), multiLevelCycle1]
  });

  var all = process.getAllBlockers();

  assert.equal(all.length, 9);

  assert.equal(all[0].get("name"), "p1");
  assert.equal(all[1].get("name"), "p2");
  assert.equal(all[2].get("name"), "p21");
  assert.equal(all[3].get("name"), "p22");
  assert.equal(all[4].get("name"), "p221");
  assert.equal(all[5].get("name"), "p3");
  assert.equal(all[6].get("name"), "p4");
  assert.equal(all[7].get("name"), "p5");
  assert.equal(all[8].get("name"), "p6");

});
