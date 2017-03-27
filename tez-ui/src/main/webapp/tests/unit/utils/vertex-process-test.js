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

import VertexProcess from '../../../utils/vertex-process';
import { module, test } from 'qunit';

import Ember from 'ember';

module('Unit | Utility | vertex process');

test('Basic creation test', function(assert) {
  let process = VertexProcess.create();

  assert.ok(process);

  assert.ok(process.name);
  assert.ok(process.completeTime);
  assert.ok(process.blockingEventName);

  assert.ok(process.events);
  assert.ok(process.eventBars);
  assert.ok(process.unblockDetails);

  assert.ok(process.eventsHash);
  assert.ok(process.getTooltipContents);

  assert.ok(process.consolidateStartTime);
  assert.ok(process.consolidateEndTime);
  assert.ok(process.getConsolidateColor);
});

test('unblockDetails test', function(assert) {
  var process = VertexProcess.create(),
      testEdge2 = {}, testEdge3 = {}, testEdge4 = {};
  assert.equal(process.get("unblockDetails"), undefined);

  process = VertexProcess.create({
    blockers: [VertexProcess.create({
      vertex: {
        name: "v1",
        endTime: 10
      }
    }), VertexProcess.create({
      vertex: {
        name: "v2",
        endTime: 15
      }
    }), VertexProcess.create({
      vertex: {
        name: "v3",
        endTime: 20
      }
    })]
  });
  process.get("edgeHash").setProperties({
    v2: testEdge2,
    v3: testEdge3,
    v4: testEdge4
  });

  assert.equal(process.get("unblockDetails.edge"), testEdge3);
  assert.equal(process.get("unblockDetails.time"), 20);

  process.blockers[2].set("vertex", Ember.Object.create({
    name: "v4",
    endTime: 12
  }));
  assert.equal(process.get("unblockDetails.edge"), testEdge2);
  assert.equal(process.get("unblockDetails.time"), 15);

  process.blockers[2].vertex.set("endTime", 25);
  assert.equal(process.get("unblockDetails.edge"), testEdge4);
  assert.equal(process.get("unblockDetails.time"), 25);
});

test('events test', function(assert) {
  var process = VertexProcess.create({
    vertex: Ember.Object.create({
      name: "v1",
      events: [{
        eventtype: "testEvent1"
        //No timestamp, will be removed
      },{
        eventtype: "testEvent2",
        timestamp: 10
      }],
      initTime: 20,
      startTime: 30,
      firstTaskStartTime: 40,
      lastTaskFinishTime: 50,
      endTime: 60
    })
  });

  assert.equal(process.get("events.length"), 6);

  assert.equal(process.get("events.0.name"), "testEvent2");
  assert.equal(process.get("events.1.name"), "VERTEX_INITIALIZED");
  assert.equal(process.get("events.2.name"), "VERTEX_STARTED");
  assert.equal(process.get("events.3.name"), "FIRST_TASK_STARTED");
  assert.equal(process.get("events.4.name"), "LAST_TASK_FINISHED");
  assert.equal(process.get("events.5.name"), "VERTEX_FINISHED");

  assert.equal(process.get("events.0.time"), 10);
  assert.equal(process.get("events.1.time"), 20);
  assert.equal(process.get("events.2.time"), 30);
  assert.equal(process.get("events.3.time"), 40);
  assert.equal(process.get("events.4.time"), 50);
  assert.equal(process.get("events.5.time"), 60);

  // unblockTime < firstTaskStartTime, and we don't consider as a relevant event
  process.set("blockers", [VertexProcess.create({
    vertex: Ember.Object.create({
      name: "v2",
      endTime: 30
    })
  })]);
  assert.equal(process.get("events.length"), 6);

  process.set("blockers", [VertexProcess.create({
    vertex: Ember.Object.create({
      name: "v3",
      endTime: 55
    })
  })]);

  assert.equal(process.get("events.length"), 7);
  assert.equal(process.get("events.6.name"), "DEPENDENT_VERTICES_COMPLETE");
  assert.equal(process.get("events.6.time"), 55);
});

test('getTooltipContents-event test', function(assert) {
  var process = VertexProcess.create(),
      eventTooltip = process.getTooltipContents("event", {
    events: [{
      name: "TestEventName1",
      time: 10
    }, {
      name: "TestEventName2",
      time: 20,
      info: {
        inf1: "val1",
        inf2: 30
      }
    }, {
      name: "TestEventName3",
      time: 40,
      edge: {
        edgeId: "221296172",
        inputVertexName: "Map 4",
        outputVertexName: "Map 1",
        dataMovementType: "BROADCAST",
        dataSourceType: "PERSISTED",
        schedulingType: "SEQUENTIAL",
        edgeSourceClass: "org.apache.tez.runtime.library.output.UnorderedKVOutput",
        edgeDestinationClass: "org.apache.tez.runtime.library.input.UnorderedKVInput"
      }
    }]
  });

  assert.equal(eventTooltip.length, 4);

  assert.equal(eventTooltip[0].title, "TestEventName1");
  assert.equal(eventTooltip[0].properties.length, 1);
  assert.equal(eventTooltip[0].properties[0].name, "Time");
  assert.equal(eventTooltip[0].properties[0].value, 10);
  assert.equal(eventTooltip[0].properties[0].type, "date");

  assert.equal(eventTooltip[1].title, "TestEventName2");
  assert.equal(eventTooltip[1].properties.length, 3);
  assert.equal(eventTooltip[1].properties[0].name, "Time");
  assert.equal(eventTooltip[1].properties[0].value, 20);
  assert.equal(eventTooltip[1].properties[0].type, "date");

  assert.equal(eventTooltip[1].properties[1].name, "inf1");
  assert.equal(eventTooltip[1].properties[1].value, "val1");
  assert.equal(eventTooltip[1].properties[1].type, undefined);

  assert.equal(eventTooltip[1].properties[2].name, "inf2");
  assert.equal(eventTooltip[1].properties[2].value, 30);
  assert.equal(eventTooltip[1].properties[2].type, "number");

  assert.equal(eventTooltip[2].title, "TestEventName3");
  assert.equal(eventTooltip[2].properties.length, 1);
  assert.equal(eventTooltip[2].properties[0].name, "Time");
  assert.equal(eventTooltip[2].properties[0].value, 40);
  assert.equal(eventTooltip[2].properties[0].type, "date");

  assert.equal(eventTooltip[3].title, "Edge From Final Dependent Vertex");
  assert.equal(eventTooltip[3].properties.length, 7);
  assert.equal(eventTooltip[3].properties[0].name, "Input Vertex");
  assert.equal(eventTooltip[3].properties[0].value, "Map 4");
  assert.equal(eventTooltip[3].properties[1].name, "Output Vertex");
  assert.equal(eventTooltip[3].properties[1].value, "Map 1");
  assert.equal(eventTooltip[3].properties[2].name, "Data Movement");
  assert.equal(eventTooltip[3].properties[2].value, "BROADCAST");
  assert.equal(eventTooltip[3].properties[3].name, "Data Source");
  assert.equal(eventTooltip[3].properties[3].value, "PERSISTED");
  assert.equal(eventTooltip[3].properties[4].name, "Scheduling");
  assert.equal(eventTooltip[3].properties[4].value, "SEQUENTIAL");
  assert.equal(eventTooltip[3].properties[5].name, "Source Class");
  assert.equal(eventTooltip[3].properties[5].value, "UnorderedKVOutput");
  assert.equal(eventTooltip[3].properties[6].name, "Destination Class");
  assert.equal(eventTooltip[3].properties[6].value, "UnorderedKVInput");
});

test('getTooltipContents-process test', function(assert) {
  function getCellContent(row) {
    return row.get(this.get("contentPath"));
  }
  var process = VertexProcess.create({
    name: "TestName",
    vertex: Ember.Object.create({
      prop1: "val1",
      prop2: "val2",
      prop3: "val3"
    }),
    getVisibleProps: function () {
      return [Ember.Object.create({
        id: "prop1",
        headerTitle: "Prop 1",
        contentPath: "prop1",
        getCellContent: getCellContent,
        cellDefinition: {
          type: "Type1",
          format: "Format1"
        }
      }), Ember.Object.create({
        id: "prop2",
        headerTitle: "Prop 2",
        contentPath: "prop2",
        getCellContent: getCellContent
      })];
    }
  });

  var processTooltip = process.getTooltipContents("event-bar")[0];
  assert.equal(processTooltip.title, "TestName");
  assert.equal(processTooltip.properties[0].name, "Prop 1");
  assert.equal(processTooltip.properties[0].value, "val1");
  assert.equal(processTooltip.properties[0].type, "Type1");
  assert.equal(processTooltip.properties[0].format, "Format1");
  assert.equal(processTooltip.properties[1].name, "Prop 2");

  processTooltip = process.getTooltipContents("process-line")[0];
  assert.equal(processTooltip.title, "TestName");
  assert.equal(processTooltip.properties[0].name, "Prop 1");
  assert.equal(processTooltip.properties[0].value, "val1");
  assert.equal(processTooltip.properties[0].type, "Type1");
  assert.equal(processTooltip.properties[0].format, "Format1");
  assert.equal(processTooltip.properties[1].name, "Prop 2");

  processTooltip = process.getTooltipContents("consolidated-process", {
    contribution: 10
  })[0];
  assert.equal(processTooltip.title, "TestName");
  assert.equal(processTooltip.description, "Contribution 10%");

});
