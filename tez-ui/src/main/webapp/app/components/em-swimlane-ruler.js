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

import Component from '@ember/component';
import { computed } from '@ember/object';
import { htmlSafe } from '@ember/template';
import {  intervalToDuration } from 'date-fns'

const DEFAULT_MARK_COUNT = 10;

export default Component.extend({

  zoom: 100,
  processor: null,
  scroll: 0,

  classNames: ["em-swimlane-ruler"],

  markDef: computed("processor.{startTime,timeWindow}", "zoom", function () {
    var markCount = parseInt(DEFAULT_MARK_COUNT * this.zoom / 100),
      startTime = this.get('processor.startTime') || 0,
      timeWindow = this.get("processor.timeWindow") || 0,
      // How much time in millis does 1/10 of the visible ruler represent
      tenthVisibleDuration = parseInt(timeWindow / markCount),
      tenthVisibleEnd = startTime + tenthVisibleDuration,
      // Do duration calculation from start time so months and years are correct
      duration = intervalToDuration({start: startTime, end: tenthVisibleEnd}),

      // largest positive time unit
      markUnit = "Milliseconds",
      markBaseValue = 0,
      markWindow = 0,
      styleWidth = 0;

    let approxMillis = {
      Years: 365 * 30 * 24 * 60 * 60 * 1000,
      Months: 30 * 24 * 60 * 60 * 1000,
      Days: 24 * 60 * 60 * 1000,
      Hours: 60 * 60 * 1000,
      Minutes: 60 * 1000,
      Seconds: 1000,
      Milliseconds: 1
    }

    if(markBaseValue = duration.years) {
      markUnit = "Years";
    }
    else if(markBaseValue = duration.months) {
      markUnit = "Months";
    }
    else if(markBaseValue = duration.days) {
      markUnit = "Days";
    }
    else if(markBaseValue = duration.hours) {
      markUnit = "Hours";
    }
    else if(markBaseValue = duration.minutes) {
      markUnit = "Minutes";
    }
    else if(markBaseValue = duration.seconds) {
      markUnit = "Seconds";
    }
    else {
      // durations millis
      markBaseValue = tenthVisibleDuration % 1000
    }

    // Floor to nearest divisible of 10 (19 -> 10)
    if(markBaseValue > 10) {
      markBaseValue = Math.floor(markBaseValue / 10) * 10;
    }

    markWindow = markBaseValue * approxMillis[markUnit];
    styleWidth = markWindow / timeWindow * 100;

    return {
      unit: markUnit,
      baseValue: markBaseValue,
      style: htmlSafe(`width: ${styleWidth}%;`),
      count: parseInt(100 / styleWidth * 1.1)
    };
  }),

  unitTextStyle: computed("scroll", function () {
    var scroll = this.scroll;
    return htmlSafe(`left: ${scroll}px;`);
  }),

  marks: computed("processor.timeWindow", "markDef", function () {
    var def = this.markDef,
        baseValue = def.baseValue,
        marks = [];

    for(var i=0, count = def.count; i < count; i++) {
      marks.push({
        duration: parseInt(baseValue * i)
      });
    }

    return marks;
  })

});
