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
import moment from 'moment';

const DEFAULT_MARK_COUNT = 10;

export default Ember.Component.extend({

  zoom: null,
  processor: null,
  scroll: 0,

  classNames: ["em-swimlane-ruler"],

  markDef: Ember.computed("processor.timeWindow", "zoom", function () {
    var markCount = parseInt(DEFAULT_MARK_COUNT * this.get("zoom") / 100),
        timeWindow = this.get("processor.timeWindow"),
        duration = moment.duration(parseInt(timeWindow / markCount)),

        markUnit = "Milliseconds",
        markBaseValue = 0,
        markWindow = 0,
        styleWidth = 0;

    if(markBaseValue = duration.years()) {
      markUnit = "Years";
    }
    else if(markBaseValue = duration.months()) {
      markUnit = "Months";
    }
    else if(markBaseValue = duration.days()) {
      markUnit = "Days";
    }
    else if(markBaseValue = duration.hours()) {
      markUnit = "Hours";
    }
    else if(markBaseValue = duration.minutes()) {
      markUnit = "Minutes";
    }
    else if(markBaseValue = duration.seconds()) {
      markUnit = "Seconds";
    }
    else {
      markBaseValue = duration.milliseconds();
    }

    if(markBaseValue > 10) {
      markBaseValue = Math.floor(markBaseValue / 10) * 10;
    }

    markWindow = moment.duration(markBaseValue, markUnit.toLowerCase()).asMilliseconds();
    styleWidth = markWindow / timeWindow * 100;

    return {
      unit: markUnit,
      baseValue: markBaseValue,
      style: Ember.String.htmlSafe(`width: ${styleWidth}%;`),
      count: parseInt(100 / styleWidth * 1.1)
    };
  }),

  unitTextStyle: Ember.computed("scroll", function () {
    var scroll = this.get("scroll");
    return Ember.String.htmlSafe(`left: ${scroll}px;`);
  }),

  marks: Ember.computed("processor.timeWindow", "markDef", function () {
    var def = this.get("markDef"),
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
