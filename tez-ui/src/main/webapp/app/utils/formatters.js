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
import numeral from 'numeral';

const DEFAULT_DATE_TIMEZONE = "UTC",
      DEFAULT_DATE_FORMAT = "DD MMM YYYY HH:mm:ss",
      DEFAULT_NUM_FORMAT = '0,0',
      DEFAULT_MEM_FORMAT = '0 b';

function durationFormatter(arr, value, unit) {
  if(value > 0) {
    if(value > 1) {
      unit += 's';
    }
    arr.push(value + unit);
  }
}

const DURATION_FORMATS = {
  long: {
    collateFunction: durationFormatter,

    year: " year",
    month: " month",
    day: " day",
    hour: " hour",
    minute: " minute",
    second: " second",
    millisecond: " millisecond"
  },
  short: {
    collateFunction: durationFormatter,

    year: " yr",
    month: " mo",
    day: " day",
    hour: " hr",
    minute: " min",
    second: " sec",
    millisecond: " msec"
  },
  xshort: {
    collateFunction: function (arr, value, unit) {
      if(value > 0) {
        arr.push(value + unit);
      }
    },

    year: "Y",
    month: "M",
    day: "D",
    hour: "h",
    minute: "m",
    second: "s",
    millisecond: "ms"
  }
};

function validateNumber(value, message) {
  value = parseFloat(value);

  if(isNaN(value)) {
    throw new Error(message || "Invalid number!");
  }

  return value;
}

export default Ember.Controller.create({
  date: function (value, options) {
    var date = moment.tz(value, options.valueFormat, options.valueTimeZone || DEFAULT_DATE_TIMEZONE);

    date = options.timeZone ? date.tz(options.timeZone) : date.local();
    date = date.format(options.format || DEFAULT_DATE_FORMAT);

    if(date === "Invalid date") {
      throw new Error(date);
    }

    return date;
  },
  duration: function (value, options) {
    var format = DURATION_FORMATS[options.format || "xshort"],
        duration,
        ret = [];

    value = validateNumber(value, "Invalid duration");

    if(value === 0) {
      return `0${format.millisecond}`;
    }

    duration = moment.duration(value, options.valueUnit);

    format.collateFunction(ret, duration.years(), format.year);
    format.collateFunction(ret, duration.months(), format.month);
    format.collateFunction(ret, duration.days(), format.day);
    format.collateFunction(ret, duration.hours(), format.hour);
    format.collateFunction(ret, duration.minutes(), format.minute);
    format.collateFunction(ret, duration.seconds(), format.second);
    format.collateFunction(ret, Math.round(duration.milliseconds()), format.millisecond);

    return ret.join(" ");
  },
  number: function (value, options) {
    value = validateNumber(value);
    return numeral(value).format(options.format || DEFAULT_NUM_FORMAT);
  },
  memory: function (value) {
    value = validateNumber(value, "Invalid memory");
    if(value === 0) {
      return "0 B";
    }
    return numeral(value).format(DEFAULT_MEM_FORMAT);
  },
  json: function (value, options) {
    if(value && typeof value === "object" && value.constructor === Object) {
      try {
        value = JSON.stringify(value, options.replacer, options.space || 4);
      }
      catch(err){
        Ember.Logger.error(err);
      }
    }
    return value;
  }
});
