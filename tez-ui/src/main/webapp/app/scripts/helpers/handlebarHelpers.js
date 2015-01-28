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

/**
   * formats the given unix timestamp. returns NaN if its not a number.
   *
   * @param {number} unixtimestamp 
   * @returns {string} 
   * @method formatUnixTimestamp
   */
Em.Handlebars.helper('formatUnixTimestamp', function(timestamp) {
	if (!App.Helpers.number.isValidInt(timestamp)) return 'NaN';
	if (timestamp > 0) {
		return App.Helpers.date.dateFormat(timestamp);
	}
	return '';
});

/**
 * Format value with US style thousands separator
 * @param {string/number} value to be formatted
 * @returns {string} Formatted string
 */
Em.Handlebars.helper('formatNumThousands', function (value) {
  return App.Helpers.number.formatNumThousands(value);
});

/*
 * formats the duration.
 *
 * @param {duration} duration in milliseconds
 * @return {Number}
 * @method formatDuration
 */
Em.Handlebars.helper('formatDuration', function(startTime, endTime) {
	// unixtimestamp is in seconds. javascript expects milliseconds.
	if (endTime < startTime || !!endTime) {
		end = new Date().getTime();
	}

	return App.Helpers.date.durationSummary(startTime, endTime);
});

Em.Handlebars.helper('formatTimeMillis', function(duration) {
  return App.Helpers.date.timingFormat(duration, true);
});

function replaceAll(str, str1, str2, ignore) 
{
    return str.replace(new RegExp(str1.replace(/([\/\,\!\\\^\$\{\}\[\]\(\)\.\*\+\?\|\<\>\-\&])/g,"\\$&"),(ignore?"gi":"g")),(typeof(str2)=="string")?str2.replace(/\$/g,"$$$$"):str2);
} 

//TODO: needs better indendation.
Em.Handlebars.helper('formatDiagnostics', function(diagnostics) {
  var x = replaceAll(diagnostics, '[', '<div class="indent"><i>&nbsp;</i>');
  x = replaceAll(x, '],', '</div><i>&nbsp;</i>');
  x = replaceAll(x, ']', '</div>');
  return new Handlebars.SafeString(x);
});
