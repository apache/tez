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