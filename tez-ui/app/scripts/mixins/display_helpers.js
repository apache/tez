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

// TODO : document
App.Helpers.DisplayHelper = Em.Mixin.create({
	startTimeDisplay: function() {
		var startTime = this.get('startTime');
		return startTime > 0 ? App.Helpers.date.dateFormat(startTime) : '';
	}.property('startTime'),

	endtimeDisplay: function() {
		var endTime = this.get('endTime');
		return endTime > 0 ?  App.Helpers.date.dateFormat(endTime) : '';
	}.property('endTime'),

	duration: function() {
		var startTime = this.get('startTime');
    var endTime = this.get('endTime');
    if(endTime < startTime || endTime == undefined) {
      endTime =  new Date().getTime();
    }
    return App.Helpers.date.duration(startTime, endTime);
	}.property('startTime', 'endTime')
});