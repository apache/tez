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

var jsonToDagMap = {
	id: 'entity',
	startTime: 'otherinfo.startTime', //TODO: what is the difference between this and the one in the otherinfo
	endTime: 'otherinfo.endTime',
	name: 'primaryfilters.dagName',
	user: 'primaryfilters.user',
	applicationId: 'otherinfo.applicationId',
	status: 'otherinfo.status',
	diagnostics: 'otherinfo.diagnostics'
};

App.Mappers.Dag = {
	mapSingle : function(json) {
		return Em.JsonMapper.map(json, jsonToDagMap);
	},

	mapMany : function(json) {
		if (Array.isArray(json.entities)) {
			return json.entities.map(this.mapSingle);
		}

		return [];
	}
}

App.Mappers.Vertex = {
	mapSingle: function(json) {
		return Em.JsonMapper.map(json, jsonToVertexMap);
	},

	mapMany: function(json) {
		if (Array.isArray(json.entities)) {
			return json.entities.map(this.mapSingle);
		}

		return [];
	}
}