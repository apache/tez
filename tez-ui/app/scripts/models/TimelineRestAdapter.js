var typeToPathMap = {
	dag: 'TEZ_DAG_ID',
	vertex: 'TEZ_VERTEX_ID',
	taskAttempt: 'TEZ_TASK_ATTEMPT_ID',
};

App.TimelineRESTAdapter = DS.RESTAdapter.extend({
	host: App.AtsBaseUrl,
	namespace: 'ws/v1/timeline',

	pathForType: function(type) {
		return typeToPathMap[type];
	},

});

App.TimelineSerializer = DS.RESTSerializer.extend({
	primaryKey: 'entity',

	attrs: {
		key: 'entity',
	},

	extractSingle: function(store, primaryType, rawPayload, recordId) {
		// rest serializer expects singular form of model as the root key.
		var payload = {};
		payload[primaryType.typeKey] = rawPayload;
		return this._super(store, primaryType, payload, recordId);
	},

	extractArray: function(store, primaryType, rawPayload) {
		// restserializer expects a plural of the model but TimelineServer returns
		// it in entities.
		var payload = {};
		payload[primaryType.typeKey.pluralize()] = rawPayload.entities;
		return this._super(store, primaryType, payload);
	},

});


var timelineJsonToDagMap = {
  id: 'entity',
  startTime: 'otherinfo.startTime',
  endTime: 'otherinfo.endTime',
  name: 'primaryfilters.dagName.0',
  user: 'primaryfilters.user.0',
  applicationId: 'otherinfo.applicationId',
  status: 'otherinfo.status',
  diagnostics: 'otherinfo.diagnostics',
  counterGroups: 'counterGroups'
};


App.DagSerializer = App.TimelineSerializer.extend({
  normalizePayload: function(rawPayload){
    // no normalization required for multiple case (findAll)
    if (!!rawPayload.dags) {
      return rawPayload;
    }

    var dag = rawPayload.dag;
    var counterGroups = dag.otherinfo.counters.counterGroups;
    delete dag.otherinfo.counters;

    // todo move counter parsing outside
    // flatten the dag -> countergroup -> counter structure
    var dagID = dag.entity;
    var allCounters = [];

    // put counter group id in the list
    dag['counterGroups'] = counterGroups.map(function(cg) {
      cg.entity = dagID + '/' + cg.counterGroupName;
      cg.parentID = {
        type: 'dag',
        id: dagID
      };

      // replace the counters with thier id.
      cg.counters = cg.counters.map(function(counter){
        counter.entity = cg.entity + '/' + counter.counterName;
        counter.cgID = cg.entity;
        allCounters.push(counter);
        return counter.entity;
      });

      return cg.entity;
    });

    var normalizedPayload = {
      'dag': dag,
      'counterGroups': counterGroups,
      'counters': allCounters
    };
    return normalizedPayload;
  },

  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToDagMap);
  },
});

App.CounterGroupSerializer = DS.JSONSerializer.extend({
  normalize: function(type, hash, prop) {
    return {
      'id': hash.entity,
      'name': hash.counterGroupName,
      'displayName': hash.counterGroupDisplayName,
      'counters': hash.counters,
      'parent': hash.parentID
    };
  }
});

App.CounterSerializer = DS.JSONSerializer.extend({
  normalize: function(type, hash, prop) {
    return {
      'id': hash.entity,
      'name': hash.counterName,
      'displayName': hash.counterDisplayName,
      'value': hash.counterValue,
      'parent': hash.cgID
    };
  }
});

var timelineJsonToTaskAttemptMap = {
  id: 'entity',
  startTime: 'otherinfo.startTime',
  endTime: 'otherinfo.endTime',
  dagId: 'primaryfilters.dagId',
  containerId: 'otherinfo.containerId',
  status: 'otherinfo.status',
};


App.TaskAttemptSerializer = App.TimelineSerializer.extend({
  normalizePayload: function(rawPayload){

    // TODO - fake a containerId until it is present
    var taskAttempts = rawPayload.taskAttempts;
    for (var i = 0; i < taskAttempts.length; ++i) {
      var taskAttempt = taskAttempts[i];
      var inProgressLogsURL = taskAttempt.otherinfo.inProgressLogsURL;
      var regex = /.*(container_.*?)\/.*/;
      var match = regex.exec(inProgressLogsURL);
      taskAttempt.otherinfo.containerId = match[1];
      taskAttempt.primaryfilters.dagId = taskAttempt.primaryfilters.TEZ_DAG_ID[0];
    }
    return rawPayload;
  },

  normalize: function(type, hash, prop) {
    var post = Em.JsonMapper.map(hash, timelineJsonToTaskAttemptMap);
    return post;
  },
});
