/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

App.Helpers.misc = {
  getStatusClassForEntity: function(status, hasFailedTasks) {
    if(!status) return '';

    switch(status) {
      case 'FAILED':
        return 'failed';
      case 'KILLED':
        return 'killed';
      case 'RUNNING':
        return 'running';
      case 'ERROR':
        return 'error';
      case 'SUCCEEDED':
        if (!!hasFailedTasks) {
          return 'warning';
        }
        /*
        TODO: TEZ-2113
        var counterGroups = dag.get('counterGroups');
        var numFailedTasks = this.getCounterValueForDag(counterGroups,
          dag.get('id'), 'org.apache.tez.common.counters.DAGCounter',
          'NUM_FAILED_TASKS'
        ); 

        if (numFailedTasks > 0) {
          return 'warning';
        }*/

        return 'success';
      case 'UNDEFINED':
        return 'unknown';
      case 'SCHEDULED':
        return 'schedule';
      default:
        return 'submitted';
    }
  },

  getRealStatus: function(entityState, yarnAppState, yarnAppFinalState) {
    if (entityState != 'RUNNING' || (yarnAppState != 'FINISHED' && yarnAppState != 'KILLED' && yarnAppState != 'FAILED')) {
      return entityState;
    }

    if (yarnAppState == 'KILLED' || yarnAppState == 'FAILED') {
      return yarnAppState;
    }

    return yarnAppFinalState;
  },

	getCounterValueForDag: function(counterGroups, dagID, counterGroupName, counterName) {
		if (!counterGroups) {
			return 0;
		}

		var cgName = dagID + '/' + counterGroupName;
		var cg = 	counterGroups.findBy('id', cgName);
		if (!cg) {
			return 0;
		}
		var counters = cg.get('counters');
		if (!counters) {
			return 0;
		}
		
		var counter = counters.findBy('id', cgName + '/' + counterName);
		if (!counter) return 0;

		return counter.get('value');
	},

  isValidDagStatus: function(status) {
    return $.inArray(status, ['SUBMITTED', 'INITING', 'RUNNING', 'SUCCEEDED',
      'KILLED', 'FAILED', 'ERROR']) != -1;
  },

  isFinalDagStatus: function(status) {
    return $.inArray(status, ['SUCCEEDED', 'KILLED', 'FAILED', 'ERROR']) != -1;
  },

  isValidTaskStatus: function(status) {
    return $.inArray(status, ['RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED']) != -1;
  },

  isStatusInUnsuccessful: function(status) {
    return $.inArray(status, ['FAILED', 'KILLED', 'UNDEFINED']) != -1;
  },

  /**
   * To trim a complete class path with namespace to the class name.
   */
  getClassName: function (classPath) {
    return classPath.substr(classPath.lastIndexOf('.') + 1);
  },

  /**
   * Return a normalized group name for a counter name
   * @param groupName {String}
   * @return Normlaized name
   */
  getCounterGroupDisplayName: function (groupName) {
    var displayName = App.Helpers.misc.getClassName(groupName), // Remove path
        ioParts,
        toText;

    function removeCounterFromEnd(text) {
      if(text.substr(-7) == 'Counter') {
        text = text.substr(0, text.length - 7);
      }
      return text;
    }

    displayName = removeCounterFromEnd(displayName);

    // Reformat per-io counters
    switch(App.Helpers.misc.checkIOCounterGroup(displayName)) {
      case 'in':
        ioParts = displayName.split('_INPUT_');
        toText = 'to %@ Input'.fmt(ioParts[1]);
      break;
      case 'out':
        ioParts = displayName.split('_OUTPUT_');
        toText = 'to %@ Output'.fmt(ioParts[1]);
      break;
    }
    if(ioParts) {
      ioParts = ioParts[0].split('_');
      if(ioParts.length > 1) {
        displayName = '%@ - %@ %@'.fmt(
          removeCounterFromEnd(ioParts.shift()),
          ioParts.join('_'),
          toText
        );
      }
    }

    return displayName;
  },

  /*
   * Normalizes counter style configurations
   * @param counterConfigs Array
   * @return Normalized configurations
   */
  normalizeCounterConfigs: function (counterConfigs, controller) {
    return counterConfigs.map(function (configuration) {
      var groupName = configuration.counterGroupName || configuration.groupId,
          counterName = configuration.counterName || configuration.counterId;

      configuration.headerCellName = '%@ - %@'.fmt(
        App.Helpers.misc.getCounterGroupDisplayName(groupName),
        counterName
      );
      configuration.id = '%@/%@'.fmt(groupName, counterName),

      configuration.observePath = true;
      configuration.contentPath = 'counterGroups';
      configuration.counterGroupName = groupName;
      configuration.counterName = counterName;

      if(controller) {
        configuration.onSort = controller.onInProgressColumnSort.bind(controller);
      }

      configuration.getSortValue = App.Helpers.misc.getCounterCellContent;
      configuration.getCellContent =
          configuration.getSearchValue = App.Helpers.misc.getCounterCellContentFormatted;
      return configuration;
    });
  },

  getCounterQueryParam: function (columns) {
    var counterHash = {},
        counters = [];

    columns.forEach(function (column) {
      var groupName = column.get('counterGroupName'),
          counterName = column.get('counterName');
      if(column.get('contentPath') == 'counterGroups') {
        counterHash[groupName] = counterHash[groupName] || [];
        counterHash[groupName].push(counterName);
      }
    });
    for(var groupName in counterHash) {
      counters.push('%@/%@'.fmt(groupName, counterHash[groupName].join(',')));
    }

    return counters.join(';');
  },

  /*
   * Merges counter information from AM counter object into ATS counters array
   */
  mergeCounterInfo: function (targetATSCounters, sourceAMCounters) {
    var atsCounters, atsCounter,
        counters;

    targetATSCounters = targetATSCounters || [];

    try{
      for(var counterGroupName in sourceAMCounters) {
        counters = sourceAMCounters[counterGroupName],
        atsCounters = targetATSCounters.findBy('counterGroupName', counterGroupName);
        if(!atsCounters) {
          atsCounters = [];
          targetATSCounters.push({
            counterGroupName: counterGroupName,
            counterGroupDisplayName: counterGroupName,
            counters: atsCounters
          });
        }
        else {
          atsCounters = atsCounters.counters;
        }
        for(var counterName in counters) {
          atsCounter = atsCounters.findBy('counterName', counterName);
          if(atsCounter) {
            Em.set(atsCounter, 'counterValue', counters[counterName]);
          }
          else {
            atsCounters.push({
              "counterName": counterName,
              "counterDisplayName": counterName,
              "counterValue": counters[counterName]
            });
          }
        }
      }
    }
    catch(e){
      Em.Logger.info("Counter merge failed", e);
    }

    return targetATSCounters;
  },

  /*
   * Creates column definitions form configuration object array
   * @param columnConfigs Array
   * @return columnDefinitions Array
   */
  createColumnsFromConfigs: function (columnConfigs) {
    return columnConfigs.map(function (columnConfig) {
      if(columnConfig.getCellContentHelper) {
        columnConfig.getCellContent = App.Helpers.get(columnConfig.getCellContentHelper);
      }
      columnConfig.minWidth = columnConfig.minWidth || 135;

      return columnConfig.filterID ?
          App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin, columnConfig) :
          App.ExTable.ColumnDefinition.create(columnConfig);
    });
  },

  createColumnDescription: function (columnConfigs) {
    return columnConfigs.map(function (column) {
      return App.BasicTableComponent.ColumnDefinition.create(column);
    });
  },

  /*
   * Returns a counter value from for a row
   * @param row
   * @return value
   */
  getCounterCellContent: function (row) {
    var contentPath = this.id.split('/'),
        value = null;

    try{
      value = row.get('counterGroups').
          findBy('counterGroupName', contentPath[0])
          ['counters'].
          findBy('counterName', contentPath[1])
          ['counterValue'];
    }catch(e){}

    return value;
  },

  /*
   * Returns a counter value from for a row
   * @param row
   * @return value
   */
  getCounterCellContentFormatted: function (row) {
    var value = App.Helpers.misc.getCounterCellContent.call(this, row);
    return App.Helpers.number.formatNumThousands(value);
  },

  /* 
   * returns a formatted message, the real cause is unknown and the error object details
   * depends on the error cause. the function tries to handle ajax error or a native errors
   */
  formatError: function(error, defaultErrorMessage) {
    var msg;
    // for cross domain requests, the error is not set if no access control headers were found.
    // this could be either because there was a n/w error or the cors headers being not set.
    if (error.status === 0 && error.statusText === 'error') {
      msg = defaultErrorMessage ;
    } else {
      msg = error.statusText || error.message;
    }
    msg = msg || 'Unknown error';
    if (!!error.responseText) {
      msg += error.responseText;
    }

    if(error.requestOptions) {
      msg = '%@<br/>Could not retrieve expected data from %@ @ %@'.fmt(
        msg,
        error.requestOptions.targetServer,
        error.requestOptions.url
      )
    }

    return {
      errCode: error.status || 'Unknown', 
      msg: msg,
      details: error.stack
    };
  },

  /**
   * Normalize path
   * @param path {String}
   * @return normalized path {String}
   */
  normalizePath: function (path) {
    if(path && path.charAt(path.length - 1) == '/') {
      path = path.slice(0, -1);
    }
    return path;
  },

  // Tez originally shows the status for task and task attempt only on
  // completion. this causes confusion to the user as the status would not be
  // displayed. so if status is not set return the status as 'RUNNING'. We do
  // not diffentiate between running and scheduled.
  getFixedupDisplayStatus: function(originalStatus) {
    // if status is not set show it as running, since originally the task did
    // not have a status set on scheduled/running.
    // with the new version we set the status of task as scheduled and that of
    // task attempt as running
    if (!originalStatus || originalStatus == 'SCHEDULED') {
      originalStatus = 'RUNNING';
    }
    return originalStatus;
  },

  /**
   * Merge content of obj2 into obj2, array elements will be concated.
   * @param obj1 {Object}
   * @param obj2 {Object}
   */
  merge: function objectMerge(obj1, obj2) {
    $.each(obj2, function (key, val) {
      if(Array.isArray(obj1[key]) && Array.isArray(val)) {
        $.merge(obj1[key], val);
      }
      else if($.isPlainObject(obj1[key]) && $.isPlainObject(val)) {
        objectMerge(obj1[key], val);
      }
      else {
        obj1[key] = val;
      }
    });
  },

  getTaskIndex: function(dagID, taskID) {
    var idPrefix = 'task_%@_'.fmt(dagID.substr(4));
    return taskID.indexOf(idPrefix) == 0 ? taskID.substr(idPrefix.length) : id;
  },

  getVertexIdFromName: function(idToNameMap, vertexName) {
    idToNameMap = idToNameMap || {};
    var vertexId = undefined;
    $.each(idToNameMap, function(id, name) {
      if (name === vertexName) {
        vertexId = id;
        return false;
      }
    });
    return vertexId;
  },

  /* Gets the application id from dagid
   * @param dagId {String}
   * @return application id for the dagid {String}
   */
  getAppIdFromDagId: function(dagId) {
    var dagIdRegex = /^dag_(\d+)_(\d+)_\d+$/,
        appId = undefined;
    if (dagIdRegex.test(dagId)) {
      appId = dagId.replace(dagIdRegex, 'application_$1_$2');
    }
    return appId;
  },

  /* Gets the application id from vertex id
   * @param vertexId {String}
   * @return application id for the vertexId {String}
   */
  getAppIdFromVertexId: function(vertexId) {
    var vertexIdRegex = /^vertex_(\d+)_(\d+)_\d+_\d+$/
        appId = undefined;
    if (vertexIdRegex.test(vertexId)) {
      appId = vertexId.replace(vertexIdRegex, 'application_$1_$2');
    }
    return appId;
  },  

  /* Gets the dag index from the dag id
   * @param dagId {String}
   * @return dag index for the given dagId {String}
   */
  getDagIndexFromDagId: function(dagId) {
    return dagId.split('_').splice(-1).pop();
  },

  /*
   * Return index for the given id
   * @param id {string}
   * @return index {Number}
   */
  getIndexFromId: function (id) {
    return parseInt(id.split('_').splice(-1).pop());
  },

  /**
   * Remove the specific record from store
   * @param store {DS.Store}
   * @param type {String}
   * @param id {String}
   */
  //TODO: TEZ-2876 Extend store to have a loadRecord function that skips the cache
  removeRecord: function (store, type, id) {
    var record = store.getById(type, id);
    if(record) {
      store.unloadRecord(record);
    }
  },

  downloadDAG: function(dagID, options) {
    var opts = options || {},
        batchSize = opts.batchSize || 1000,
        baseurl = '%@/%@'.fmt(App.env.timelineBaseUrl, App.Configs.restNamespace.timeline),
        itemsToDownload = [
          {
            url: getUrl('TEZ_APPLICATION', 'tez_' + this.getAppIdFromDagId(dagID)),
            context: { name: 'application', type: 'TEZ_APPLICATION' },
            onItemFetched: processSingleItem
          },
          {
            url: getUrl('TEZ_DAG_ID', dagID),
            context: { name: 'dag', type: 'TEZ_DAG_ID' },
            onItemFetched: processSingleItem
          },
          {
            url: getUrl('TEZ_VERTEX_ID', dagID),
            context: { name: 'vertices', type: 'TEZ_VERTEX_ID', part: 0 },
            onItemFetched: processMultipleItems
          },
          {
            url: getUrl('TEZ_TASK_ID', dagID),
            context: { name: 'tasks', type: 'TEZ_TASK_ID', part: 0 },
            onItemFetched: processMultipleItems
          },
          {
            url: getUrl('TEZ_TASK_ATTEMPT_ID', dagID),
            context: { name: 'task_attempts', type: 'TEZ_TASK_ATTEMPT_ID', part: 0 },
            onItemFetched: processMultipleItems
          }
        ],
        numItemTypesToDownload = itemsToDownload.length,
        downloader = App.Helpers.io.fileDownloader(),
        zipHelper = App.Helpers.io.zipHelper({
          onProgress: function(filename, current, total) {
            Em.Logger.debug('%@: %@ of %@'.fmt(filename, current, total));
          },
          onAdd: function(filename) {
            Em.Logger.debug('adding %@ to Zip'.fmt(filename));
          }
        });

    function getUrl(type, dagID, fromID) {
      var url;
      if (type == 'TEZ_DAG_ID' || type == 'TEZ_APPLICATION') {
        url = '%@/%@/%@'.fmt(baseurl, type, dagID);
      } else {
        url = '%@/%@?primaryFilter=TEZ_DAG_ID:%@&limit=%@'.fmt(baseurl, type, dagID, batchSize + 1);
        if (!!fromID) {
          url = '%@&fromId=%@'.fmt(url, fromID);
        }
      }
      return url;
    }

    function checkIfAllDownloaded() {
      numItemTypesToDownload--;
      if (numItemTypesToDownload == 0) {
        downloader.finish();
      }
    }

    function processSingleItem(data, context) {
      var obj = {};
      obj[context.name] = data;

      zipHelper.addFile({name: '%@.json'.fmt(context.name), data: JSON.stringify(obj, null, 2)});
      checkIfAllDownloaded();
    }

    function processMultipleItems(data, context) {
      var obj = {};
      var nextBatchStart = undefined;

      if (!$.isArray(data.entities)) {
        throw "invalid data";
      }

      // need to handle no more entries , zero entries
      if (data.entities.length > batchSize) {
        nextBatchStart = data.entities.pop().entity;
      }
      obj[context.name] = data.entities;

      zipHelper.addFile({name: '%@_part_%@.json'.fmt(context.name, context.part), data: JSON.stringify(obj, null, 2)});

      if (!!nextBatchStart) {
        context.part++;
        downloader.queueItem({
          url: getUrl(context.type, dagID, nextBatchStart),
          context: context,
          onItemFetched: processMultipleItems
        });
      } else {
        checkIfAllDownloaded();
      }
    }

    downloader.queueItems(itemsToDownload);

    downloader.then(function() {
      Em.Logger.info('Finished download');
      zipHelper.close();
    }).catch(function(e) {
      Em.Logger.error('Failed to download: ' + e);
      zipHelper.abort();
    });

    var that = this;
    zipHelper.then(function(zippedBlob) {
      saveAs(zippedBlob, '%@.zip'.fmt(dagID));
      if ($.isFunction(opts.onSuccess)) {
        opts.onSuccess();
      }
    }).catch(function() {
      Em.Logger.error('zip Failed');
      if ($.isFunction(opts.onFailure)) {
        opts.onFailure();
      }
    });

    return {
      cancel: function() {
        downloader.cancel();
      }
    }
  },

  /**
   * Returns in/out/empty string based counter group type
   * @param counterGroupName {String}
   * @return in/out/empty string
   */
  checkIOCounterGroup: function (counterGroupName) {
    if(counterGroupName == undefined){
      debugger;
    }
    var relationPart = counterGroupName.substr(counterGroupName.indexOf('_') + 1);
    if(relationPart.match('_INPUT_')) {
      return 'in';
    }
    else if(relationPart.match('_OUTPUT_')) {
      return 'out';
    }
    return '';
  },

  /**
   * Return unique values form array based on a property
   * @param array {Array}
   * @param property {String}
   * @return uniqueArray {Array}
   */
  getUniqueByProperty: function (array, property) {
    var propHash = {},
        uniqueArray = [];

    array.forEach(function (item) {
      if(item && !propHash[item[property]]) {
        uniqueArray.push(item);
        propHash[item[property]] = true;
      }
    });

    return uniqueArray;
  },

  /*
   * Extends the path and adds new query params into an url
   * @param url {String} Url to modify
   * @param path {String} Path to be added
   * @param queryParams {Object} Params to be added
   * @return modified path
   */
  modifyUrl: function (url, path, queryParams) {
    var urlParts = url.split('?'),
        params = {};

    if(queryParams) {
      if(urlParts[1]) {
        params = urlParts[1].split('&').reduce(function (obj, param) {
          var paramParts;
          if(param.trim()) {
            paramParts = param.split('=');
            obj[paramParts[0]] = paramParts[1];
          }
          return obj;
        }, {});
      }

      params = $.extend(params, queryParams);

      queryParams = [];
      $.map(params, function (val, key) {
        queryParams.push(key + "=" + val);
      });

      urlParts[1] = queryParams.join('&');
    }

    urlParts[0] += path || '';

    return urlParts[1] ? '%@?%@'.fmt(urlParts[0], urlParts[1]) : urlParts[0];
  },

  constructLogLinks: function (attempt, yarnAppState, amUser) {
    var path,
        link,
        logLinks = {},
        params = amUser ? {
          "user.name": amUser
        } : {};

    if(attempt) {
      link = attempt.get('inProgressLog') || attempt.get('completedLog');
      if(link) {
        if(!link.match("/syslog_")) {
          path = "/syslog_" + attempt.get('id');
          if(amUser) {
            path += "/" + amUser;
          }
        }
        logLinks.viewUrl = App.Helpers.misc.modifyUrl(link, path, params);
      }

      link = attempt.get('completedLog');
      if (link && yarnAppState === 'FINISHED' || yarnAppState === 'KILLED' || yarnAppState === 'FAILED') {
        params["start"] = "0";

        if(!link.match("/syslog_")) {
          path = "/syslog_" + attempt.get('id');
          if(amUser) {
            path += "/" + amUser;
          }
        }

        logLinks.downloadUrl = App.Helpers.misc.modifyUrl(link, path, params);
      }
    }

    return logLinks;
  },

  timelinePathForType: (function () {
    var typeToPathMap = {
      dag: 'TEZ_DAG_ID',

      vertex: 'TEZ_VERTEX_ID',
      dagVertex: 'TEZ_VERTEX_ID',

      task: 'TEZ_TASK_ID',
      dagTask: 'TEZ_TASK_ID',
      vertexTask: 'TEZ_TASK_ID',

      taskAttempt: 'TEZ_TASK_ATTEMPT_ID',
      dagTaskAttempt: 'TEZ_TASK_ATTEMPT_ID',
      vertexTaskAttempt: 'TEZ_TASK_ATTEMPT_ID',
      taskTaskAttempt: 'TEZ_TASK_ATTEMPT_ID',

      hiveQuery: 'HIVE_QUERY_ID',

      tezApp: 'TEZ_APPLICATION'
    };
    return function (type) {
      return typeToPathMap[type];
    };
  })(),

  getTimelineFilterForType: (function () {
    var typeToPathMap = {
      dag: 'TEZ_DAG_ID',

      vertex: 'TEZ_VERTEX_ID',
      dagVertex: 'TEZ_VERTEX_ID',

      task: 'TEZ_TASK_ID',
      dagTask: 'TEZ_TASK_ID',
      vertexTask: 'TEZ_TASK_ID',

      taskAttempt: 'TEZ_TASK_ATTEMPT_ID',
      dagTaskAttempt: 'TEZ_TASK_ATTEMPT_ID',
      vertexTaskAttempt: 'TEZ_TASK_ATTEMPT_ID',
      taskTaskAttempt: 'TEZ_TASK_ATTEMPT_ID',

      hiveQuery: 'HIVE_QUERY_ID',

      tezApp: 'applicationId'
    };
    return function (type) {
      return typeToPathMap[type];
    };
  })(),

  dagStatusUIOptions: [
    { label: 'All', id: null },
    { label: 'Submitted', id: 'SUBMITTED' },
    { label: 'Running', id: 'RUNNING' },
    { label: 'Succeeded', id: 'SUCCEEDED' },
    { label: 'Failed', id: 'FAILED' },
    { label: 'Error', id: 'ERROR' },
  ],

  vertexStatusUIOptions: [
    { label: 'All', id: null },
    { label: 'Running', id: 'RUNNING' },
    { label: 'Succeeded', id: 'SUCCEEDED' },
    { label: 'Failed', id: 'FAILED' },
    { label: 'Killed', id: 'KILLED' },
    { label: 'Error', id: 'ERROR' },
  ],

  taskStatusUIOptions: [
    { label: 'All', id: null },
    { label: 'Running', id: 'SCHEDULED' },
    { label: 'Succeeded', id: 'SUCCEEDED' },
    { label: 'Failed', id: 'FAILED' },
    { label: 'Killed', id: 'KILLED' },
  ],

  taskAttemptStatusUIOptions: [
    { label: 'All', id: null },
    { label: 'Running', id: 'RUNNING' },
    { label: 'Succeeded', id: 'SUCCEEDED' },
    { label: 'Failed', id: 'FAILED' },
    { label: 'Killed', id: 'KILLED' },
  ],

  defaultQueryParamsConfig: {
    refreshModel: true,
    replace: true
  }

}
