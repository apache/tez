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

App.Dag = App.AbstractEntity.extend({

  idx: function() {
    return App.Helpers.misc.getDagIndexFromDagId(this.get('id'));
  }.property('id'),

  submittedTime: DS.attr('number'),

  // start time of the entity
  startTime: DS.attr('number'),

  // end time of the entity
  endTime: DS.attr('number'),

  duration: function () {
    return App.Helpers.date.duration(this.get('startTime'), this.get('endTime'))
  }.property('startTime', 'endTime'),

  // set type to DAG
  entityType: App.EntityType.DAG,

  // Name of the dag.
  name: DS.attr('string'),

  // user name who ran this dag.
  user: DS.attr('string'),

  // application ID of this dag.
  applicationId: function() {
    return App.Helpers.misc.getAppIdFromDagId(this.get('id'));
  }.property('id'),

  tezApp: DS.belongsTo('tezApp'),
  appDetail: DS.belongsTo('appDetail'),

  // status
  status: DS.attr('string'),
  hasFailedTaskAttempts: DS.attr('boolean'),
  hasFailedTasks: function() {
    var f = this.get('numFailedTasks');
    return !!f && f > 0;
  }.property('numFailedTasks'),
  numFailedTasks: DS.attr('number'),

  // diagnostics info if any.
  diagnostics: DS.attr('string'),

  // Dag plan reated data
  planName: DS.attr('string'),
  planVersion: DS.attr('number'),
  appContextInfo: DS.attr('object'),
  vertices: DS.attr('array'), // Serialize when required
  edges: DS.attr('array'), // Serialize when required
  vertexGroups: DS.attr('array'),
  vertexIdToNameMap: DS.attr('array'),

  counterGroups: DS.attr('array'),
  amWebServiceVersion: DS.attr('string')
});

App.CounterGroup = DS.Model.extend({
  name: DS.attr('string'),

  displayName: DS.attr('string'),

  counters: DS.hasMany('counter', { inverse: 'parent' }),

  parent: DS.belongsTo('abstractEntity', { polymorphic: true })
});

App.Counter = DS.Model.extend({
  name: DS.attr('string'),

  displayName: DS.attr('string'),

  value: DS.attr('number'),

  parent: DS.belongsTo('counterGroup')
});

App.Edge = DS.Model.extend({

  fromVertex: DS.belongsTo('vertex'),

  toVertex: DS.belongsTo('vertex'),

  /**
   * Type of this edge connecting vertices. Should be one of constants defined
   * in 'App.EdgeType'.
   */
  edgeType: DS.attr('string'),

  dag: DS.belongsTo('dag')
});

App.Vertex = App.AbstractEntity.extend({
  name: DS.attr('string'),
  vertexIdx: function() {
    return this.get('id').split('_').splice(-1).pop();
  }.property('id'),

  dag: DS.belongsTo('dag'),
  dagID: DS.attr('string'),
  applicationId: function() {
    return App.Helpers.misc.getAppIdFromVertexId(this.get('id'));
  }.property('id'),
  dagIdx: function() {
    return this.get('dagID').split('_').splice(-1).pop();
  }.property('dagID'),

  tezApp: DS.belongsTo('tezApp'),

  /**
   * State of this vertex. Should be one of constants defined in
   * App.VertexState.
   */
  status: DS.attr('string'),
  hasFailedTaskAttempts: DS.attr('boolean'),
  hasFailedTasks: function() {
    var f = this.get('failedTasks');
    return !!f && f > 0;
  }.property('failedTasks'),

  /**
   * Vertex type has to be one of the types defined in 'App.VertexType'
   * @return {string}
   */
  type: DS.attr('string'),

  /**
   * A vertex can have multiple incoming edges.
   */
  incomingEdges: DS.hasMany('edge', {inverse: 'fromVertex' }),

  /**
   * This vertex can have multiple outgoing edges.
   */
  outgoingEdges: DS.hasMany('edge', {inverse: 'toVertex'}),

  startTime: DS.attr('number'),

  endTime: DS.attr('number'),

  firstTaskStartTime: DS.attr('number'),

  firstTasksToStart: DS.attr('string'),

  lastTaskFinishTime: DS.attr('number'),

  lastTasksToFinish: DS.attr('string'),

  minTaskDuration: DS.attr('number'),

  maxTaskDuration: DS.attr('number'),

  avgTaskDuration: DS.attr('number'),

  shortestDurationTasks: DS.attr('string'),

  longestDurationTasks: DS.attr('string'),

  processorClassName: DS.attr('string'),

  /**
   * Provides the duration of this job. If the job has not started, duration
   * will be given as 0. If the job has not ended, duration will be till now.
   *
   * @return {Number} Duration in milliseconds.
   */
  duration: function () {
    return App.Helpers.date.duration(this.get('startTime'), this.get('endTime'))
  }.property('startTime', 'endTime'),

  /**
   * Each Tez vertex can perform arbitrary application specific computations
   * inside. The application can provide a list of operations it has provided in
   * this vertex.
   *
   * Array of strings. [{string}]
   */
  operations: DS.attr('array'),

  /**
   * Provides additional information about the 'operations' performed in this
   * vertex. This is shown directly to the user.
   */
  operationPlan: DS.attr('string'),

  /**
   * Number of actual Map/Reduce tasks in this vertex
   */
  numTasks: DS.attr('number'),

  name: DS.attr('string'),

  failedTasks: DS.attr('number'),
  sucessfulTasks: DS.attr('number'),
  numTasks: DS.attr('number'),
  killedTasks: DS.attr('number'),

  diagnostics: DS.attr('string'),

  counterGroups: DS.attr('array'),

  tasksNumber: function () {
    return this.getWithDefault('tasksCount', 0);
  }.property('tasksCount'),

  /**
   * Local filesystem usage metrics for this vertex
   */
  fileReadBytes: DS.attr('number'),

  fileWriteBytes: DS.attr('number'),

  fileReadOps: DS.attr('number'),

  fileWriteOps: DS.attr('number'),

  /**
   * Spilled records
   */
  spilledRecords: DS.attr('number'),

  /**
   * HDFS usage metrics for this vertex
   */
  hdfsReadBytes: DS.attr('number'),

  hdfsWriteBytes: DS.attr('number'),

  hdfsReadOps: DS.attr('number'),

  hdfsWriteOps: DS.attr('number'),

  /**
   * Record metrics for this vertex
   */
  recordReadCount: DS.attr('number'),

  recordWriteCount: DS.attr('number'),

  inputs: DS.hasMany('input'),
  outputs: DS.hasMany('output'),

  totalReadBytes: function () {
    return this.get('fileReadBytes') + this.get('hdfsReadBytes');
  }.property('fileReadBytes', 'hdfsReadBytes'),

  totalWriteBytes: function () {
    return this.get('fileWriteBytes') + this.get('hdfsWriteBytes');
  }.property('fileWriteBytes', 'hdfsWriteBytes'),

  totalReadBytesDisplay: function () {
    return  App.Helpers.number.bytesToSize(this.get('totalReadBytes'));
  }.property('totalReadBytes'),

  totalWriteBytesDisplay: function () {
    return  App.Helpers.number.bytesToSize(this.get('totalWriteBytes'));
  }.property('totalWriteBytes'),

  durationDisplay: function () {
    return App.Helpers.date.timingFormat(this.get('duration'), true);
  }.property('duration')
});
App.DagVertex = App.Vertex.extend({});

App.Input = App.AbstractEntity.extend({
  entity: DS.attr('string'),

  inputName: DS.attr('string'),
  inputClass: DS.attr('string'),
  inputInitializer: DS.attr('string'),

  configs: DS.hasMany('kVData', { async: false })
});

App.Output = App.AbstractEntity.extend({
  entity: DS.attr('string'),

  outputName: DS.attr('string'),
  outputClass: DS.attr('string'),

  configs: DS.hasMany('kVData', { async: false })
});

App.AppDetail = App.AbstractEntity.extend({
  attemptId: DS.attr('string'),

  user: DS.attr('string'),
  name: DS.attr('string'),
  queue: DS.attr('string'),
  type: DS.attr('string'),

  appState: DS.attr('string'),
  finalAppStatus: DS.attr('string'),
  progress: DS.attr('string'),

  startedTime: DS.attr('number'),
  elapsedTime: DS.attr('number'),
  finishedTime: DS.attr('number'),
  submittedTime: DS.attr('number'),

  diagnostics: DS.attr('string'),
});

App.TezApp = App.AbstractEntity.extend({
  appId: DS.attr('string'),
  entityType: DS.attr('string'),
  domain: DS.attr('string'),
  user: DS.attr('string'),

  startedTime: DS.attr('number'),

  appDetail: DS.belongsTo('appDetail', { async: true }),
  dags: DS.hasMany('dag', { async: true }),

  configs: DS.hasMany('kVData', { async: false })
});


App.Task = App.AbstractEntity.extend({
  status: DS.attr('status'),

  index: function () {
    var id = this.get('id'),
        idPrefix = 'task_%@_'.fmt(this.get('dagID').substr(4));
    return id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id;
  }.property('id'),

  dagID: DS.attr('string'),

  successfulAttemptId: DS.attr('string'),

  attempts: DS.attr('array'),

  vertex: DS.belongsTo('vertex'),
  vertexID: DS.attr('string'),

  tezApp: DS.belongsTo('tezApp'),

  startTime: DS.attr('number'),

  endTime: DS.attr('number'),

  duration: function () {
    return App.Helpers.date.duration(this.get('startTime'), this.get('endTime'))
  }.property('startTime', 'endTime'),

  diagnostics: DS.attr('string'),

  numAttempts: DS.attr('number'),

  pivotAttempt: DS.belongsTo('taskAttempt'),

  counterGroups: DS.attr('array'), // Serialize when required
  numFailedTaskAttempts: DS.attr('number'),
  hasFailedTaskAttempts: function() {
    var numAttempts = this.get('numFailedTaskAttempts') || 0;
    return numAttempts > 1;
  }.property('numFailedTaskAttempts')
});
App.DagTask = App.Task.extend({});
App.VertexTask = App.Task.extend({});

App.DagProgress = DS.Model.extend({
  progress: DS.attr('number'),
  appId: DS.attr('string'),
  dagIdx: DS.attr('number')
});

App.VertexProgress = DS.Model.extend({
  progress: DS.attr('number'),
  appId: DS.attr('string'),
  dagIdx: DS.attr('string')
});

App.DagInfo = DS.Model.extend({
  // we need appId and dagIdx as they are used for querying with AM
  appId: function() {
    return App.Helpers.misc.getAppIdFromDagId(this.get('id'));
  }.property('id'),
  dagIdx: function() {
    return App.Helpers.misc.getDagIndexFromDagId(this.get('id'));
  }.property('id'),

  progress: DS.attr('number'),
  status: DS.attr('string'),
});

App.VertexInfo = DS.Model.extend({
  // we need appId and dagIdx as they are used for querying with AM
  appId: function() {
    return App.Helpers.misc.getAppIdFromDagId(this.get('id'));
  }.property('id'),
  dagIdx: function() {
    return App.Helpers.misc.getDagIndexFromDagId(this.get('id'));
  }.property('id'),

  progress: DS.attr('number'),
  status: DS.attr('string'),
  totalTasks: DS.attr('number'),
  runningTasks: DS.attr('number'),
  succeededTasks: DS.attr('number'),
  failedTaskAttempts: DS.attr('number'),
  killedTaskAttempts: DS.attr('number'),
  pendingTasks: function() {
    return this.get('totalTasks') - this.get('runningTasks') - this.get('succeededTasks');
  }.property('totalTasks', 'runningTasks', 'succeededTasks')
});

App.KVDatum = DS.Model.extend({
  key: DS.attr('string'),
  value: DS.attr('string'),
});

App.HiveQuery = DS.Model.extend({
  query: DS.attr('string')
});

App.VertexState = {
  NEW: "NEW",
  INITIALIZING: "INITIALIZING",
  INITED: "INITED",
  RUNNING: "RUNNING",
  SUCCEEDED: "SUCCEEDED",
  FAILED: "FAILED",
  KILLED: "KILLED",
  ERROR: "ERROR",
  TERMINATING: "TERMINATING",
  JOBFAILED: "JOB FAILED"
};

App.VertexType = {
  MAP: 'MAP',
  REDUCE: 'REDUCE',
  UNION: 'UNION'
};

App.EdgeType = {
  SCATTER_GATHER: "SCATTER_GATHER",
  BROADCAST: "BROADCAST",
  CONTAINS: "CONTAINS"
};
