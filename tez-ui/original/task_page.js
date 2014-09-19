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
$(document).ready( function () {
  var task_id=window.sessionStorage.getItem('taskid');
  var hostname=window.sessionStorage.getItem('hostname');
  var port=window.sessionStorage.getItem('port');
  var turl="http://"+hostname+":"+port+"/ws/v1/timeline/TEZ_TASK_ID/"+task_id;

  $.getJSON(turl, function(task) {
    $('#taskid').text("Task Id - " + task_id);
    var task_table=$('#task_overview').DataTable({
      "paging": false,
      "searching": false,
      "ordering": false,
      "info": false,
      });
    var taskStartEpoch=task.otherinfo.startTime;
    var taskStartTime=new Date(taskStartEpoch);
    var taskEndEpoch=task.otherinfo.endTime;
    var taskEndTime=new Date(taskEndEpoch);
    var startTime, status, scheduledTime, endTime, diagnostics;
    task_table.row.add([task.entity, taskStartTime.toGMTString(), taskEndTime.toGMTString(), task.otherinfo.timeTaken, task.otherinfo.status]);
    task_table.draw();

    var task_attempts_table=$('#task_attempts').DataTable();
    var turl='http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID?primaryFilter=TEZ_TASK_ID:' + task_id;
    $.getJSON(turl, function(task_attempts) {
      $.each(task_attempts.entities, function(i, task_attempt) {
        var callback='tcallback("'+ task_attempt.entity+'");>';			
        var task_attempt_id_cell='<a href="task_attempt_page.html" onclick='+ callback + task_attempt.entity +" </a>";
        var taskAttemptStartEpoch=task_attempt.otherinfo.startTime;
        var taskAttemptStartTime=new Date(taskAttemptStartEpoch);
        var taskAttemptEndEpoch=task_attempt.otherinfo.endTime;
        var taskAttemptEndTime=new Date(taskAttemptEndEpoch);
        task_attempts_table.row.add([task_attempt_id_cell, taskAttemptStartTime.toGMTString(), taskAttemptEndTime.toGMTString(), task_attempt.otherinfo.timeTaken, task_attempt.otherinfo.status]);
      });
      task_attempts_table.draw();
    });

    var counterGroupNum = 0;
    $.each(task.otherinfo.counters.counterGroups, function(i, counterGroup){
      $("<th>Counters</th></tr></thead><tbody><th>" + counterGroup.counterGroupDisplayName + "</th>");
      $("<tr><th>Counters</th></tr></thead><tbody><th>" + counterGroup.counterGroupDisplayName + "</th><td class='table'><table id='countergroup" + counterGroupNum + "'><thead><tr><th>Name</th><th>Value</th></tr></thead></table></td></tr>").appendTo('#counterbody');
      var counterGroupDT = $('#countergroup' + counterGroupNum).DataTable();
      counterGroupNum++;
      $.each(counterGroup.counters, function(i, counter) {
        counterGroupDT.row.add([counter.counterDisplayName, counter.counterValue]);
      });
      counterGroupDT.draw();
    });
    counters_table=$('#counter').DataTable(
        { "paging": false,
          "searching": false,
          "ordering": false,
          "info": false,
        });
  });
});
function tcallback(id) {
	window.sessionStorage.setItem('taskattemptid',id);
}
