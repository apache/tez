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
  var task_attempt_id=window.sessionStorage.getItem('taskattemptid');
  var hostname=window.sessionStorage.getItem('hostname');
  var port=window.sessionStorage.getItem('port');
  var taurl="http://"+hostname+":"+port+"/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID/"+task_attempt_id;

  $.getJSON(taurl, function(task_attempt) {
    $('#taskattemptid').text("Task Attempt Id - " + task_attempt_id);
    var task_attempt_table=$('#task_attempt_overview').DataTable({
      "paging": false,
      "searching": false,
      "ordering": false,
      "info": false,
      });
    var taskAttemptStartEpoch=task_attempt.otherinfo.startTime;
    var taskAttemptStartTime=new Date(taskAttemptStartEpoch);
    var taskAttemptEndEpoch=task_attempt.otherinfo.endTime;
    var taskAttemptEndTime=new Date(taskAttemptEndEpoch);
    var inProgressLogs='<a href=http://'+task_attempt.otherinfo.inProgressLogsURL+'> inProgressLog</a>';
    var completedLogs='<a href='+task_attempt.otherinfo.completedLogsURL+'> completeLog</a>';
    task_attempt_table.row.add([task_attempt.entity, taskAttemptStartTime.toGMTString(), taskAttemptEndTime.toGMTString(), task_attempt.otherinfo.timeTaken, task_attempt.otherinfo.status, inProgressLogs, completedLogs]);
    task_attempt_table.draw();

    var counterGroupNum = 0;
    $.each(task_attempt.otherinfo.counters.counterGroups, function(i, counterGroup){
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
