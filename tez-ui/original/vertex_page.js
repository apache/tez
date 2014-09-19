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
	var vertex_id=window.sessionStorage.getItem('vertid');
	var hostname=window.sessionStorage.getItem('hostname');
	var port=window.sessionStorage.getItem('port');
	var vurl='http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_VERTEX_ID/'+vertex_id;

  $.getJSON(vurl, function(vertex) {
    $('#vertexid').text("Vertex Id - " + vertex_id);

    var vertex_table=$('#vertex_overview').DataTable(
      { "paging": false,
        "searching": false,
        "ordering": false,
        "info": false,
      });
    vertexStartEpoch=vertex.otherinfo.startTime;
    vertexStartTime=new Date(vertexStartEpoch);
    vertexEndEpoch=vertex.otherinfo.endTime;
    vertexEndTime=new Date(vertexEndEpoch);
    vertex_table.row.add([vertex.entity, vertex.otherinfo.vertexName, vertexStartTime.toGMTString(), vertexEndTime.toGMTString(), vertex.otherinfo.timeTaken, vertex.otherinfo.numTasks, vertex.otherinfo.status]);
    vertex_table.draw();

    var task_table=$('#tasks').DataTable();
    var turl='http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_TASK_ID?primaryFilter=TEZ_VERTEX_ID:' + vertex_id;
    $.getJSON(turl, function(tasks) {
      $.each(tasks.entities, function(i, task) {
        var callback='tcallback("'+ task.entity+'");>';			
        var task_id_cell='<a href="task_page.html" onclick='+ callback + task.entity +" </a>";
        var taskStartEpoch=task.otherinfo.startTime;
        var taskStartTime=new Date(taskStartEpoch);
        var taskEndEpoch=task.otherinfo.endTime;
        var taskEndTime=new Date(taskEndEpoch);
        task_table.row.add([task_id_cell, taskStartTime.toGMTString(), taskEndTime.toGMTString(), task.otherinfo.timeTaken, task.otherinfo.status]);
      });
      task_table.draw();
    });

    var counterGroupNum = 0;
    $.each(vertex.otherinfo.counters.counterGroups, function(i, counterGroup){
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
	window.sessionStorage.setItem('taskid',id);
}
