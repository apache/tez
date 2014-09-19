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
  var dag_id=window.sessionStorage.getItem('dag_id');
  var hostname=window.sessionStorage.getItem('hostname');
  var port=window.sessionStorage.getItem('port');
  var dag_name,start_time,status,initTime,timetaken,epoch;
  var vertex_table,counters_table,dct;
  var dagurl= 'http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_DAG_ID/'+dag_id;
  $.getJSON(dagurl, function(dagent) {
    dag_name=dagent.otherinfo.dagPlan.dagName;					
    $('#dagid').text("DAG Id - " + dag_id);
    startEpoch=dagent.otherinfo.startTime;
    startTime=new Date(startEpoch);
    endEpoch=dagent.otherinfo.initTime;
    endTime=new Date(endEpoch);
    timeTaken=dagent.otherinfo.timeTaken;
    status=dagent.otherinfo.status;
    var dag_overview = $('#dag_overview').DataTable(
      { "paging": false,
        "searching": false,
        "ordering": false,
        "info": false,
      });
    dag_overview.row.add([
      dag_id,
      dag_name,
      startTime.toGMTString(),
      endTime.toGMTString(),
      timeTaken,
      status
      ]).draw();
    vertex_table=$('#dag_vertex').DataTable();
    var vurl='http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_VERTEX_ID?primaryFilter=TEZ_DAG_ID:' + dag_id;

    $.getJSON(vurl, function(vertex_entities) {
      $.each(vertex_entities.entities, function(i, vertex) {
        var callback ='vcallback("'+ vertex.entity +'");>';
        var vertex_id='<a href="vertex_page.html" onclick='+ callback + vertex.entity +" </a>";
        var vertexStartEpoch=vertex.otherinfo.startTime;
        var vertexStartTime=new Date(vertexStartEpoch);
        var vertexEndEpoch=vertex.otherinfo.endTime;
        var vertexEndTime=new Date(vertexEndEpoch);
        vertex_table.row.add([vertex_id, vertex.otherinfo.vertexName, vertexStartTime.toGMTString(), vertexEndTime.toGMTString(), vertex.otherinfo.timeTaken, vertex.otherinfo.numTasks, vertex.otherinfo.status]);
      });
      vertex_table.draw();
    });
    var counterGroupNum = 0;
    $.each(dagent.otherinfo.counters.counterGroups, function(i, counterGroup){
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

function vcallback(id) {
  window.sessionStorage.setItem('vertid', id);
}
