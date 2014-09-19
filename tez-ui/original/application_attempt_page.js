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
  var id=window.sessionStorage.getItem('application_attempt_id');
  var hostname=window.sessionStorage.getItem('hostname');
  var port=window.sessionStorage.getItem('port');
  var url= 'http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_APPLICATION_ATTEMPT/' + id + '?fields=relatedentities';
  var dag_name,start_time,status,initTime,timetaken,epoch;
  var dagtable = $('#dag_table').DataTable();
  $.getJSON(url, function(entity) {
    $.each(entity.relatedentities.TEZ_DAG_ID, function(i,dag_id) {
      var dagurl= 'http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_DAG_ID/'+dag_id;
      $.getJSON(dagurl, function(dagent) {
        dag_name=dagent.primaryfilters.dagName;					
        epoch=dagent.otherinfo.startTime;
        start_time=new Date(epoch);
        status=dagent.otherinfo.status;
        epoch=dagent.otherinfo.initTime;
        initTime=new Date(epoch);
        timetaken=dagent.otherinfo.timeTaken;
				var callback= 'vcallback("' + dag_id + '");>';
        dagtable.row.add([
					'<a href="dag_page.html" onclick=' + callback + dag_id + " </a>",
          dag_name,
          start_time.toGMTString(),
          timetaken,
          status
          ]).draw();
      });
    });
  });
});

function vcallback(id) {
	window.sessionStorage.setItem('dag_id', id);
}
