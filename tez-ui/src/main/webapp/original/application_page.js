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
   var t = $('#table_id').DataTable();
   var hostname=window.sessionStorage.getItem('hostname');
   var port=window.sessionStorage.getItem('port');
   var url='http://'+hostname+':'+port+'/ws/v1/timeline/TEZ_APPLICATION_ATTEMPT';

   $.ajax({
		type:'GET',
		url:url,
		dataType:'json',
		success: function(data)
		{
			$.each(data.entities,function(i,entity){
				var submittime,starttime, epoch_time, id, usr;
				epoch_time=entity.otherinfo.appSubmitTime;
				submittime=new Date(epoch_time);
				epoch_time=entity.starttime;
				starttime=new Date(epoch_time);
				id=entity.entity;
				usr=entity.primaryfilters.user[0];
				var callback= 'cookiegen("'+ id+'");>';
				t.row.add([
					submittime.toGMTString(),
					starttime.toGMTString(),
					'<a href="application_attempt_page.html" onclick='+ callback + id+" </a>",
					usr,
				]).draw();
			});	
		},
	});

} );
function cookiegen(id){
	window.sessionStorage.setItem('application_attempt_id',id);
}
