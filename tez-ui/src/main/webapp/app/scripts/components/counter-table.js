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

App.CounterTableComponent = Ember.Table.EmberTableComponent.extend({
	hasFooter: false,
	hasHeader: true,
	forceFillColumns: true,
	data: null,

	columns: function() {
		var groupColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Group',
      getCellContent: function(row) {
      	return row.get('counterGroup');
      }
    });

		var nameColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Counter Name',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          '<span {{bind-attr class=":ember-table-content view.cellContent.isCG:countertable-group-header:countertable-row"}}>\
          	{{view.cellContent.name}}\
           </span>')
      }),
      getCellContent: function(row) {
      	return {
      		isCG: row.get('counters') != undefined,
      		name: row.get('name')
      	};
      }
    });

		var valueColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Value',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          '<span {{bind-attr class=":ember-table-content view.cellContent.isCG:countertable-group-header"}}>\
          	{{view.cellContent.value}}\
           </span>')
      }),
      getCellContent: function(row) {
      	return {
      		isCG: row.get('counters') != undefined,
      		value: row.get('value')
      	};
      }
    });

    return [nameColumn, valueColumn];
	}.property(),

	content: function() {
		var allCounters = [];
		if (!!this.data) {
			this.data.forEach(function(cg){
				allCounters.push(cg);
				[].push.apply(allCounters, cg.get('counters').content);
			});
		}
		return allCounters;
	}.property('data'),
});

Em.Handlebars.helper('counter-table-component', App.CounterTableComponent);