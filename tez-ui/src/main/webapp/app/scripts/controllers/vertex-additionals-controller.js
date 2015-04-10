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

App.VertexAdditionalsController = Em.ObjectController.extend({
  needs: 'vertex',

  controllerName: 'VertexAdditionalsController',

  loadEntities: function() {
    var inputs = this.get('inputs.content'),
        outputs = this.get('outputs.content');

    this.set('inputContent', inputs);
    this.set('inputsAvailable', inputs.length > 0);

    this.set('outputContent', outputs);
    this.set('outputsAvailable', outputs.length > 0);

    this.set('loading', false);
  },

  inputColumns: function() {
    return App.Helpers.misc.createColumnDescription([
      {
        id: 'inputId',
        headerCellName: 'Name',
        contentPath: 'inputName',
        searchAndSortable: false,
      },
      {
        id: 'inputClass',
        headerCellName: 'Class',
        contentPath: 'inputClass',
        searchAndSortable: false,
      },
      {
        id: 'inputInitializer',
        headerCellName: 'Initializer',
        contentPath: 'inputInitializer',
        searchAndSortable: false,
      },
      {
        id: 'configurations',
        headerCellName: 'Configurations',
        searchAndSortable: false,
        templateName: 'components/basic-table/linked-cell',
        getCellContent: function(row) {
          if(row.get('configs.content.length')) {
            return {
              linkTo: 'input.configs',
              displayText: 'View Configurations',
              entityId: row.get('id')
            };
          }
        }
      }
    ]);
  }.property(),

  outputColumns: function() {
    return App.Helpers.misc.createColumnDescription([
      {
        id: 'outputId',
        headerCellName: 'Name',
        contentPath: 'outputName',
        searchAndSortable: false,
      },
      {
        id: 'outputClass',
        headerCellName: 'Class',
        contentPath: 'outputClass',
        searchAndSortable: false,
      },
      {
        id: 'configurations',
        headerCellName: 'Configurations',
        searchAndSortable: false,
        templateName: 'components/basic-table/linked-cell',
        getCellContent: function(row) {
          if(row.get('configs.content.length')) {
            return {
              linkTo: 'output.configs',
              displayText: 'View Configurations',
              entityId: row.get('id')
            };
          }
        }
      }
    ]);
  }.property(),

});