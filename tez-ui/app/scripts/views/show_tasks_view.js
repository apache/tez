App.ShowTasksView = Em.View.extend({
	templateName: 'views/show_tasks',

	parentEntityType: '',
	parentEntityID: '',

	init: function() {
		this.controller.set('store', this.store);
		this.controller.set('content', this);
	},

	columns: function() {
    var columnHelper = function(columnName, valName) {
      return Em.Table.ColumnDefinition.create({
        textAlign: 'text-align-left',
        headerCellName: columnName,
        getCellContent: function(row) {
          return row.get(valName);
        }
      });
    }

    var idColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      //TODO: change hard coding.
      headerCellName: this.get('parentEntityType') == 'task' ? 'Task Attempt ID' : 'Task Id',
      tableCellViewClass: Em.Table.TableCell.extend({
      	template: Em.Handlebars.compile(
          this.get('parentEntityType') == 'task' ?
          "{{#link-to 'taskAttempt' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}" :
          "{{#link-to 'task' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
      }),
      getCellContent: function(row) {
      	return row.get('id');
      }
    });

    var vertexColumn = columnHelper('Vertex', 'vertexID');

    var startTimeColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Submission Time',
      getCellContent: function(row) {
      	return App.Helpers.date.dateFormat(row.get('startTime'));
      }
    });

    var endTimeColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'End Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('endTime'));
      }
    });

    var statusColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Status',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          '<span class="ember-table-content">&nbsp;\
          <i {{bind-attr class=":task-status view.cellContent.statusIcon"}}></i>\
          &nbsp;&nbsp;{{view.cellContent.status}}</span>')
      }),
      getCellContent: function(row) {
      	return { 
          status: row.get('status'),
          statusIcon: App.Helpers.misc.getStatusClassForEntity(row)
        };
      }
    });
    
    //TODO: for now a hack change this to take columns and type to show.
    if (this.get('parentEntityType') == 'task') {
      return [idColumn, startTimeColumn, endTimeColumn, statusColumn];
    } else {
      return [idColumn, vertexColumn, startTimeColumn, endTimeColumn, statusColumn];
    }
  }.property(),

});