App.TasksView = Em.View.extend({
	templateName: 'views/tasks_view',

	init: function() {
	},
	parentEntityType: '', 
	parentEntityID: '',

	tasks: [],

	getTasks: function() {
		return;
		var store = this.get('controller').get('store');
		store.findQuery('task', {
			primaryFilter: 'TEZ_DAG_ID:' + this.get('parentEntityID')
		}).then(function() {
		});
	}.observes('parentEntityID'),

	foo: function() {
		var x = this.get('parentEntityID');
	}.on('didInsertElement')
});
