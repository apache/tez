#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import imp, json, os, shutil, sys, tempfile, zipfile
try:
    imp.find_module('texttable')
    from texttable import Texttable
except ImportError:
	sys.stderr.write("Could not import Texttable\nRetry after 'pip install texttable'\n")
	exit()

tmpdir = tempfile.mkdtemp()

def extract_zip(filename):
	file_dir = os.path.join(tmpdir, os.path.splitext(filename)[0])
	if not os.path.exists(file_dir):
		os.makedirs(file_dir)

	zip_ref = zipfile.ZipFile(os.path.abspath(filename), 'r')
	zip_ref.extractall(os.path.abspath(file_dir))
	zip_ref.close()
	return file_dir


def diff(file1, file2):
	# extract ZIP files
	file1_dir = extract_zip(file1)
	file2_dir = extract_zip(file2)

	# tez debugtool writes json data to TEZ_DAG file whereas tez UI writes to dag.json
	# also in dag.json data is inside "dag" root node
	file1_using_dag_json = True
	dag_json_file1 = os.path.join(file1_dir, "dag.json")
	if os.path.isfile(dag_json_file1) == False:
		file1_using_dag_json = False
		dag_json_file1 = os.path.join(file1_dir, "TEZ_DAG")
		if os.path.isfile(dag_json_file1) == False:
			print "Unable to find dag.json/TEZ_DAG file inside the archive " + file1
			exit()

	file2_using_dag_json = True
	dag_json_file2 = os.path.join(file2_dir, "dag.json")
	if os.path.isfile(dag_json_file2) == False:
		file2_using_dag_json = False
		dag_json_file2 = os.path.join(file2_dir, "TEZ_DAG")
		if os.path.isfile(dag_json_file2) == False:
			print "Unable to find dag.json/TEZ_DAG file inside the archive " + file1
			exit()

	# populate diff table
	difftable = {}
	with open(dag_json_file1) as data_file:
		file1_dag_json = json.load(data_file)["dag"] if file1_using_dag_json else json.load(data_file)
		counters = file1_dag_json['otherinfo']['counters']
		for group in counters['counterGroups']:
			countertable = {}
			for counter in group['counters']:
				counterName = counter['counterName']
				countertable[counterName] = []
				countertable[counterName].append(counter['counterValue'])

			groupName = group['counterGroupName']
			difftable[groupName] = countertable

		# add other info
		otherinfo = file1_dag_json['otherinfo']
		countertable = {}
		countertable["TIME_TAKEN"] = [otherinfo['timeTaken']]
		countertable["COMPLETED_TASKS"] = [otherinfo['numCompletedTasks']]
		countertable["SUCCEEDED_TASKS"] = [otherinfo['numSucceededTasks']]
		countertable["FAILED_TASKS"] = [otherinfo['numFailedTasks']]
		countertable["KILLED_TASKS"] = [otherinfo['numKilledTasks']]
		countertable["FAILED_TASK_ATTEMPTS"] = [otherinfo['numFailedTaskAttempts']]
		countertable["KILLED_TASK_ATTEMPTS"] = [otherinfo['numKilledTaskAttempts']]
		difftable['otherinfo'] = countertable

	with open(dag_json_file2) as data_file:
		file2_dag_json = json.load(data_file)["dag"] if file2_using_dag_json else json.load(data_file)
		counters = file2_dag_json['otherinfo']['counters']
		for group in counters['counterGroups']:
			groupName = group['counterGroupName']
			if groupName not in difftable:
				difftable[groupName] = {}
			countertable = difftable[groupName]
			for counter in group['counters']:
				counterName = counter['counterName']
				# if counter does not exist in file1, add it with 0 value
				if counterName not in countertable:
					countertable[counterName] = [0]
				countertable[counterName].append(counter['counterValue'])

		# append other info
		otherinfo = file2_dag_json['otherinfo']
		countertable = difftable['otherinfo']
		countertable["TIME_TAKEN"].append(otherinfo['timeTaken'])
		countertable["COMPLETED_TASKS"].append(otherinfo['numCompletedTasks'])
		countertable["SUCCEEDED_TASKS"].append(otherinfo['numSucceededTasks'])
		countertable["FAILED_TASKS"].append(otherinfo['numFailedTasks'])
		countertable["KILLED_TASKS"].append(otherinfo['numKilledTasks'])
		countertable["FAILED_TASK_ATTEMPTS"].append(otherinfo['numFailedTaskAttempts'])
		countertable["KILLED_TASK_ATTEMPTS"].append(otherinfo['numKilledTaskAttempts'])
		difftable['otherinfo'] = countertable

	# if some counters are missing, consider it as 0 and compute delta difference
	for k,v in difftable.items():
		for key, value in v.items():
			# if counter value does not exisit in file2, add it with 0 value
			if len(value) == 1:
				value.append(0)

			# store delta difference
			delta = value[1] - value[0]
			value.append(("+" if delta > 0 else "") + str(delta))

	return difftable

def print_table(difftable, name1, name2, detailed=False):
	table = Texttable(max_width=0)
	table.set_cols_align(["l", "l", "l", "l", "l"])
	table.set_cols_valign(["m", "m", "m", "m", "m"])
	table.add_row(["Counter Group", "Counter Name", name1, name2, "delta"]);
	for k in sorted(difftable):
		# ignore task specific counters in default output
		if not detailed and ("_INPUT_" in k or "_OUTPUT_" in k):
			continue

		v = difftable[k]
		row = []
		# counter group. using shortname here instead of FQCN
		if detailed:
			row.append(k)
		else:
			row.append(k.split(".")[-1])

		# keys as list (counter names)
		row.append("\n".join(list(v.keys())))

		# counter values for dag1
		for key, value in v.items():
			if len(value) == 1:
				value.append(0)
			value.append(value[0] - value[1])

		# dag1 counter values
		name1Val = []
		for key, value in v.items():
			name1Val.append(str(value[0]))
		row.append("\n".join(name1Val))

		# dag2 counter values
		name2Val = []
		for key, value in v.items():
			name2Val.append(str(value[1]))
		row.append("\n".join(name2Val))

		# delta values
		deltaVal = []
		for key, value in v.items():
			deltaVal.append(str(value[2]))
		row.append("\n".join(deltaVal))

		table.add_row(row)

	print table.draw() + "\n"


def main(argv):
	sysargs = len(argv)
	if sysargs < 2:
		print "Usage: python counter-diff.py dag_file1.zip dag_file2.zip [--detail]"
		return -1

	file1 = argv[0]
	file2 = argv[1]
	difftable = diff(file1, file2)

	detailed = False
	if sysargs == 3 and argv[2] == "--detail":
		detailed = True

	print_table(difftable, os.path.splitext(file1)[0], os.path.splitext(file2)[0], detailed)

if __name__ == "__main__":
	try:
		sys.exit(main(sys.argv[1:]))
	finally:
		shutil.rmtree(tmpdir)