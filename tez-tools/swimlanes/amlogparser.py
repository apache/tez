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

import sys,re
from itertools import groupby
from bz2 import BZ2File
from gzip import GzipFile as GZFile
try:
	from urllib.request import urlopen
except:
	from urllib2 import urlopen as urlopen

class AMRawEvent(object):
	def __init__(self, ts, dag, event, args):
		self.ts = ts
		self.dag = dag
		self.event = event
		self.args = args
	def __repr__(self):
		return "%s->%s (%s)" % (self.dag, self.event, self.args)

def first(l):
	return (l[:1] or [None])[0]

def kv_add(d, k, v):
	if(d.has_key(k)):
		oldv = d[k]
		if(type(oldv) is list):
			oldv.append(v)
		else:
			oldv = [oldv, v]
		d[k] = oldv
	else:
		d[k] = v
			
def csv_kv(args):
	kvs = {};
	pairs = [p.strip() for p in args.split(",")]
	for kv in pairs:
		if(kv.find("=") == -1):
			kv_add(kvs, kv, None)
		elif(kv.find("=") == kv.rfind("=")):
			(k,v) = kv.split("=")
			kv_add(kvs, k, v)
	return kvs

class AppMaster(object):
	def __init__(self, raw):
		self.raw = raw
		self.kvs = csv_kv(raw.args)
		self.name = self.kvs["appAttemptId"]
		self.zero = int(self.kvs["startTime"])
		#self.ready = int(self.kvs["initTime"])
		#self.start = int(self.kvs["appSubmitTime"])
		self.containers = None
		self.dags = None
	def __repr__(self):
		return "[%s started at %d]" % (self.name, self.zero)

class DummyAppMaster(object):
	""" magic of duck typing """
	def __init__(self, dag):
		self.raw = None
		self.kvs = {}
		self.name = "Appmaster for %s" % dag.name
		self.zero = dag.start
		self.containers = None
		self.dags = None
	
class Container(object):
	def __init__(self, raw):
		self.raw = raw
		self.kvs = csv_kv(raw.args)
		self.name = self.kvs["containerId"]
		self.start = int(self.kvs["launchTime"])
		self.stop = -1 
		self.status = 0
		self.node =""
	def __repr__(self):
		return "[%s start=%d]" % (self.name, self.start)

class DummyContainer(object):
	def __init__(self, attempt):
		self.raw = None
		self.kvs = {}
		self.name = attempt.container
		self.status = 0
		self.start = attempt.start
		self.stop = -1
		self.status = 0
		self.node = None

class DAG(object):
	def __init__(self, raw):
		self.raw = raw
		self.name = raw.dag
		self.kvs = csv_kv(raw.args)
		self.start = (int)(self.kvs["startTime"])
		self.finish = (int)(self.kvs["finishTime"])
		self.duration = (int)(self.kvs["timeTaken"])
	def structure(self, vertexes):
		self.vertexes = [v for v in vertexes if v.dag == self.name]
	def attempts(self):
		for v in self.vertexes:
			for t in v.tasks:
				for a in t.attempts:
					if(a.dag == self.name):
						yield a
	def __repr__(self):
		return "%s (%d+%d)" % (self.name, self.start, self.duration)

class Vertex(object):
	def __init__(self, raw):
		self.raw = raw
		self.dag = raw.dag
		self.kvs = csv_kv(raw.args)
		self.name = self.kvs["vertexName"]
		self.initZero = (int)(self.kvs["initRequestedTime"])
		self.init = (int)(self.kvs["initedTime"])
		self.startZero = (int)(self.kvs["startRequestedTime"])
		self.start = (int)(self.kvs["startedTime"])
		self.finish = (int)(self.kvs["finishTime"])
		self.duration = (int)(self.kvs["timeTaken"])
	def structure(self, tasks):
		self.tasks = [t for t in tasks if t.vertex == self.name]
	def __repr__(self):
		return "%s (%d+%d)" % (self.name, self.start, self.duration)


class Task(object):
	def __init__(self, raw):
		self.raw = raw
		self.dag = raw.dag
		self.kvs = csv_kv(raw.args)
		self.vertex = self.kvs["vertexName"]
		self.name = self.kvs["taskId"]
		self.start = (int)(self.kvs["startTime"])
		self.finish = (int)(self.kvs["finishTime"])
		self.duration = (int)(self.kvs["timeTaken"])
	def structure(self, attempts):
		self.attempts = [a for a in attempts if a.task == self.name]
	def __repr__(self):
		return "%s (%d+%d)" % (self.name, self.start, self.duration)

class Attempt(object):
	def __init__(self, pair):
		start = first(filter(lambda a: a.event == "TASK_ATTEMPT_STARTED", pair))
		finish = first(filter(lambda a: a.event == "TASK_ATTEMPT_FINISHED", pair))
		if start is None or finish is None:
			print [start, finish];
		self.raw = finish
		self.kvs = csv_kv(start.args)
		if finish is not None:
			self.dag = finish.dag
			self.kvs.update(csv_kv(finish.args))
			self.finish = (int)(self.kvs["finishTime"])
			self.duration = (int)(self.kvs["timeTaken"])
		self.name = self.kvs["taskAttemptId"]
		self.task = self.name[:self.name.rfind("_")].replace("attempt","task")
		(_, _, amid, dagid, vertexid, taskid, attemptid) = self.name.split("_")
		self.tasknum = int(taskid)
		self.attemptnum = int(attemptid)
		self.vertex = self.kvs["vertexName"]
		self.start = (int)(self.kvs["startTime"])
		self.container = self.kvs["containerId"]
		self.node = self.kvs["nodeId"]
	def __repr__(self):
		return "%s (%d+%d)" % (self.name, self.start, self.duration)
		

def open_file(f):
	if(f.endswith(".gz")):
		return GZFile(f)
	elif(f.endswith(".bz2")):
		return BZ2File(f)
	elif(f.startswith("http://")):
		return urlopen(f)
	return open(f)

class AMLog(object):	
	def init(self):
		ID=r'[^\]]*'
		TS=r'[0-9:\-, ]*'
		MAIN_RE=r'^(?P<ts>%(ts)s) [?INFO]? [(?P<thread>%(id)s)] \|?(org.apache.tez.dag.)?history.HistoryEventHandler\|?: [HISTORY][DAG:(?P<dag>%(id)s)][Event:(?P<event>%(id)s)]: (?P<args>.*)'
		MAIN_RE = MAIN_RE.replace('[','\[').replace(']','\]')
		MAIN_RE = MAIN_RE % {'ts' : TS, 'id' : ID}
		self.MAIN_RE = re.compile(MAIN_RE)
	
	def __init__(self, f):
		fp = open_file(f)
		self.init()
		self.events = filter(lambda a:a, [self.parse(l.strip()) for l in fp])
	
	def structure(self):
		am = self.appmaster() # this is a copy
		containers = dict([(a.name, a) for a in self.containers()])
		dags = self.dags()
		vertexes = self.vertexes()
		tasks = self.tasks()
		attempts = self.attempts()
		for t in tasks:
			t.structure(attempts)
		for v in vertexes:
			v.structure(tasks)
		for d in dags:
			d.structure(vertexes)
		for a in attempts:
			if containers.has_key(a.container):
				c = containers[a.container]
				c.node = a.node
			else:
				c = DummyContainer(a)
				containers[a.container] = c
		if not am:
			am = DummyAppMaster(first(dags))
		am.containers = containers
		am.dags = dags
		return am

	def appmaster(self):
		return first([AppMaster(ev) for ev in self.events if ev.event == "AM_STARTED"])
	
	def containers(self):
		containers = [Container(ev) for ev in self.events if ev.event == "CONTAINER_LAUNCHED"]
		containermap = dict([(c.name, c) for c in containers])
		for ev in self.events:
			if ev.event == "CONTAINER_STOPPED":
				kvs = csv_kv(ev.args)
				if containermap.has_key(kvs["containerId"]):
					containermap[kvs["containerId"]].stop = int(kvs["stoppedTime"])
					containermap[kvs["containerId"]].status = int(kvs["exitStatus"])
		return containers
				
	
	def dags(self):
		dags = [DAG(ev) for ev in self.events if ev.event == "DAG_FINISHED"]
		return dags
	
	def vertexes(self):
		""" yes, not vertices """
		vertexes = [Vertex(ev) for ev in self.events if ev.event == "VERTEX_FINISHED"]
		return vertexes
	
	def tasks(self):
		tasks = [Task(ev) for ev in self.events if ev.event == "TASK_FINISHED"]
		return tasks
	
	def attempts(self):
		key = lambda a:a[0]
		value = lambda a:a[1]
		raw = [(csv_kv(ev.args)["taskAttemptId"], ev) for ev in self.events if ev.event == "TASK_ATTEMPT_STARTED" or ev.event == "TASK_ATTEMPT_FINISHED"]
		pairs = groupby(sorted(raw), key = key)
		attempts = [Attempt(map(value,p)) for (k,p) in pairs]
		return attempts
	
	def parse(self, l):		
		if(l.find("[HISTORY]") != -1):
			m = self.MAIN_RE.match(l)
			ts = m.group("ts")
			dag = m.group("dag")
			event = m.group("event")
			args = m.group("args")
			return AMRawEvent(ts, dag, event, args)

def main(argv):
	tree = AMLog(argv[0]).structure()
	# AM -> dag -> vertex -> task -> attempt
	# AM -> container
	for d in tree.dags:
		for a in d.attempts():
			print [a.vertex, a.name, a.container, a.start, a.finish]

if __name__ == "__main__":
	main(sys.argv[1:])
