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

import sys,math,os.path
import StringIO
from amlogparser import AMLog
from getopt import getopt

class ColourManager(object):
	def __init__(self):
		# text-printable colours
		self.colours = [
		'#E4F5FC', '#62C2A2', '#E2F2D8', '#A9DDB4', '#E2F6E1', '#D8DAD7', '#BBBDBA', '#FEE6CE', '#FFCF9F',
		'#FDAE69', '#FDE4DD', '#EDE6F2', '#A5BDDB', '#FDE1EE', '#D8B9D8', '#D7DCEC', '#BABDDA', '#FDC5BF',
		'#FC9FB3', '#FDE1D2', '#FBBB9E', '#DBEF9F', '#AADD8E', '#81CDBB', '#C7EDE8', '#96D9C8', '#E3EBF4',
		'#BAD3E5', '#9DBDD9', '#8996C8', '#CEEAC6', '#76CCC6', '#C7E9BE', '#9ED99C', '#71C572', '#EFF1EE',
		'#949693', '#FD8D3D', '#FFF7ED', '#FED3AE', '#FEBB8F', '#FCE9CA', '#FED49B', '#FBBC85', '#FB8E58',
		'#FFEEE8', '#D0D0E8', '#76A9CE', '#FDFFFC', '#E9E2EE', '#64A8D2', '#FAF7FC', '#F6ECF2', '#F8E7F0',
		'#C994C6', '#E063B1', '#ECEDF7', '#DDD9EB', '#9B9BCA', '#FEDFDE', '#F8689F', '#FC9273', '#FC6948',
		'#F6FDB6', '#78C67B', '#EBF9B0', '#C5E9B0', '#40B7C7', '#FDF7BA', '#FFE392', '#FFC34C', '#FF982A']
		self.i = 0
	def next(self):
		self.i += 1
		return self.colours[self.i % len(self.colours)]

def attempts(tree):
	for d in tree.dags:
		for a in d.attempts():
			yield (a.vertex, a.name, a.container, a.start, a.finish)

def attrs(args):
	s = ""
	for k in args:
		v = args[k]
		k = k.replace("_","-") # css
		if type(v) is str:
			s += "%s='%s' " % (k,v)
		else:
			s += "%s=%s " % (k,str(v))
	return s

class SVGHelper(object):
	def __init__(self, w, h, parent=None):
		self.width = w		
		self.height = h
		self.parent = parent
		if(not parent):
			self.lines = StringIO.StringIO()
			self.write("""<?xml version="1.0" standalone="no"?>
		<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
		""")
		else:
			self.lines = parent.lines
		self.write("""<svg xmlns="http://www.w3.org/2000/svg" version="1.1" xmlns:xlink="http://www.w3.org/1999/xlink" height="%d" width="%d">""" % (h, w))	
		self.write("""
		<script type="text/ecmascript" xlink:href="http://code.jquery.com/jquery-2.1.1.min.js" />
		""")
	def line(self, x1, y1, x2, y2, style="stroke: #000", **kwargs):
		self.write("""<line x1="%d" y1="%d" x2="%d" y2="%d"  style="%s" %s />""" % (x1, y1, x2, y2, style, attrs(kwargs)))
	def rect(self, left, top, right, bottom, style="", title="", link=None):
		w = (right-left)
		h = (bottom-top)
		if link:
			self.write("<a xlink:href='%s'>" % link)
		self.write("""<rect x="%d" y="%d" width="%d" height="%d" style="%s"><title>%s</title></rect>""" % (left, top, w, h, style, title))
		if link:
			self.write("</a>")
	def text(self, x, y, text, style="", transform=""):
		self.write("""<text x="%d" y="%d" style="%s" transform="%s">%s</text>""" % (x, y, style, transform, text))
	def link(self, x, y, text, link, style=""):
		self.write("<a xlink:href='%s'>" % link)
		self.text(x, y, text, style)
		self.write("</a>")
	def write(self, s):
		self.lines.write(s)
	def flush(self):
		self.write("</svg>")
		if(self.parent):
			self.parent.flush()
		return self.lines.getvalue()

def usage():
	sys.stderr.write("""
usage: swimlane.py [-t ms-per-pixel] [-o outputfile] [-f redline-fraction] <log-file>

Input files for this tool can be prepared by "yarn logs -applicationId <application_...> | grep HISTORY".
""")

def main(argv):
	(opts, args) = getopt(argv, "o:t:f:")
	out = sys.stdout
	ticks = -1 # precision of 1/tick
	fraction = -1
	for k,v in opts:
		if(k == "-o"):
			out = open(v, "w")
		if(k == "-t"):
			ticks = int(v)
		if(k == "-f"):
			if(int(v) < 100):
				fraction = int(v)/100.0
	if len(args) == 0:
		return usage()
	log = AMLog(args[0]).structure()
	lanes = [c.name for c in sorted(log.containers.values(), key=lambda a: a.start)]
	marginTop = 128
	marginRight = 100;
	laneSize = 24
	y = len(lanes)*laneSize
	items = attempts(log)
	maxx = max([a[4] for a in items])
	if ticks == -1:
		ticks = min(1000, (maxx - log.zero)/2048)
	xdomain = lambda t : (t - log.zero)/ticks 
	x = xdomain(maxx)
	svg = SVGHelper(x+2*marginRight+256, y+2*marginTop)
	a = marginTop
	svg.text(x/2, 32, log.name, style="font-size: 32px; text-anchor: middle")	
	containerMap = dict(zip(list(lanes), xrange(len(lanes))))
	svg.text(marginRight - 16, marginTop - 32, "Container ID", "text-anchor:end; font-size: 16px;")
	# draw a grid
	for l in lanes:
		a += laneSize
		svg.text(marginRight - 4, a, l, "text-anchor:end; font-size: 16px;")
		svg.line(marginRight, a, marginRight+x, a, "stroke: #ccc")
	for x1 in set(range(0, x, 10*ticks)) | set([x]):
		svg.text(marginRight+x1, marginTop-laneSize/2, "%0.2f s" % ((x1 *  ticks)/1000), "text-anchor: middle; font-size: 12px")
		svg.line(marginRight+x1, marginTop-laneSize/2, marginRight+x1, marginTop+y, "stroke: #ddd")
	svg.line(marginRight, marginTop, marginRight+x, marginTop)
	svg.line(marginRight, y+marginTop, marginRight+x, y+marginTop)
	svg.line(marginRight, marginTop, marginRight, y+marginTop)
	svg.line(marginRight+x, marginTop, marginRight+x, y+marginTop)
	
	colourman = ColourManager()
	for c in log.containers.values():
		y1 = marginTop+(containerMap[c.name]*laneSize)
		x1 = marginRight+xdomain(c.start)
		svg.line(x1, y1, x1, y1 + laneSize, style="stroke: green")
		if c.stop > c.start:
			x2 = marginRight+xdomain(c.stop)
			if (c.status == 0):
				svg.line(x2, y1, x2, y1 + laneSize, style="stroke: green")
			else: 
				svg.line(x2, y1, x2, y1 + laneSize, style="stroke: red")
				svg.text(x2, y1, "%d" % (c.status), style="text-anchor: right; font-size: 12px; stroke: red", transform="rotate(90, %d, %d)" % (x2, y1)) 
			svg.rect(x1, y1, x2, y1 + laneSize, style="fill: #ccc; opacity: 0.3")
		elif c.stop == -1:
			x2 = marginRight+x 
			svg.rect(x1, y1, x2, y1 + laneSize, style="fill: #ccc; opacity: 0.3")
	for dag in log.dags:
		x1 = marginRight+xdomain(dag.start)
		svg.line(x1, marginTop-24, x1, marginTop+y, "stroke: black;", stroke_dasharray="8,4")
		x2 = marginRight+xdomain(dag.finish)
		svg.line(x2, marginTop-24, x2, marginTop+y, "stroke: black;", stroke_dasharray="8,4")
		svg.line(x1, marginTop-24, x2, marginTop-24, "stroke: black")
		svg.text((x1+x2)/2, marginTop-32, "%s (%0.1f s)" % (dag.name, (dag.finish-dag.start)/1000.0) , "text-anchor: middle; font-size: 12px;")		
		vertexes = set([v.name for v in dag.vertexes])
		colourmap = dict([(v,colourman.next()) for v in list(vertexes)])
		for c in dag.attempts():
			colour = colourmap[c.vertex]
			y1 = marginTop+(containerMap[c.container]*laneSize)+1
			x1 = marginRight+xdomain(c.start)
			x2 = marginRight+xdomain(c.finish)
			y2 = y1 + laneSize - 2
			locality = (c.kvs.has_key("DATA_LOCAL_TASKS") * 1) + (c.kvs.has_key("RACK_LOCAL_TASKS")*2)
			#CompletedLogs may not be present in latest tez logs
			link = c.kvs.get("completedLogs", "")
			svg.rect(x1, y1, x2, y2, title=c.name, style="fill: %s; stroke: #ccc;" % (colour), link=link)
			if locality > 1: # rack-local (no-locality isn't counted)
				svg.rect(x1, y2-4, x2, y2, style="fill: #f00; fill-opacity: 0.5;", link=link)
			if x2 - x1 > 64:
				svg.text((x1+x2)/2, y2-12, "%s (%05d_%d)" % (c.vertex, c.tasknum, c.attemptnum), style="text-anchor: middle; font-size: 9px;")
			else:
				svg.text((x1+x2)/2, y2-12, "%s" % c.vertex, style="text-anchor: middle; font-size: 9px;")
		finishes = sorted([c.finish for c in dag.attempts()])
		if(len(finishes) > 10 and fraction > 0):
			percentX = finishes[int(len(finishes)*fraction)]
			svg.line(marginRight+xdomain(percentX), marginTop, marginRight+xdomain(percentX), y+marginTop, style="stroke: red")
			svg.text(marginRight+xdomain(percentX), y+marginTop+12, "%d%% (%0.1fs)" % (int(fraction*100), (percentX - dag.start)/1000.0), style="font-size:12px; text-anchor: middle")
	out.write(svg.flush())
	out.close()

if __name__ == "__main__":
	sys.exit(main(sys.argv[1:]))
