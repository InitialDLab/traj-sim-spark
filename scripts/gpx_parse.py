#!/usr/bin/env python

import gpxpy
import gpxpy.gpx
import sys

if len(sys.argv) < 2:
    print "usage:", sys.argv[0], "<gpx_file_name> <output_path>"
    sys.exit(1)

gpx_file_name = sys.argv[1]
trace_id = gpx_file_name[-13:-4]
if sys.argv[2] == "-":
    output_file = sys.stdout
else:
    output_path = sys.argv[2] + '/' + trace_id
    output_file = open(output_path, 'w')

print >> sys.stderr, 'parsing', gpx_file_name, '...'

gpx_file = open(gpx_file_name, 'r')
gpx = gpxpy.parse(gpx_file)

i = 0
for track in gpx.tracks:
	cur_trace_id = trace_id + '-' + str(i)
	for segment in track.segments:
		for point in segment.points:
			output_file.write('{0}\t{1:.6f}\t{2:.6f}\n'.format(cur_trace_id, point.latitude, point.longitude))
	i = i + 1

gpx_file.close()
if sys.argv[2] != "-":
    output_file.close()
#print 'GPX:', gpx.to_xml()
