#!/usr/bin/env python

import sys

if len(sys.argv) < 2:
	print "usage:", sys.argv[0], '<sample_point_csv> <output_path>'
	sys.exit(1)

sample_point_csv = sys.argv[1]
trace_id = sample_point_csv[-9:]

if sys.argv[2] == '-':
	output_file = sys.stdout
else:
	output_path = sys.argv[2] + "/" + trace_id
	output_file = open(output_path, 'w')

print >> sys.stderr, 'converting', sample_point_csv, '...'

sample_point_file = open(sample_point_csv, 'r')
prev_traj_id = ''
for line in sample_point_file:
    elements = line.rstrip('\n').split('\t')
    if elements[0] != prev_traj_id:
        prev_traj_id = elements[0]
        prev_point = (float(elements[1]), float(elements[2]))
        seg_id = 0
    else:
        output_file.write('%s\t%.6f\t%.6f\t%.6f\t%.6f\t%d\n' % (prev_traj_id, float(prev_point[0]), float(prev_point[1]), float(elements[1]), float(elements[2]), seg_id))
        prev_point = (elements[1], elements[2])
        seg_id += 1
sample_point_file.close()

if sys.argv[2] != '-':
    output_file.close()
