import numpy as np
#import matplotlib.pyplot as plt
#import seaborn as sns

#sns.set(color_codes=True)

dis_arr = []
pts_arr = []
span_arr = []

f = open('osm_de_stat.txt', 'r')

for line in f:
	splitted = line.split()
	pts_arr.append(int(splitted[1]))
	dis_arr.append(float(splitted[2]))
	span_arr.append(float(splitted[3]))

# Filters for BJTaxi...
#data = filter(lambda x: x[1] < 129 and x[1] > 13 and x[2] > 0.01, zip(dis_arr, pts_arr, span_arr))
# Filters For gen_traj_10M
#data = filter(lambda x: x[1] < 523 and x[1] > 20 and x[2] > 0.1, zip(dis_arr, pts_arr, span_arr))
# Filters for OSM_traj
#data = filter(lambda x: x[2] > 0.001 and x[2] < 0.50 and x[1] > 50 and x[1] < 4670, zip(dis_arr, pts_arr, span_arr))

data = filter(lambda x: x[1] > 20 and x[2] > 0.001 and x[2] < 0.5080, zip(dis_arr, pts_arr, span_arr))
print "# of Traj after filter:", len(data)

pts_arr = map(lambda x: x[1], data)
span_arr = map(lambda x: x[2], data)
dis_arr = map(lambda x: x[0], data)

print "variances (pts, dis, span):"
print np.var(pts_arr)
print np.var(dis_arr)
print np.var(span_arr)
print "max min (pts, dis, span):"
print max(pts_arr), ' ', min(pts_arr)
print max(dis_arr), ' ', min(dis_arr)
print max(span_arr), ' ', min(span_arr)
print "standard deviation (pts, dis, span):"
print np.std(pts_arr)
print np.std(dis_arr)
print np.std(span_arr)
print "average (pts, dis, span):"
print np.average(pts_arr)
print np.average(dis_arr)
print np.average(span_arr)
print "median deviation (pts, dis, span):"
print np.median(pts_arr)
print np.median(dis_arr)
print np.median(span_arr)
print "5% 95% percentile (pts, dis, span)"
print np.percentile(pts_arr, 5), ' ', np.percentile(pts_arr, 95)
print np.percentile(dis_arr, 5), ' ', np.percentile(dis_arr, 95)
print np.percentile(span_arr, 5), ' ', np.percentile(span_arr, 95)

#print map(lambda x: x[1], data)

#span_plot = sns.distplot(span_arr)
#span_plot.get_figure().savefig("osm_traj_span.png")
#pts_plot = sns.distplot(pts_arr)
#pts_plot.get_figure().savefig("osm_traj_pts.png")
#dis_plot = sns.distplot(dis_arr)
#dis_plot.get_figure().savefig("osm_traj_dis.png")
