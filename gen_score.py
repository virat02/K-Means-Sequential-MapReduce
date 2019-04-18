import os
import math

#dir_name = os.popen('ls -th | grep centroid | head -1').read().strip()
dir_name = "output"
cluster_file = dir_name + "/" + os.popen("ls "+dir_name+"/ | grep cluster").read().strip()
centroids_file = dir_name + "/" + os.popen("ls "+dir_name+"/ | grep part").read().strip()

centroids = {}

with open(centroids_file) as f:
    for i in f.read().strip().split("\n"):
        el = i.split(":")
        centroids[el[0]] = [float(i) for i in el[1].split(";")]

sqe = {}
with open(cluster_file) as f:
    for line  in f:
        el = line.strip().split(":")
        if el[0] not in sqe:
            sqe[el[0]] = 0
        s = 0
        for i,j in zip(centroids[el[0]],[float(i) for i in el[1].split(";")]):
            s += (i - j)**2
        sqe[el[0]] += math.sqrt(s)

total_score = 0
for k,v in sqe.iteritems():
    total_score += v

print total_score
