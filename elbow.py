import os
import json

cmd = '/Users/vivinwilson/LSDP/hadoop-2.8.5/bin/hadoop jar target/mr-demo-1.0.jar kmeans.KMeans input output '

results = {}
for i in range(1,16):
    os.system('rm -rf centroids{1..100}')
    os.system('rm -rf output')
    os.system('python gen_centroids0.py '+str(i))
    os.system(cmd+str(i))
    results[i] = os.popen('python gen_score.py').read().strip()
print "OUTPUT :"
for i in results:
    print i,results[i]
