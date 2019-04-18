import random
import sys
k = int(sys.argv[1])
# k = 6

with open("input/normalized_data.csv") as f:
    with open("centroids0/part",'w') as w:
        data = f.read()[:1600].split("\n")
        random.shuffle(data)
        for i in range(k):
            w.write(str(i)+":"+data[i]+"\n")
    
            
