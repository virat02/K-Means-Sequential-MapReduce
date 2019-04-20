import json

min_max = {}
with open("data/winequality-red.csv") as f:
    for line in f:
        for i,j in enumerate([float(i) for i in line.strip().split(";")][:-1]):
            if not i in min_max:
                min_max[i] = {"max":None,"min":None}

            if min_max[i]["min"] == None or min_max[i]["min"] > j:
                min_max[i]["min"] = j

            if min_max[i]["max"] == None or min_max[i]["max"] < j:
                min_max[i]["max"] = j

for k in min_max:
    min_max[k]["diff"] = min_max[k]["max"] - min_max[k]["min"]

with open("data/winequality-red.csv") as f:
    with open("input/normalized_data.csv",'w') as w:
        for line in f:
            data = []
            for i,j in enumerate([float(i) for i in line.strip().split(";")]):
                if i not in min_max:
                    data.append(str(int(j)))
                else:
                    data.append(str((j - min_max[i]["min"])/min_max[i]["diff"]))
            w.write(";".join(data)+"\n")

            
