import sys

dic = {}
with open(sys.argv[1]) as f:
    for line in f:
        key,val = line.strip().split(':')
        if key not in dic:
            dic[key] = {}
            dic[key]["set"] = {}
            dic[key]["points"] = 0
        if(val.split(";")[-1] not in dic[key]["set"]):
            dic[key]["set"][val.split(";")[-1]] = 0
        dic[key]["set"][val.split(";")[-1]] += 1
        dic[key]["points"] += 1
s = 0
for key,value in dic.iteritems():
    s += value['points']
    print key,value
print "SUM",s
