import sys

file = open("input/normalized_data.csv", "r")
l = []

text = file.read()
file.close()
i = 0
while (i<4000):
    l.append(text)
    i += 1

final_text = "".join(l)

writer = open("input/normalized_data.csv", "w")
writer.write(final_text)
writer.close()
