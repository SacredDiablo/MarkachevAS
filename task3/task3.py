#!/usr/bin/env python
# coding: utf-8

import sys
import numpy as np
text = sys.argv[1]

z = [[],[],[],[],[]]
for i in range(5):
    with open(text+'\\'+'Cash'+str(i+1)+'.txt') as txt:
        for cnt,line in enumerate(txt):
            z[i].append(float(line))

c = 0
x = []
i = 0
j  =0
while j < len(z[0]):
    while i < 5:
        c += float(z[i][j])
        i += 1
    x.append(c)   
    i = 0
    c = 0
    j += 1

print(x.index(max(x))+1)

