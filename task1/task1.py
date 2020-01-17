#!/usr/bin/env python
# coding: utf-8

import sys
import numpy as np
file = open(sys.argv[1])
data = [int(line) for line in file]
print ("%.2f" %np.percentile(data,90))
print ("%.2f" %max(set(data), key=data.count))
print ("%.2f" %max(data))
print ("%.2f" %min(data))
print ("%.2f" %np.average(data))
file.close()
