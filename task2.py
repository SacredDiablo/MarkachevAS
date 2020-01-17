#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import numpy as np
text = open(sys.argv[1],'r')
text2 = open(sys.argv[2],'r')


# In[6]:


data, X, Y = [], [], []
for line in text:
    data += line.split(' ')
for i in range(len(data)):
    if i%2 == 0:
        X.append(float(data[i]))
    else: Y.append(float(data[i]))
text.close()


# In[7]:


data2, X2, Y2 = [], [], []
for line in text2:
    data2 += line.split(' ')
for i in range(len(data2)):
    if i%2 == 0:
        X2.append(float(data2[i]))
    else: Y2.append(float(data2[i]))
text2.close()


# In[4]:


for i in range(len(X2)):
    if (X2[i] > X[0] and Y2[i] > Y[0]) and (X2[i] > X[1] and Y2[i] < Y[1])     and (X2[i] < X[2] and Y2[i] < Y[2]) and (X2[i] < X[3] and Y2[i] > Y[3]):
        print(2)
    elif (X2[i] == X[0] and Y2[i] == Y[0]) or (X2[i] == X[1] and Y2[i] == Y[1])     or (X2[i] == X[2] and Y2[i] == Y[2]) or (X2[i] == X[3] and Y2[i] == Y[3]):
        print(0)   
    elif (X2[i] == X[0] and Y2[i] > Y[0]) and (X2[i] == X[1] and Y2[i] < Y[1])     or (X2[i] == X[2] and Y2[i] < Y[2]) and (X2[i] == X[3] and Y2[i] > Y[3]):
        print(1) 
    else: print(3)


# In[ ]:




