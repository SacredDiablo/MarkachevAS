#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8
import numpy as np
import time
import sys


# In[30]:


if int(sys.argv[1]) == 1:
    m = int(input('Введите M:'))
    n = int(input('Введите N:'))
    array = np.random.randint(2, size=(m, n))
if int(sys.argv[1]) == 2:
    array = []
    with open(input('Введите имя файла: ')) as f:
        for line in f:
            array.append([int(x) for x in line.split()])
    m = len(array)
    n = len(array[0])


# In[27]:


t=time.time()
a = True
z = 0
while a == True:
    z+=1
    count = 0
    new_array = np.zeros((m,n), np.int)
    for i in range(0,m):
        for j in range(0,n):
            if i == 0 and j == 0:
                count = int( array[i+1][j] + array[i+1][j+1]+ array[i][j+1])
            elif i == m - 1 and j == n - 1:
                count = int( array[i-1][j] + array[i-1][j-1]+ array[i][j-1])
            elif i == 0 and j == n - 1:
                    count = int( array[i+1][j] + array[i+1][j-1]+ array[i][j-1])
            elif i == m-1 and j == 0:
                    count = int( array[i-1][j] + array[i-1][j+1]+ array[i][j+1])
            elif i == 0:
                count = int(array[i+1][j-1] + array[i+1][j] + array[i+1][j+1]                        + array[i][j-1] + array[i][j+1])

            elif i == m-1:
                count = int(array[i-1][j-1] + array[i-1][j] + array[i-1][j+1]                        + array[i][j-1] + array[i][j+1])
            elif j == 0:
                count = int(array[i-1][j] + array[i-1][j+1]                        + array[i+1][j] + array[i+1][j+1]                        + array[i][j+1])
            elif j == n-1:
                count = int(array[i-1][j-1] + array[i-1][j]                        + array[i+1][j-1] + array[i+1][j]                        + array[i][j-1])        
    
            elif i>0 and j>0 and i<m-1 and j<n-1:
                count = int(array[i-1][j-1] + array[i-1][j] + array[i-1][j+1]                    + array[i+1][j-1] + array[i+1][j] + array[i+1][j+1]                    + array[i][j-1] + array[i][j+1])
          
            if array[i][j] == 0:
                if count == 3:
                    new_array[i][j] = 1
                else: new_array[i][j] = 0
            if array[i][j] == 1:            
                if count<2 or count>3:
                    new_array[i][j]=0
                elif count == 2 or count == 3:
                    new_array[i][j]=array[i][j]
    if np.array_equal(array, new_array) == False:
        array = new_array               
        print ('Iteration: ', z)
        print (array)
    else: 
        a = False
    time.sleep(1)   
print('Best array: ', z-1)
            

