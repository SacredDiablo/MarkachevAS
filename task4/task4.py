#!/usr/bin/env python
# coding: utf-8

import sys

text = open(sys.argv[1],'r')
line = text.read().split("\n")
text.close()

#получение данных из файла, разбиение на приходящих и уходящих посетителей
all_time = []
prihod = []
uhod = []
for i in range(len(line)):
    all_time += str(line[i]).split(' ')
for i in range(len(all_time)):
    if i%2 == 0: prihod.append(all_time[i])
    else: uhod.append(all_time[i])
#перевод из времени в числа для дальнейшего использования        
prihod_time = []
uhod_time = []
for i in range(len(prihod)):
    c = prihod[i].split(':')
    prihod_time.append(int(c[0])*60+int(c[1]))
    c = uhod[i].split(':')
    uhod_time.append(int(c[0])*60+int(c[1]))

uhod_time =sorted(uhod_time)
prihod_time = sorted(prihod_time)

#приведение в поминутную градацию
all_in = []
for j in range(len(prihod_time)):
    for i in range(prihod_time[j],uhod_time[j]):
        all_in.append(i)

#составление словаря
counter = {}
end_data = []
for elem in all_in:
    counter[elem] = counter.get(elem, 0) + 1

doubles = {element: count for element, count in counter.items()}
for i in doubles:
    if doubles[i] == max(doubles.values()):
        end_data.append(i)        
#подсчет        
prihod1 = []
uhod1 =[]
for i in range(len(end_data)):
    if i == 0:
        if ((end_data[i]+1)%60) > 0:
            prihod1.append(str(end_data[i]//60)+':'+str(end_data[i]%60))
        else: 
            prihod1.append(str((end_data[i]+1)//60)+':00')
            
    elif len(end_data)-1 > i > 0 and end_data[i+1]- end_data[i]>1:
        if ((end_data[i]+1)%60) > 0:
            uhod1.append(str((end_data[i]+1)//60)+':'+str((end_data[i]+1)%60))
            prihod1.append(str((end_data[i+1])//60)+':'+str((end_data[i+1])%60))
        else: 
            uhod1.append(str((end_data[i]+1)//60)+':00')
            prihod1.append(str((end_data[i+1])//60)+':'+str((end_data[i+1])%60))
            
    elif len(end_data)-1 == i:
        if ((end_data[i]+1)%60) > 0:
            uhod1.append(str((end_data[i])//60)+':'+str((end_data[i]+1)%60))
        else: 
            uhod1.append(str((end_data[i]+1)//60)+':00')
#вывод
print_end = []
for i in range(len(prihod1)):
    print_end.append(str(prihod1[i])+' '+str(uhod1[i]))
for i in range(len(print_end)):
    print(print_end[i])

