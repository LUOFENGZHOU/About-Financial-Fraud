#-*- coding: UTF-8 -*-

import os
import sys
import pickle
import re
import  codecs
import chardet
import string
import shutil
import csv
import time
import pandas as pd
import gc
gc.disable()

path_cart="e:\\Users\\Desktop\\ppp\\17.csv"
path_trd="e:\\Users\\Desktop\\ppp\\data_17.csv"

data_cart=pd.read_csv(path_cart, error_bad_lines=False, usecols=[11,16],encoding='utf8')
var_stk_date=[]
var_stk_id=[]

for indexs in data_cart.index:
    date=str(data_cart.loc[indexs].values[:][:][:][0])
    id=str(data_cart.loc[indexs].values[:][:][:][1]).zfill(6)
    date=time.strptime(date, "%Y/%m/%d")
    date=int(time.mktime(date))
    var_stk_date.append(date)
    var_stk_id.append(id)


data=pd.read_csv(path_cart, error_bad_lines=False,encoding='utf8')
data_trd=pd.read_csv(path_trd, error_bad_lines=False, usecols=[0,1,2],encoding='utf8')
frames_dretend=[]
for id in var_stk_id:
    current_date_trd = []
    current_dretend_trd = []
    i=0
    for indexs in data_trd.index:
        id_trd=str(int(float(str(data_trd.loc[indexs].values[:][:][:][0])))).zfill(6)
        if("证券代码" not in id_trd):
            if("没有单位" not in id_trd):
                if(int(id_trd)>int(id)):
                    break
        #print(indexs)
        if(id!=id_trd):
            continue
        else:
            date_trd = str(data_trd.loc[indexs].values[:][:][:][1])
            dretnd_trd = str(data_trd.loc[indexs].values[:][:][:][2])
            date_trd = time.strptime(date_trd, "%Y/%m/%d")
            date_trd = int(time.mktime(date_trd))
            current_date_trd.append(date_trd)
            current_dretend_trd.append(dretnd_trd)
            # i=i+1
            # print(i)

    #前十个
    result_dretend=[]
    result_dretend_temp=[]
    num=var_stk_id.index(id)
    date=var_stk_date[num]
    date_temp=date-86400
    for i in range(0,10):
        while (date_temp not in current_date_trd):
            date_temp-=86400
        num_dretend=current_date_trd.index(date_temp)
        result_dretend_temp.append(current_dretend_trd[num_dretend])
        date_temp-=86400
    if(len(result_dretend_temp)==10):
        for i in range(0,10):
            result_dretend.append(result_dretend_temp[9-i])
    else:
        print("数量不为10")
    #初始化
    result_dretend_temp=[]
    date_temp=date
    #本身
    while(date_temp not in current_date_trd):
        date_temp+=86400
    num_dretend=current_date_trd.index(date_temp)
    result_dretend.append(current_dretend_trd[num_dretend])
    #初始化
    date_temp=date
    #后60个
    for i in range(0,60):
        while(date_temp not in current_date_trd):
            date_temp+=86400
        num_dretend=current_date_trd.index(date_temp)
        result_dretend.append(current_dretend_trd[num_dretend])
        element_max=0
        for element in current_date_trd:
            if(element>date_temp):
                element_max=element
        if element_max==0:
            result_dretend.append("null")
            print("null error")
            break
        date_temp+=86400
    if(len(result_dretend)!=71):
        print(id)
        print("数量不为71")
    frames_dretend.append(result_dretend)
gc.enable()
list=[]
for i in range(-10,61):
    list.append(str(i))
result = pd.DataFrame(frames_dretend,columns=list.copy())
dataframe = pd.concat([data, result], axis=1, join_axes=[data.index])
dataframe.to_csv("test_17.csv", index=False, sep=',', encoding='gbk')
print("Done!")
input()