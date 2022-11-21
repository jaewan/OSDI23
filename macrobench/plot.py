#! /usr/bin/env python3

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm

application = "Push based shuffle"

path_prefix = "../data/push_based_shuffle_large/"
headers = ["runtime" ,"spilled_amount", "spilled_objects", "write_throughput", "restored_amount", "restored_objects", "read_throughput"]

files = ["DFS0", "RAY0", "DFS1", "RAY1", "DFS2", "RAY2", "DFS_EagerSpill0", "DFS_EagerSpill1", "DFS_EagerSpill2"]
for file in files:
    df = pd.read_csv(path_prefix+file+".csv")
    print(file)
    print(df[headers[0]].mean())
    print(df[headers[0]].std())
    #print(df.describe().iloc[1,:])

'''
files = ["RAY","DFS","DFS_Backpressure","DFS_BlockSpill","DFS_Backpressure_BlockSpill_Deadlock", "EAGERSPILL"]#,"1","2"]
legends = ["Production Ray","DFS + [                   ] + [             ]","DFS + Backpressure + [             ]",
        "DFS + [                   ] + BlockSpill","DFS + Backpressure + BlockSpill", "EagerSpill"]#,"Deadlock #1","Deadlock #2"]

working_sets = [1,2,4,8]
working_sets_len = len(working_sets)
num_of_bars = len(files)
X = np.arange(working_sets_len)
# set width of bar
barWidth = 1/(num_of_bars+1)
fig = plt.subplots(figsize =(12, 8))
viridis = cm.get_cmap('viridis',num_of_bars)


data = []
std = []
error = []
num_spilled_objs = []

i = 0
for file in files:
    print(file)
    df = pd.read_csv(path_prefix+file+".csv")
    data.append(df['time'].values.tolist())
    std.append(df['std'].values.tolist())
    num_spilled_objs.append(df['num_spill_objs'].values.tolist())
    d=[] 
    for j in range(working_sets_len):
        val = data[i][j]
        s = std[i][j]/2
        d.append({'min':val-s, 'max':val+s})
    error.append(d)
    i += 1


# Set position of bar on X axis
br = []
br.append(np.arange(working_sets_len))
for i in range(num_of_bars - 1):
    br.append([x + barWidth for x in br[i]])
br1 = np.arange(len(data[0]))

for i in range(num_of_bars):
    plt.bar(br[i], data[i], color = viridis.colors[i], width = barWidth,
            edgecolor ='grey', label = legends[i])

pos = 0
for i in range(working_sets_len):
    for j in range(num_of_bars):
        plt.vlines(pos, error[j][i]['min'], error[j][i]['max'], color='k')
        plt.text(pos, data[j][i], num_spilled_objs[j][i], ha='center', va='bottom')
        pos += barWidth
    pos += barWidth

# Adding Xticks
plt.xlabel('Working Set Ratio', fontweight ='bold', fontsize = 35)
plt.ylabel('Runtime', fontweight ='bold', fontsize = 35)
plt.xticks([r + barWidth for r in range(working_sets_len)], working_sets, fontsize = 20)
plt.yticks(fontsize = 20)
plt.legend(loc='upper left', fontsize=20)
#plt.title("Performance Breakdown with Streaming", fontweight ='bold', fontsize = 18)
plt.savefig(application+".png")
'''
