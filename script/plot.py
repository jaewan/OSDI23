#! /usr/bin/env python3

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm

application = "streaming"
if len(sys.argv) > 1:
    application = sys.argv[1]

path_prefix = "../data/" + application + "/"
headers = ["std","var","working_set","object_store_size","object_size","time","num_spill_objs","spilled_size"]
files = ["RAY","DFS","DFS_Backpressure","DFS_BlockSpill","DFS_Backpressure_BlockSpill_Deadlock", "EagerSpill"]#,"1","2"]
legends = ["Production Ray","DFS + [                   ] + [             ]","DFS + SkiRental + [             ]",
        "DFS + [                   ] + SkiRental","DFS + Backpressure + SkiRental", "EagerSpill"]#,"Deadlock #1","Deadlock #2"]

working_sets = [1,2,4,8]
working_sets_len = len(working_sets)
num_of_bars = len(files)
X = np.arange(working_sets_len)
# set width of bar
barWidth = 1/(num_of_bars+1)
fig = plt.subplots(figsize =(12, 8))
viridis = cm.get_cmap('viridis',num_of_bars)

colors = ['#10739E', '#D69B00', '#B38100', '#B38100', '#7D5A00', '#4D3700']

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

plt.grid(zorder=0, axis='y', color='grey')

# Set position of bar on X axis
br = []
plt.grid(zorder=0, axis='y', color='grey')
br.append(np.arange(working_sets_len))
for i in range(num_of_bars - 1):
    br.append([x + barWidth for x in br[i]])
br1 = np.arange(len(data[0]))

for i in range(num_of_bars):
    if i == 2:
        plt.bar(br[i], data[i], color = colors[i], width = barWidth,
                edgecolor ='black', hatch='\\',  label = legends[i])
    elif i == 3:
        plt.bar(br[i], data[i], color = colors[i], width = barWidth,
                edgecolor ='black', hatch='//',  label = legends[i])
    elif i ==4:
        plt.bar(br[i], data[i], color = colors[i], width = barWidth,
                edgecolor ='black', hatch='xx',  label = legends[i])
    else:
        plt.bar(br[i], data[i], color = colors[i], width = barWidth,
                edgecolor ='black', label = legends[i])

pos = 0
for i in range(working_sets_len):
    for j in range(num_of_bars):
        plt.vlines(pos, error[j][i]['min'], error[j][i]['max'], color='k')
        #plt.text(pos, data[j][i], num_spilled_objs[j][i], ha='center', va='bottom')
        pos += barWidth
    pos += barWidth

# Adding Xticks
plt.xlabel('Working Set Ratio', fontweight ='bold', fontsize = 35)
plt.ylabel('Runtime', fontweight ='bold', fontsize = 35)
plt.xticks([r + barWidth for r in range(working_sets_len)], working_sets, fontsize = 20)
plt.yticks(fontsize = 20)
#plt.legend(loc='upper left', fontsize=20)
#plt.title("Performance Breakdown with Streaming", fontweight ='bold', fontsize = 18)
plt.savefig(application+".png")
