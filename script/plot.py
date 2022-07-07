import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm

path_prefix = "../data/deadlock_Performance_pipeline_"
headers = ["std","var","working_set","object_store_size","object_size","time"]
files = ["RAY","DFS","Constant_Wait","1","2"]
legends = ["Production Ray","DFS","Constant_Wait","Deadlock #1","Deadlock #2"]

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


i = 0
for file in files:
    df = pd.read_csv(path_prefix+file+".csv")
    data.append(df['time'].values.tolist())
    std.append(df['std'].values.tolist())
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
        pos += barWidth
    pos += barWidth

# Adding Xticks
plt.xlabel('Working Set Ratio', fontweight ='bold', fontsize = 15)
plt.ylabel('Runtime', fontweight ='bold', fontsize = 15)
plt.xticks([r + barWidth for r in range(working_sets_len)], working_sets)

plt.legend()
plt.savefig("pipeline.png")
