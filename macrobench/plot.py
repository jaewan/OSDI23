#! /usr/bin/env python3
#dict_keys(['Blues', 'BrBG', 'BuGn', 'BuPu', 'CMRmap', 'GnBu', 'Greens', 'Greys', 'OrRd', 'Oranges', 'PRGn', 'PiYG', 'PuBu', 'PuBuGn', 'PuOr', 'PuRd', 'Purples', 'RdBu', 'RdGy', 'RdPu', 'RdYlBu', 'RdYlGn', 'Reds', 'Spectral', 'Wistia', 'YlGn', 'YlGnBu', 'YlOrBr', 'YlOrRd', 'afmhot', 'autumn', 'binary', 'bone', 'brg', 'bwr', 'cool', 'coolwarm', 'copper', 'cubehelix', 'flag', 'gist_earth', 'gist_gray', 'gist_heat', 'gist_ncar', 'gist_rainbow', 'gist_stern', 'gist_yarg', 'gnuplot', 'gnuplot2', 'gray', 'hot', 'hsv', 'jet', 'nipy_spectral', 'ocean', 'pink', 'prism', 'rainbow', 'seismic', 'spring', 'summer', 'terrain', 'winter', 'Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2', 'Set1', 'Set2', 'Set3', 'tab10', 'tab20', 'tab20b', 'tab20c', 'Blues_r', 'BrBG_r', 'BuGn_r', 'BuPu_r', 'CMRmap_r', 'GnBu_r', 'Greens_r', 'Greys_r', 'OrRd_r', 'Oranges_r', 'PRGn_r', 'PiYG_r', 'PuBu_r', 'PuBuGn_r', 'PuOr_r', 'PuRd_r', 'Purples_r', 'RdBu_r', 'RdGy_r', 'RdPu_r', 'RdYlBu_r', 'RdYlGn_r', 'Reds_r', 'Spectral_r', 'Wistia_r', 'YlGn_r', 'YlGnBu_r', 'YlOrBr_r', 'YlOrRd_r', 'afmhot_r', 'autumn_r', 'binary_r', 'bone_r', 'brg_r', 'bwr_r', 'cool_r', 'coolwarm_r', 'copper_r', 'cubehelix_r', 'flag_r', 'gist_earth_r', 'gist_gray_r', 'gist_heat_r', 'gist_ncar_r', 'gist_rainbow_r', 'gist_stern_r', 'gist_yarg_r', 'gnuplot_r', 'gnuplot2_r', 'gray_r', 'hot_r', 'hsv_r', 'jet_r', 'nipy_spectral_r', 'ocean_r', 'pink_r', 'prism_r', 'rainbow_r', 'seismic_r', 'spring_r', 'summer_r', 'terrain_r', 'winter_r', 'Accent_r', 'Dark2_r', 'Paired_r', 'Pastel1_r', 'Pastel2_r', 'Set1_r', 'Set2_r', 'Set3_r', 'tab10_r', 'tab20_r', 'tab20b_r', 'tab20c_r', 'magma', 'magma_r', 'inferno', 'inferno_r', 'plasma', 'plasma_r', 'viridis', 'viridis_r', 'cividis', 'cividis_r'])

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm

application = "Push based shuffle"

path_prefix = "../data/push_based_shuffle_large/"
headers = ["runtime" ,"spilled_amount", "spilled_objects", "write_throughput", "restored_amount", "restored_objects", "read_throughput"]

files = ["RAY1", "DFS1", "RAY2", "DFS_Backpressure2", "RAY0", "DFS_Backpressure_EagerSpill1"]

legends = ["Ray Batch","DFS + [                   ] + [             ]", "Ray No Backpressure", 
           "DFS + Backpressure + [             ]", "Ray App Scheduling", "DFS + Backpressure + EagerSpill"]

data = []
for file in files:
    df = pd.read_csv(path_prefix+file+".csv")
    #print(file)
    #print(df[headers[0]].mean())
    #print(df[headers[0]].std())
    #print(df.describe().iloc[1,:])
    data.append(df[headers[0]].mean())

num_of_bars = len(files)
# set width of bar
barWidth = 1/(num_of_bars+2)
fig = plt.subplots(figsize =(16, 8))
boa_colors = cm.get_cmap('Oranges_r',num_of_bars)

pos = 0
i = 0
bars = []
plt.grid(zorder=0, axis='y', color='grey')
while i < num_of_bars:
    bars.append(plt.bar(pos, data[i], color = 'steelblue', width = barWidth,
            edgecolor ='grey', label = "Ray", zorder=3))
    pos += barWidth
    i += 1
    bars.append(plt.bar(pos, data[i], color = boa_colors(i//2), width = barWidth,
            edgecolor ='grey', label = legends[i], zorder=3))
    pos += barWidth
    pos += barWidth
    i += 1

# Adding Xticks
plt.xlabel('Schemes Applied', fontsize = 35)
plt.ylabel('Runtime',  fontsize = 35)
#xlabel = ['','','']
plt.xticks([barWidth/2, barWidth*3 + barWidth/2, barWidth*5 + barWidth/2,barWidth*7 + barWidth/2], ["No Execution Order", "No Backpressure", "App Scheduled",
                                                             "No Execution Order"], fontsize = 20)
plt.yticks(fontsize = 20)
#plt.legend(bbox_to_anchor=(0.75, 0.9),
plt.legend((bars[0], bars[1], bars[3], bars[5]),("Ray", "DFS + [                   ] + [             ]",
           "DFS + Backpressure + [             ]", "DFS + Backpressure + EagerSpill"), loc='best',
          fancybox=True, framealpha=0.5, shadow=False, fontsize=15)
plt.title("Push Based Shuffle with each schemes", fontsize = 35)
plt.savefig("push_based_shuffle.png")
