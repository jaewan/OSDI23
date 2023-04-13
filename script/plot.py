#! /usr/bin/env python3

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math

application = "pipeline"
MULTINODE=True

headers = ["time,num_spill_objs,spilled_size,migration_count,working_set,object_store_size,object_size,std,var"]
#files = ["RAY","DFS","DFS_Backpressure","DFS_BlockSpill","DFS_Backpressure_BlockSpill_Deadlock", "EagerSpill", "Boa"]#,"1","2"]
files = ["RAY","DFS","DFS_Backpressure","DFS_BlockSpill","DFS_Backpressure_BlockSpill_Deadlock", "Boa"]
#files = ["DFS","DFS_Backpressure","DFS_BlockSpill", "EagerSpill"]
working_sets = [1,2,4,8,16,32]#[1,2,4,8]
working_sets_len = len(working_sets)

if len(sys.argv) > 1:
    application = sys.argv[1]

def parse_data():
    path_prefix = "../data/"

    data = []
    std = []
    migration_count = []
    error = []
    num_spilled_objs = []

    if MULTINODE:
        path_prefix += application + "/"
        OBJECT_SIZE=100000000
        OBJECT_STORE_SIZE=16000000000

        for file in files:
            df = pd.read_csv(path_prefix+file+".csv")
            target_df = df[(df['object_store_size'] == OBJECT_STORE_SIZE) & (df['object_size'] == OBJECT_SIZE)]
            averages = []
            nso = []
            mc = []
            d = []
            for ws in working_sets:
                ws_df = target_df.loc[target_df['working_set'] == ws, 'time']
                averages.append(ws_df.mean())
                d.append({'min':ws_df.min(), 'max':ws_df.max()})
                nso.append(target_df.loc[target_df['working_set'] == ws, 'num_spill_objs'].mean())
                mc.append(target_df.loc[target_df['working_set'] == ws, 'migration_count'].mean())
            data.append(averages)
            error.append(d)
            num_spilled_objs.append(nso)
            migration_count.append(mc)

    else:
        path_prefix += "/single_node/" + application + "/"
        i = 0
        for file in files:
            print(file)
            df = pd.read_csv(path_prefix+file+".csv")
            data.append(df['time'].values.tolist())
            std.append(df['std'].values.tolist())
            num_spilled_objs.append(df['num_spill_objs'].values.tolist())
            migration_count.append(df['migration_count'].values.tolist())
            d=[] 
            for j in range(working_sets_len):
                val = data[i][j]
                s = std[i][j]/2
                d.append({'min':val-s, 'max':val+s})
            error.append(d)
            i += 1
    return data, error, num_spilled_objs, migration_count

if __name__ == '__main__':
    data, error, num_spilled_objs, migration_count = parse_data()
    for i in range(len(files)):
        print('working set ratio ' + str(working_sets[i]), end ="\t")
        for j in range(working_sets_len):
            if math.isnan(data[i][j]):
                data[i][j] = 0
            if math.isnan(error[i][j]['min']):
                error[i][j]['min'] = 0
            if math.isnan(error[i][j]['max']):
                error[i][j]['max'] = 0
            if math.isnan(num_spilled_objs[i][j]):
                num_spilled_objs[i][j] = 0
            if math.isnan(migration_count[i][j]):
                migration_count[i][j] = 0
            print(migration_count[j][i], end ="\t")
        print("")

    legends = ["Production Ray","DFS + [                   ] + [             ]","DFS + Backpressure + [             ]",
            "DFS + [                   ] + BlockSpill","DFS + Backpressure + BlockSpill", "Boa", "Eagerspill", "Offline"]#,"Deadlock #1","Deadlock #2"]
    num_of_bars = len(files)
    X = np.arange(working_sets_len)
    # set width of bar
    barWidth = 1/(num_of_bars+1)
    fig = plt.subplots(figsize =(12, 8))
    colors = ['#10739E', '#D69B00', '#B38100', '#B38100', '#7D5A00', '#4D3700', 'brown', 'blue']
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
            plt.text(pos, data[j][i], round(migration_count[j][i]), ha='center', va='bottom')
            pos += barWidth
        pos += barWidth

    # Adding Xticks
    plt.xlabel('Working Set Ratio', fontweight ='bold', fontsize = 35)
    plt.ylabel('Runtime', fontweight ='bold', fontsize = 35)
    plt.xticks([r + barWidth for r in range(working_sets_len)], working_sets, fontsize = 20)
    plt.yticks(fontsize = 20)
    plt.legend(loc='best', fontsize=20)
    plt.title("Performance Breakdown with " + application, fontweight ='bold', fontsize = 18)
    plt.savefig(application+".png")
