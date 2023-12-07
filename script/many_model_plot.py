import numpy as np
import matplotlib.pyplot as plt

# Data on X-axis

colors = ['#10739E','#4D3700']
boa_data = (21.152, 28.827,44.108,52.501,63.56,85.77)
ray_data = (21.176, 31.808,47.757,121.2,147.4,169.75)

boa_restored = [0,0,0.5,1.8]
ray_restored = [0,0,20,76]

# Position of bars on x-axis
ind = np.arange(len(boa_data))

# Figure size
plt.figure(figsize=(12,8))
plt.grid(zorder=0, axis='y', color='grey')

# Width of a bar 
width = 0.3       

# Plotting
ray_bar = plt.bar(ind, ray_data , width, label='Production Ray', color=colors[0], zorder=3)
boa_bar = plt.bar(ind + width, boa_data, width, label='Boa', color=colors[1], zorder=3)

'''
i = 0
for bar in ray_bar:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2.0, height, f'{ray_data[i]:.0f}', ha='center', va='bottom')
    i = i + 1

i = 0
for bar in boa_bar:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2.0, height, f'{boa_data[i]:.0f}', ha='center', va='bottom')
    i = i + 1
'''


plt.xlabel('Number of NYC Taxi Files per Node', fontsize = 35)
plt.ylabel('Runtime (sec)', fontsize = 35)
#plt.title('title')

# xticks()
# First argument - A list of positions at which ticks should be placed
# Second argument -  A list of labels to place at the given locations
plt.xticks(ind + width / 2, ('6', '9', '12', '15', '18','21'), fontsize = 20)
plt.yticks(fontsize = 20)

# Finding the best position for legends and putting it
plt.legend(loc='best', fontsize=35)
plt.savefig("many_model.png")
