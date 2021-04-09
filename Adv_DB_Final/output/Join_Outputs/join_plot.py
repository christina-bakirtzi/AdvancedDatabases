import numpy as np
import matplotlib.pyplot as plt

groups = 1

br = []
ny = []

for name in 'broadcast', 'repartition':
	file = open(name + '_join.txt', 'r', encoding='utf8')
	for line in file.readlines():
		if ('Total time:' in line):
			time = float(line.split(' ')[2])
			br.append(time)
	file.close()

for name in 'N', 'Y':
	file = open('sql_join_' + name + '.txt', 'r', encoding='utf8')
	for line in file.readlines():
		if ('Time with' in line):
			time = float(line.split(' ')[7])
			ny.append(time)
	file.close()

fig, ax = plt.subplots()
index = np.arange(groups)
bar_width = 0.2
opacity = 0.6

rects1 = plt.bar(index + 2*bar_width, br[0], bar_width, alpha=opacity, color='navy', label='Broadcast')
rects2 = plt.bar(index + 3*bar_width, br[1], bar_width, alpha=opacity, color='cyan', label='Repartition')
rects4 = plt.bar(index, 0, bar_width)
rects4 = plt.bar(index + bar_width, 0, bar_width)
rects4 = plt.bar(index + 4*bar_width, 0, bar_width)
rects4 = plt.bar(index + 5*bar_width, 0, bar_width)

plt.xlabel('Join Type')
plt.ylabel('Time(s)')
plt.title('Time per Join')
plt.xticks(index + 2.5*bar_width, (''))
plt.legend()

plt.tight_layout()
plt.show()

##################################################################################################################

fig, ax = plt.subplots()
index = np.arange(groups)
bar_width = 0.2
opacity = 0.6

rects1 = plt.bar(index + 2*bar_width, ny[0], bar_width, alpha=opacity, color='navy', label='SQL(Enabled)')
rects2 = plt.bar(index + 3*bar_width, ny[1], bar_width, alpha=opacity, color='cyan', label='SQL(Disabled)')
rects4 = plt.bar(index, 0, bar_width)
rects4 = plt.bar(index + bar_width, 0, bar_width)
rects4 = plt.bar(index + 4*bar_width, 0, bar_width)
rects4 = plt.bar(index + 5*bar_width, 0, bar_width)

plt.xlabel('Join Type')
plt.ylabel('Time(s)')
plt.title('Time per Join')
plt.xticks(index + 2.5*bar_width, (''))
plt.legend()

plt.tight_layout()
plt.show()