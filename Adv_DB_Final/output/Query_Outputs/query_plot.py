import numpy as np
import matplotlib.pyplot as plt

groups = 5
qall = []
q = []

for second in 'rdd', 'sql', 'sql_b':
	for first in 'q1', 'q2', 'q3', 'q4', 'q5':
		file = open(first + '_' + second + '.txt', 'r', encoding='utf8')
		for line in file.readlines():
			if ('Completed in' in line):
				time = float(line.split(' ')[2])
		q.append(time)
		file.close()
	qall.append(q)
	q = []

fig, ax = plt.subplots()
index = np.arange(groups)
bar_width = 0.2
opacity = 0.6

rects1 = plt.bar(index, qall[0], bar_width, alpha=opacity, color='navy', label='RDD')
rects2 = plt.bar(index + bar_width, qall[1], bar_width, alpha=opacity, color='cyan', label='SQL(csv input)')
rects3 = plt.bar(index + 2*bar_width, qall[2], bar_width, alpha=opacity, color='darkviolet', label='SQL(parquet input)')

plt.xlabel('Query')
plt.ylabel('Time(s)')
plt.title('Time per Query')
plt.xticks(index + bar_width, ('q1', 'q2', 'q3', 'q4', 'q5'))
plt.legend()

plt.tight_layout()
plt.show()