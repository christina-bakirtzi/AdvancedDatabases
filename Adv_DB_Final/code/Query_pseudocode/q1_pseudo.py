

map(movies, value):
	for line in movies:
		data = line.split(',')
		if((data[3].split('-')[0]>=2000) 
		and data[5]!=0 and data[6]!=0):
			name = data[1]
			year = data[3].split('-')[0]
			profit = ((data[6]-data[5])/data[5])*100
			emit(year, (name, profit))

reduce(year, list((name, profit)...)):
	maxname = '0'	
	maxprofit = 0
	for pair in values:
		if (pair[1] > maximum)
			maxname = pair[0]
			maxprofit = pair[1]
	emit(year, (maxname, maxprofit))