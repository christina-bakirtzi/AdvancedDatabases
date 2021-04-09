

map(movies, value):
	for line in movies:
		data = line.split(',')
		if(2000<=(data[3].split('-')[0])<=2019):
			movieID = data[0]
			year = data[3].split('-')[0]
			summary = data[2]
			for word in summary.split(' '):
				emit((movieID, year), 1)

reduce((movieID, year), list(1)):
	for i in values:
		count += i
	emit(movieID, (year, count))
#(movieID, (year, #ofWords))

map(movie_genres, value):
	for line in movie_genres:
		if (line.split(',')[1] == 'Drama'):
			movieID = line.split(',')[0]
			genre = line.split(',')[1]
			emit(movieID, genre)

Join(movies, movie_genres)

map(movieID, ((year, numofWords), genre)):
	if (2000<=values[0][0]<=2004):
		period = "2000-2004"
	elif (2005<=values[0][0]<=2009):
		period = "2005-2009"
	elif (2010<=values[0][0]<=2014):
		period = "2010-2014"
	elif (2015<=values[0][0]<=2019):
		period = "2015-2019"
	numofWords = values[0][1]
	emit(period, (numofWords, 1))

reduce(period, list((numofWords, 1))):
	souma = count = 0
	for pair in values:
		souma += values[0]
		count += values[1]
	avgRatingLength = souma/count
	emit(period, avgRatingLength)