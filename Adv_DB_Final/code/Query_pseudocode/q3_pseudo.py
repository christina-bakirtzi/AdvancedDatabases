

map(ratings, value):
	for line in ratings:
		movieID = line.split(',')[1]
		rating = line.split(',')[2]
		emit(movieID, (rating, 1))

reduce(movieID, list((rating, 1)...)):
	souma = count = 0
	for pair in values:
		souma += pair[0]
		count += pair[1]
	emit(movieID, (souma/count, count))
#souma/count = AvgMovieRating

map(movie_genres, value):
	for distinct(line) in movie_genres:
		movieID = line.split(',')[0]
		genre = line.split(',')[1]
		emit(movieID, genre)

Join(ratings, movie_genres)

map(movieID, ((AvgMovieRating, count), genre)):
	genre = values[1]
	AvgMovieRating = values[0][0]
	emit(genre, (AvgMovieRating, 1))

reduce(genre, list((AvgMovieRating, 1))):
	for pair in values:
		souma += pair[0]
		count += pair[1]
	emit(genre, (souma/count, count))