

map(movie_genres, values):
	for distinct(line) in movie_genres:
		movieID = line.split(',')[0]
		genre = line.split(',')[1]
		emit(movieID, genre)

map(movies, values):
	for line in movies:
		movieID = data[0]
		name = data[3].split('-')[0]
		popularity = data[7]
		emit(movieID, (name, popularity))

movie_info = Join(movie_genres, movies)

map(movieID, (genre, (name, popularity))):
	emit(movieID, (genre, name, popularity))

map(ratings, values):
	for line in movie_genres:
		movieID = line.split(',')[1]
		userID = line.split(',')[0]
		rating = line.split(',')[2]
		emit(movieID, (userID, rating))

Join(movie_info, ratings)

map(movieID, ((genre, name, popularity), (userID, rating))):
	MaxName=MinName=name
	MaxRating=MinRating=rating
	MaxPopularity=MinPopularity=popularity
	emit((genre, userID), (1, MaxName, MaxRating, MaxPopularity, MinName, MinRating, MinPopularity))

reduce(key, list(values)):
	count = 0
	MaxRating=MaxPopularity=0
	MinRating=MinPopularity=inf
	for item in values:
		count += item[0]
		if ((item[2]>MaxRating) or ((item[2]==MaxRating) and (item[3]>MaxPopularity))):
			MaxName=item[1]
			MaxRating=item[2]
			MaxPopularity=item[3]
		if ((item[5]<MinRating) or ((item[5]==MinRating) and (item[6]>MinPopularity))):
			MinName=item[4]
			MinRating=item[5]
			MinPopularity=item[6]
	emit((genre, userID), (count, MaxName, MaxRating, MaxPopularity, MinName, MinRating, MinPopularity))

map((genre, userID), (count, MaxName, MaxRating, MaxPopularity, MinName, MinRating, MinPopularity)):
	emit(genre, (userID, count, MaxName, MaxRating, MinName, MinRating))

reduce(key, list(values)):
	MaxCount=0
	for item in values:
		if (item[1]>MaxCount):
			MaxCount = item[1]
			userID = item[0]
			MaxName = item[2]
			MaxRating = item[3]
			MinName = item[4]
			MinRating = item[5]
	emit(genre, (userID, MaxCount, MaxName, MaxRating, MinName, MinRating))