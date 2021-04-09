

map(ratings, value):
	for line in ratings:
		userID = line.split(',')[0]
		rating = line.split(',')[2]
		emit(userID, (rating, 1))

reduce(userID, list((rating, 1)...)):
	souma = count = 0
	for pair in values:
		souma += pair[0]
		count += pair[1]
	emit(userID, (souma, count))

map(userID, (souma, count)):
	if ((souma/count)>3):
		emit(1, (1, 1))
	else:
		emit(1, (0, 1))

reduce(key, list((kalos, 1)...)):
	souma = count = 0
	for pair in values:
		souma += pair[0]
		count += pair[1]
	emit(key, (souma, count))
#kalos παιρνει τιμες 0 ή 1 αναλογως με το εαν
#εχουμε user με avgRating>3

map(key, (souma, count)):
	emit(1, souma*100/count)