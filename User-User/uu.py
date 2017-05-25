import re
import sys
import math
import itertools
import os

from operator import add
from itertools import combinations
from pyspark import SparkConf, SparkContext
from collections import defaultdict


# Get the id of book, user
def getId(x):
	y = re.match(r'^"(.*?)";',x).group(1)
	yArr = y.split(";\"\"")
	y = yArr[0]
	return y
	
# Returns a tuple with BookID and another tuple of UserId and its corresponding rating that the user provided
def generateTupleFor_Book_UserRating(dataLine):	
	userId = getId(dataLine)
	bookId = re.match(r'.*;"(.*?)";',dataLine).group(1)
	ratingValue = dataLine.split("\"\"")
	ratingstr = ratingValue[3].strip()
	try:
		ratingScore = float(ratingstr)
		if(ratingScore == 0.0):
			ratingScore = 1.0
	except ValueError:
		ratingScore = 3.0		
	return bookId,(userId,ratingScore)

# For a particualr Book we get array of tuples with UserId and rating
# Makes combinations of 2 tuples each time and generates pairs as (UserA, UserB) and (RatingOfBookA, RatingOfBookB)
def generateTupleFor_UserPair_RatingPair(book,userRating):
	userRatingCombinations = [list(x) for x in itertools.combinations(userRating,2)]
	for user1,user2 in userRatingCombinations:
		return (user1[0],user2[0]),(user1[1],user2[1])
	return

# From all the books we get list of rating pairs for a pair of two users.
# Calculate the cosine similarity between the two users using all these rating pairs
def getCosineSimilarity(user_pair, rating_pair_list):   	
	totalScoreOfA, totalRateProductOfAB, totalScoreOfB, numRatingPairs = (0.0, 0.0, 0.0, 0)
	for rating_pair in rating_pair_list:
		a = float(rating_pair[0])
		b = float(rating_pair[1])
		totalScoreOfA += a * a 
		totalScoreOfB += b * b
		totalRateProductOfAB += a * b
		numRatingPairs += 1
	denominator = (math.sqrt(totalScoreOfA) * math.sqrt(totalScoreOfB))
	if denominator == 0.0: 
		return book_pair, (0.0,numRatingPairs)
	else:
		cosineSimilarity = totalRateProductOfAB / denominator
  		return user_pair, (cosineSimilarity, numRatingPairs)
	
# Generate tuple with UserA and another tuple of UserB, its similarity value with A
def generateTupleFor_UserA_UserBCosine(userPair_CosineSimVal):
	userPair = userPair_CosineSimVal[0]
	cosineSimVal_n = userPair_CosineSimVal[1]
	yield(userPair[0],(userPair[1],cosineSimVal_n))
	yield(userPair[1],(userPair[0],cosineSimVal_n))
	
# Generate User Recommendation scores for each book
def generateUserRecommendations(bookId,userRating_tuple,similarity_dictionary,n):	
	totalSimilarityWithRating = defaultdict(int)
	totalSimilarity = defaultdict(int)
	for (user,rating) in userRating_tuple:
		# lookup the nearest neighbors for this user
		neighbors = similarity_dictionary.get(user,None)
		if neighbors is not None:
			for (neighbor,(cosineSim, count)) in neighbors:
				if neighbor != user:
					# update totals and sim_sums with the rating data
					totalSimilarityWithRating[neighbor] += float((str(cosineSim)).replace("\"","")) * float((str(rating)).replace("\"",""))
					totalSimilarity[neighbor] += float((str(cosineSim)).replace("\"",""))
	# create the normalized list of recommendation scores
	user_RecScores = []
   	for x in totalSimilarity.items():		
		if (x is not None) and (x[0] is not None) and (x[1] is not None):	
			user = str(x[0])
			totalScore = float(x[1])
			if totalScore == 0.0:
				user_RecScores.append((0.0, user))
			else:	
				user_RecScores.append((totalSimilarityWithRating[user]/totalScore, user))
			# sort the book Recommendation Scores in descending order
			user_RecScores.sort(reverse=True)
	return bookId,user_RecScores[:n]		
	
# Generate Book Recommendation scores for each user
def generateBookRecommendations	(bookId_userRecScores):
	bookId = bookId_userRecScores[0]
	userRecScores = bookId_userRecScores[1]
	user_BookRecScores = []
	if (userRecScores is not None) and (len(userRecScores) > 0):
		for userRecScore in userRecScores:
			try:
				userId = userRecScore[1]
				rating = userRecScore[0]
				user_BookRecScores.append((userId, (rating, bookId)))
			except ValueError:
				a = 1
	return user_BookRecScores		

def generateUserBookRecommendationLists(book_user_recommendation):
	user_BookRecScores = []
	bookId = book_user_recommendation[0]
	userRecScores = book_user_recommendation[1]
	
	if (userRecScores is not None) and (len(userRecScores) > 0):
		for userRecScore in userRecScores:
			if (userRecScore is not None) and (len(userRecScore) > 0):
				for userRecScoreTuple in userRecScore:
					try:
						userId = userRecScoreTuple[1]
						rating = userRecScoreTuple[0]
						user_BookRecScores.append((userId, (rating, bookId)))
					except ValueError:
						a = 1
	return(1,user_BookRecScores)
	
def generateUserBookRecommendationDicts(user_book_recommendations_ListOfLists):
	user_book_recommendations_MainList = []
	dict_User_BookRecommendation = {}
	tupleList_User_BookRecommendation = []
	
	for user_book_recommendations_Lists in user_book_recommendations_ListOfLists[1]:
		user_book_recommendations_MainList = user_book_recommendations_MainList + user_book_recommendations_Lists
	
	for (user, bookRating) in user_book_recommendations_MainList:
		if user in dict_User_BookRecommendation:
			dict_User_BookRecommendation[user].append(bookRating)
		else:
			dict_User_BookRecommendation[user] = [bookRating]
	
	for k,v in dict_User_BookRecommendation.items():
	    tupleList_User_BookRecommendation.append((k,v))
		
	return tupleList_User_BookRecommendation	
		
def generate_mae_data(bookRecommendations, testData):
	MaeData = []
	for (recommendationScore, book) in bookRecommendations:
		for (testData_book, testData_rating) in testData:
			if str(testData_book.encode('ascii', 'ignore')) == str(book.encode('ascii', 'ignore')):
				MaeData.append((float(recommendationScore),float(testData_rating)))
	return MaeData			

def calculate_mae(MaeData):
	n = float(len(MaeData))
	total = 0.0
	for (x,y) in MaeData:
		total += abs(x-y)
	if n == 0.0:
		return 0.0
	return total/n

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print >> sys.stderr, "Usage: program_file <Book-Rating_file> <output_path>"
		exit(-1)
	# reading input from the path mentioned			
	conf = (SparkConf()
			.setMaster("local")
			.setAppName("user-user-collaboration")
			.set("spark.executor.memory", "16g")	
			.set("spark.driver.memory", "16g")
			.set('spark.kryoserializer.buffer.max', '1g'))
	sc = SparkContext(conf = conf)	

	# Read the bookRatings csv file and split to two parts
	# One for training and one for testing to evaluate our method
	bookRatings = sc.textFile(sys.argv[1], 1)
	train_DataRDD, test_DataRDD = bookRatings.randomSplit([0.80, 0.20], seed = 11L)
	
	# Group all the Books and Ratings of each user	
	tuple_Book_UserRating = train_DataRDD.map(lambda x : generateTupleFor_Book_UserRating(x)).filter(lambda p: len(p[1]) > 1).groupByKey().cache()

	# Find all the possible pairs for user-user pair for given book and the ratings of the corresponding books	
	tuple_UserPair_RatingPair = tuple_Book_UserRating.map(lambda x: generateTupleFor_UserPair_RatingPair(x[0],x[1])).filter(lambda x: x is not None).groupByKey().cache()
	
	# Find the cosine similarity between two users
	userPair_CosineSimilarity = tuple_UserPair_RatingPair.map(lambda x: getCosineSimilarity(x[0],x[1])).filter(lambda x: x[1][0] >= 0)
	
	# Generate tuples that contain user Id as key and the value is a list of tuples that has other user ids and their cosine similarity with the key user
	tuple_UserA_UserBCosine = userPair_CosineSimilarity.flatMap(lambda x : generateTupleFor_UserA_UserBCosine(x)).collect()
	
	dict_UserA_UsersCosineList = {}
	for (user, data) in tuple_UserA_UserBCosine:
		if user in dict_UserA_UsersCosineList:
			dict_UserA_UsersCosineList[user].append(data)
		else:
			dict_UserA_UsersCosineList[user] = [data]
	maxNumOfBooksToBeRecommended = 50
	
	# Generate the list of users with their recomendation score for each book
	book_user_recommendations = tuple_Book_UserRating.map(lambda x: generateUserRecommendations(x[0], x[1], dict_UserA_UsersCosineList, maxNumOfBooksToBeRecommended)).filter(lambda x: x is not None).groupByKey().cache()
	
	user_book_recommendations_Lists = book_user_recommendations.map(lambda x: generateUserBookRecommendationLists(x)).groupByKey()
	
	user_bookRecomendations_TuplesList = user_book_recommendations_Lists.map(lambda x: generateUserBookRecommendationDicts(x)).filter(lambda x: x is not None)
	
	outputFilePath = sys.argv[2]+"/recomendations"
	user_bookRecomendations_TuplesList.saveAsTextFile(outputFilePath)	

	sc.stop()

