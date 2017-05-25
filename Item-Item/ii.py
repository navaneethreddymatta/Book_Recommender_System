import re
import sys
import math
import itertools

from operator import add
from itertools import combinations
from pyspark import SparkConf, SparkContext
from collections import defaultdict

# Get the id of book, user
def getId(x):
	y = re.match(r'^"(.*?)";',x).group(1)
	yArr = y.split(";\"\"")
	y = yArr[0]
	#return y,x
	return y
	
# print any rdd for debugging.
def PrintRdd(rdd):
	for x in rdd.collect():
		print x

# Returns a tuple with userId and another tuple of bookId and its corresponding rating that the user provided
def generateTupleFor_User_BookRating(dataLine):	
	userId = getId(dataLine)
	bookId = re.match(r'.*;"(.*?)";',dataLine).group(1)
	#ratingValue = re.match(r'.*;"(.*?)"$',dataLine)
	ratingValue = dataLine.split("\"\"")
	ratingstr = ratingValue[3].strip()
	try:
		ratingScore = float(ratingstr)
		if(ratingScore == 0.0):
			ratingScore = 1.0
	except ValueError:
		ratingScore = 3.0		
	return userId,(bookId,ratingScore)	

# For a particualr user we get array of tuples with bookId and rating
# Makes combinations of 2 tuples each time and generates pairs as (BookA, BookB) and (RatingOfBookA, RatingOfBookB)
def generateTupleFor_ItemPair_RatingPair(user,bookRating):
	#bookRating = filter(None, bookRating)
	bookRatingCombinations = [list(x) for x in itertools.combinations(bookRating,2)]
	#for a in bookRatingCombinations:
		#print a
	for item1,item2 in bookRatingCombinations:
		return (item1[0],item2[0]),(item1[1],item2[1])
	return

# From all the users we get list of rating pairs for a pair of two books.
# Calculate the cosine similarity between the two books using all these rating pairs
def getCosineSimilarity(book_pair, rating_pair_list):   	
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
  		return book_pair, (cosineSimilarity, numRatingPairs)

# Generate tuple with BookA and another tuple of BookB, its similarity value with A
def generateTupleFor_BookA_BookBCosine(bookPair_CosineSimVal):
	bookPair = bookPair_CosineSimVal[0]
	cosineSimVal_n = bookPair_CosineSimVal[1]
	yield(bookPair[0],(bookPair[1],cosineSimVal_n))
	yield(bookPair[1],(bookPair[0],cosineSimVal_n))

# Generate Book Recommendation scores for each user
def generateBookRecommendations(userId,bookRating_tuple,similarity_dictionary,n):	
	totalSimilarityWithRating = defaultdict(int)
	totalSimilarity = defaultdict(int)
	for (book,rating) in bookRating_tuple:
       		# lookup the nearest neighbors for this book
       		neighbors = similarity_dictionary.get(book,None)
        	if neighbors:
           		for (neighbor,(cosineSim, count)) in neighbors:
                		if neighbor != book:
                    			# update totals and sim_sums with the rating data
                    			totalSimilarityWithRating[neighbor] += float((str(cosineSim)).replace("\"","")) * float((str(rating)).replace("\"",""))
                    			totalSimilarity[neighbor] += float((str(cosineSim)).replace("\"",""))

    # create the normalized list of recommendation scores
	book_RecScores = []
   	for (book,totalScore) in totalSimilarity.items():
		if totalScore == 0.0:
			book_RecScores.append((0, book))
		else:	
			book_RecScores.append((totalSimilarityWithRating[book]/totalScore, book))

    	# sort the book Recommendation Scores in descending order
    	book_RecScores.sort(reverse=True)
    	return userId,book_RecScores[:n]

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
		print >> sys.stderr, "Usage: program_file <Books_file> <Users_file> <User-Rating_file> <output_path>"
		exit(-1)
	# reading input from the path mentioned			
	conf = (SparkConf()
			.setMaster("local")
			.setAppName("item-item-collaboration")
			.set("spark.executor.memory", "6g")	
			.set("spark.driver.memory", "6g"))
	#sc = SparkContext(appName="item-item-collaboration")
	sc = SparkContext(conf = conf)

	# Read the bookRatings csv file and split to two parts
	# One for training and one for testing to evaluate our method
	bookRatings = sc.textFile(sys.argv[1], 1)
	train_DataRDD, test_DataRDD = bookRatings.randomSplit([0.80, 0.20], seed = 11L)
	
	# Group all the Books and Ratings of each user	
	tuple_user_bookRating = train_DataRDD.map(lambda x : generateTupleFor_User_BookRating(x)).filter(lambda p: len(p[1]) > 1).groupByKey().cache()

	# Find all the possible pairs for item-item pair for given user and the ratings of the corresponding books	
	tuple_ItemPair_RatingPair = tuple_user_bookRating.map(lambda x: generateTupleFor_ItemPair_RatingPair(x[0],x[1])).filter(lambda x: x is not None).groupByKey().cache()
	
	# Find the cosine similarity between two books
	bookPair_CosineSimilarity = tuple_ItemPair_RatingPair.map(lambda x: getCosineSimilarity(x[0],x[1])).filter(lambda x: x[1][0] >= 0)
	
	# Generate tuples that contain book Id as key and the value is a list of tuples that has other books id and its cosine similarity with the key book
	tuple_BookA_BookBCosine = bookPair_CosineSimilarity.flatMap(lambda x : generateTupleFor_BookA_BookBCosine(x)).collect()
	
	dict_BookA_BooksCosineList = {}
	for (book, data) in tuple_BookA_BookBCosine:
		if book in dict_BookA_BooksCosineList:
			dict_BookA_BooksCosineList[book].append(data)
		else:
			dict_BookA_BooksCosineList[book] = [data]
	maxNumOfBooksToBeRecommended = 50
	
	# Generate the list of book with their recomendation score for each user
	user_book_recommendations = tuple_user_bookRating.map(lambda x: generateBookRecommendations(x[0], x[1], dict_BookA_BooksCosineList, maxNumOfBooksToBeRecommended))
	outputFilePath = sys.argv[2]+"/recomendations"
	user_book_recommendations.saveAsTextFile(outputFilePath)	
	
	# Using the above we have generated some recommendation scores, compare them with the data from the test_RDD
	test_DataRDD_User_BookRating = test_DataRDD.map(lambda x : generateTupleFor_User_BookRating(x)).filter(lambda p: len(p[1]) > 1).groupByKey().cache()
	rec_mae_data = user_book_recommendations.join(test_DataRDD_User_BookRating).flatMap( lambda x : generate_mae_data(x[1][0],list(x[1][1])))
	tupleList_recScore_dataScore = []
	for (recScore,testDataScore) in rec_mae_data.collect():
		tupleList_recScore_dataScore.append((recScore,testDataScore))
	error = calculate_mae(tupleList_recScore_dataScore)
	error = sc.parallelize([error])
	error.saveAsTextFile(sys.argv[2]+"/error")

	sc.stop()

