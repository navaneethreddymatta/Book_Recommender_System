import re
import sys

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation\
    import ALS,MatrixFactorizationModel, Rating
		
import hashlib
from math import sqrt

# Get the id of book
def getId(x):
	y = re.match(r'^"(.*?)";',x).group(1)
	yArr = y.split(";\"\"")
	y = yArr[0]
	#return y,x
	return y

# Returns a tuple with userId and another tuple of bookId and its corresponding rating that the user provided
def Generate_User_Book_Rating(dataLine):	
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
	return userId,bookId,ratingScore	
	
if __name__ == "__main__":
	if len(sys.argv) != 4:
		print >> sys.stderr, "Usage: program_file <Books_file> <User-Rating_file> <output_path> "
		exit(-1)
	conf = (SparkConf()
			.setMaster("local")
			.setAppName("als")
			.set("spark.executor.memory", "32g")
			.set("spark.driver.memory","32g"))
	sc = SparkContext(conf = conf)

	# Read the books csv file and get those ids
	booksRDD = sc.textFile(sys.argv[1], 1)
	booksRDD = booksRDD.map(lambda x : getId(x))
	#print booksRDD.collect()

	# Read the bookRatings csv file and get UserId, Bookid, Rating values
	userBookRatings = sc.textFile(sys.argv[2], 1)
	userBookRatings = userBookRatings.map(lambda x: x.encode('utf-8'))
	User_Book_Rating = userBookRatings.map(lambda x : Generate_User_Book_Rating(x))

		
	#print "User_Book_Rating:        #"
	#print User_Book_Rating.first()
	#creating a ratings object
	
	ratingsobj = User_Book_Rating.map(lambda x: Rating(int(hashlib.sha1(x[0]).hexdigest(), 16)%(10 ** 6),int(hashlib.sha1(x[1]).hexdigest(), 16)%(10 ** 8), float(x[2])))
	
	#print "Ratings value: ##########################"
	#print ratings.first()
	#Generate a training and test set
	
	train_data, test_data = ratingsobj.randomSplit([0.8,0.2],None)
	
	#cache the data to speed up process
	
	train_data.cache()
	test_data.cache()

	#Setting up the parameters for ALS
	
	alsrank = 5 # Latent Factors to be made
	alsnumIterations = 10 # Times to repeat process
	
	#Build model on the training data
	
	model = ALS.train(train_data, alsrank, alsnumIterations)
	
	# Reformat the train data
	
	prediction_input = train_data.map(lambda x:(x[0],x[1]))   

	#Returns Ratings(user, item, prediction)
	
	prediction = model.predictAll(prediction_input)
	
	# Reformat the test data
	test_input = test_data.map(lambda x:(x[0],x[1])) 
	pred_test = model.predictAll(test_input)
	
	
	#Performance Evaluation
	
	#Organize the data to make (user, product) the key) for train data
	
	trained_values = train_data.map(lambda x:((x[0],x[1]), x[2]))
	predicted_values = prediction.map(lambda x:((x[0],x[1]), x[2]))

	#Join the trained_values and predicted_values
	
	train_output_values = trained_values.join(predicted_values)

	#Calculate Mean-Squared Error

	train_MSE = train_output_values.map(lambda r: (r[1][0] - r[1][1])**2).mean()
	print "Error for Training Set:"
	print train_MSE
	#Calculate Root-Mean-Squared Error
	train_RMSE = sqrt(train_MSE)
	print train_RMSE

	#Organize the data to make (user, product) the key) for train data
	
	test_values = test_data.map(lambda x:((x[0],x[1]), x[2]))
	test_predicted_values = pred_test.map(lambda x:((x[0],x[1]), x[2]))
	
	#Join the test_values and test_predicted_values
	test_output_values = test_values.join(test_predicted_values)
	
	#Calculate Mean-Squared Error
	test_MSE = test_output_values.map(lambda x: (x[1][0] - x[1][1])**2).mean()
	#Calculate Root-Mean-Squared Error
	test_RMSE = sqrt(test_MSE)
	print "Error for Training Set:"
	print test_MSE
	print test_RMSE
	
	#Save the Error value to a text file
	error = sc.parallelize([("Train MSE:",train_MSE),("Train RMSE:",train_RMSE),("Test MSE:",test_MSE),("Test RMSE:",test_RMSE)])
	error.saveAsTextFile(sys.argv[3]+"/error")
	
	#Generate the recommendations for each user
	
	result = model.recommendProductsForUsers(10).collect()
	
	#result = model.recommendUsersForProducts(10)
	#result = model.recommendProducts(4361,20)
	
	#Save the result to a text file
	resultRDD = sc.parallelize(result)
	resultRDD.saveAsTextFile(sys.argv[3]+"/result")
	

	#Convert Book Ids and User IDs to Integer to use
	
	user_ids = User_Book_Rating.map(lambda k:(k[0],int(hashlib.sha1(k[0]).hexdigest(), 16)%(10 ** 6)))
	user_ids_list = user_ids.collect()
	
	# Save User ids to a file
	user_ids_RDD = sc.parallelize(user_ids_list)
	user_ids_RDD.saveAsTextFile(sys.argv[3]+"/users")
	
	book_ids = User_Book_Rating.map(lambda k:(k[1],int(hashlib.sha1(k[1]).hexdigest(), 16)%(10 ** 8)))
	book_ids_list = book_ids.collect()
	
	# Save Book ids to a file
	book_ids_RDD = sc.parallelize(book_ids_list)
	book_ids_RDD.saveAsTextFile(sys.argv[3]+"/books")
	
	sc.stop()

