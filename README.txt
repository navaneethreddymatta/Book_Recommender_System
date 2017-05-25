The following are the steps used to excute various parts of the assignment.

Report URL : http://webpages.uncc.edu/nmatta1/cloudproject/report.html

############# PART 1: ITEM-ITEM #############
We executed this part in DSBA cluster.

	STEP 1:
	We copied the iput files into the dsba cluster.
		$ hadoop fs -put /dsbalocationfolder/ /home/cloudera/ratings.csv

	STEP 2:
		$ spark-submit ii.py /dsbalocationfolder/ratings.csv /dsbalocationfolder/output

	STEP 3:
	The output file is generated in the output folder in the dsba location.
	This file is copied on to the local device.

############# PART 2: USER-USER #############
We executed this part in DSBA cluster.

	STEP 1:
	We copied the iput files into the dsba cluster.
		$ hadoop fs -put /dsbalocationfolder/ /home/cloudera/ratings.csv

	STEP 2:
		$ spark-submit uu.py /dsbalocationfolder/ratings.csv /dsbalocationfolder/output

	STEP 3:
	The output file is generated in the output folder in the dsba location.
	This file is copied on to the local device.

############# PART 3: ALS #############
We executed the ALS part in Amazon Web Services.

	STEP 1:
	First, we created a bucket and loaded the bucket with the input files and with the source code.

	STEP 2:
	Create an cluster with required number of nodes and connect to it through the ssh command given in description(assuming we are unsing LINUX)
	STEP 3:
	Then we downloaded the inputs from the AWS into the hadoop by using the following commands:
		$ aws s3 cp s3://bucketname/books.csv ./
		$ aws s3 cp s3://bucketname/ratings.csv ./
		$ aws s3 cp s3://bucketname/ALSfinal.py ./

	STEP 4:
		$ spark-submit ALSfinal.py s3n://bucketname/books.csv s3n://bucketname/ratings.csv s3n://bucketname/output

	STEP 5:
	To extract the files from the bucket , we used a software called "S3 Browser". With the help of this, we can connect to our aws account and download all the contents of a folder at a time.
	The output consists of error and result file. 
	Error file consists of root mean square error for the test and the train data.
	Result file consists of the top ten recommendations for each user.
	
	STEP 6:
	Then we appended all the individual files into a single file(result and error) and displayed it in our report.
