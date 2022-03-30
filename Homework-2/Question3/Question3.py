# Question 3
# Read data from HDFS
user_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/user.csv")
business_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/business.csv")
review_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/review.csv")

# split input files into list
businesses = business_data.map(lambda x : x.split("::"))
users = user_data.map(lambda x : x.split("::"))
review = review_data.map(lambda x : x.split("::"))

# Get all the businesses in stanford
businesses_in_stanford = businesses.filter(lambda x : 'Stanford' in x[1])

# Get business id of businesses in stanford 
bid_of_stanford = businesses_in_stanford.map(lambda x : (x[0], 0, 0))

# reviews with business id, name of the reviewer, rating
reviews_with_bid = review.map(lambda x : (x[2], (x[1], x[3])))

# Get all the reviews in stanford in the format user id, review rating, business id
stanford_reviews = reviews_with_bid.join(bid_of_stanford).map(lambda x: (x[1][0][0], x[1][0][1], x[0]))

# Get the output for question 3
output3 = stanford_reviews.join(users).map(lambda x : (x[1][1], x[1][0])).distinct()

# Format the output as required
output3 = output3.map(lambda x:"{}\t{}".format(x[0], float(x[1]))).distinct()
output3.coalesce(1).saveAsTextFile("/FileStore/Output_3")