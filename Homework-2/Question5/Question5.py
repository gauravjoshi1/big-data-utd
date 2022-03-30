# Read data from HDFS
user_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/user.csv")
review_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/review.csv")

# split input files into list
users = user_data.map(lambda x : x.split("::"))
review = review_data.map(lambda x : x.split("::"))

# Get total number of reviews
total_reviews = review.count()

# sum up all the ratings for business id
format_reviews = review.map(lambda x : (x[1], 1)).reduceByKey(lambda x,y: x+y)

# format users based on their id and name
format_users = users.map(lambda x : (x[0], x[1]))

# Get the total number of reviews for each user 
users_with_reviews = format_users.join(format_reviews).map(lambda x : (x[1][0], x[1][1]))

# Get user's contribution
user_review_contribution = users_with_reviews.map(lambda x : (x[0], x[1] * 100/total_reviews))
least_contributions = user_review_contribution.sortBy(lambda x : x[1])

# format output as required
output = least_contributions.map(lambda x:"{} \t{}".format(x[0], x[1]))