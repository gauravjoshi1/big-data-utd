# Question 4

business_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/business.csv")
review_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/review.csv")

# split input files into list
businesses = business_data.map(lambda x : x.split("::"))
review = review_data.map(lambda x : x.split("::"))

# create a tuple for business with business_id, address, categories 
format_business = businesses.map(lambda x : (x[0], (x[1], x[2])))

# sum up all the ratings for business id
format_reviews = review.map(lambda x : (x[2], 1)).reduceByKey(lambda x,y: x+y)

# add ratings to each business
rating = format_business.join(format_reviews)

# format ratings
formatted_ratings = rating.map(lambda x : (x[0], x[1][0][0], x[1][0][1], float(x[1][1]))).distinct()

# Sort based on ratings
unformatted_output = formatted_ratings.sortBy(lambda x : x[3], ascending=False)

# format output as required
output4 = unformatted_output.map(lambda x:"{} {} {}\t{}".format(x[0], x[1], x[2], int(float(x[3])))).take(10)

for out in output4:
    print(out)
