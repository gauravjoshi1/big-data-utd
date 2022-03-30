# Output key-value pairs of users and their mutual friends
def generate_mutual_friends(line):
    # Get user and their friends
    user = line[0]
    friendList = line[1].split(",")
    
    # list of (user, friend) and their mutual friends
    mutual_friends = []
    
    for friend in friendList:    
        if (friend != ''):
            if int(user) < int(friend):
                mutual_friend = (user + ", " + friend, set(friendList))
            else:
                mutual_friend = (friend + ", " + user, set(friendList))
            mutual_friends.append(mutual_friend)
    return mutual_friends

def formatOutput(number_of_friends):
    pair = number_of_friends[0].split(",")
    result = [i.lstrip() for i in pair]
    result = (result[0], result[1])
    output = "{}\t{}".format(result, number_of_friends[1])
    return output
  
# Read data from HDFS
text_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/mutual.txt")

# lines is a list where each line is a list containing 1 user and their mutual friends  
lines = text_data.map(lambda x : x.split("\t")).filter(lambda y : len(y) > 1)

# generate mutual friends as key-value pairs
mutual_friends = lines.flatMap(generate_mutual_friends)

# assemble all the mutual friends from across the clusters
friend_overlap = mutual_friends.reduceByKey(lambda x, y : x.intersection(y)) 

# Get total number of friends 
number_of_friends = friend_overlap.map(lambda x : (x[0], len(x[1]))).sortBy(lambda x : x[0]) 

# create a list of mutual friends
list_of_mutual_friends = number_of_friends.map(lambda x : x[1])

# Calculate sum and count of the list to calculate number of average friends
sum_of_friends_in_dataset = list_of_mutual_friends.sum()
num_of_friends_in_dataset = list_of_mutual_friends.count()
avg_friends_in_dataset = sum_of_friends_in_dataset/num_of_friends_in_dataset

# Filter out people with below average friends
below_avg_friends = number_of_friends.filter(lambda x: x[1] < avg_friends_in_dataset)

output2 = below_avg_friends.map(formatOutput)
output2.coalesce(1).saveAsTextFile("/FileStore/Output_2")