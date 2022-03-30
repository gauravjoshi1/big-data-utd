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
number_of_friends = friend_overlap.map(lambda x : (x[0], len(x[1]))).filter(lambda x : x[1] > 0).sortBy(lambda x : x[0])    

# format the output  
output = number_of_friends.map(formatOutput)

# save the file on DBFS
output.coalesce(1).saveAsTextFile("/FileStore/Output_1")