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
    
# Read data from HDFS
text_data = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/mutual.txt")

# lines is a list where each line is a list containing 1 user and their mutual friends  
lines = text_data.map(lambda x : x.split("\t")).filter(lambda y : len(y) > 1)
mutual_friends = lines.flatMap(generate_mutual_friends)

mutual_friends.collect()




