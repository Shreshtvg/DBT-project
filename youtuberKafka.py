import random
from kafka import KafkaProducer
import mysql.connector
import time

# connect to the MySQL database
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="root",
    database="youtubeDb"
)

# create a cursor to execute SQL queries
cursor = mydb.cursor()

# initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# continuously publish new tweets and hashtags to Kafka
while True:
    try:
        # select a random number of tweets to retrieve
        num_tweets = random.randint(1, 25)
        
        #----------------------------------------------------------------------------------

        # select total views per channel 
        sqlViews = "SELECT channelId, SUM(videoViewCount) AS totalViews FROM Videos GROUP BY channelId;"
        cursor.execute(sqlViews)
        # print the SQL query being executed
        print("Executing SQL query for total views per channel...")
        # publish each new videoId to the Kafka topic
        for channelId, totalViews in cursor:
            
            message = bytes(f"{channelId}:{totalViews}", encoding='utf-8')
        
            # print the data being processed
            print("Publishing total views:", channelId,":",totalViews)
        
            # publish the message to the Kafka topic
            producer.send('total_views_per_channel', value=message)

        #----------------------------------------------------------------------------------

        # select all channels with subscribers>50000
        sqlChannels = "SELECT channelId, subscriberCount, channelViewCount, videoCount, channelelapsedtime, channelCommentCount FROM Channels WHERE subscriberCount > 50000"
        cursor.execute(sqlChannels)

        # print the SQL query being executed
        print("Executing SQL query for youtube channels with subscriber count greater than 50000...")

        # publish each channel to the Kafka topic
        for channelId, subscriberCount in cursor:
           # serialize the row data to bytes
            message = bytes(f"{channelId}:{subscriberCount}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing channel ID:", channelId)
            # publish the channelID to the Kafka topic
            producer.send('channels_high_subscribers', value=message)

        #----------------------------------------------------------------------------------

        # select the top 10 videos with highest number of likes
        sqlTopLikes = "SELECT c.videoId, c.subscriberCount, c.channelId, SUM(im.likesPerSubscriber) AS totalLikesPerSubscriber FROM Channels c JOIN InteractionMetrics im ON c.channelId = im.channelId GROUP BY c.channelId, c.subscriberCount ORDER BY totalLikesPerSubscriber DESC LIMIT 10"
        cursor.execute(sqlTopLikes)

        # print the SQL query being executed
        print("Executing SQL query for top likes...")

        # publish each top tweet to the Kafka topic
        for videoId, channelId, totalLikesPerSubscriber in cursor:
             # serialize the row data to bytes
            message = bytes(f"{videoId},{channelId},{totalLikesPerSubscriber}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing videos with top likes:", videoId)
            # publish the message to the Kafka topic
            producer.send('videos_top_likes', value=message)

        #----------------------------------------------------------------------------------

        # select channels with high engagement 
        sqlHighEng = "SELECT c.channelId, SUM(im.likesPerSubscriber) / SUM(v.videoViewCount) AS engagement_ratio FROM Channels c JOIN InteractionMetrics im ON c.channelId = im.channelId JOIN Videos v ON im.videoId = v.videoId GROUP BY c.channelId ORDER BY engagement_ratio DESC;"
        
        # execute the SQL query to retrieve the channels with high engagement 
        cursor.execute(sqlHighEng)

        # print the SQL query being executed
        print("Executing SQL query for channels with high engagement...")

        # publish each channels to the Kafka topic
        for channelId, engagement_ratio in cursor:
            # serialize the row data to bytes
            message = bytes(f"{channelId}:{engagement_ratio}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing channels with high engagement:", channelId)
            # publish the message to the Kafka topic
            producer.send('top_channels_engagement', value=message)
        
        #----------------------------------------------------------------------------------

        # sleep for a random amount of time between 5 and 15 seconds
        time.sleep(random.randint(5, 15))
        
    except Exception as e:
        print(e)
        # in case of any error, sleep for a shorter time and try again
        time.sleep(2)