import praw
import json
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
reddit = praw.Reddit(
    client_id='2jut2DJLXe3RrpEG_FNyJQ',  # Replace with your client id
    client_secret='LnAtNcNbSMRIXN0WKcRmSgGXOzfVTg',  # Replace with your client secret
    user_agent='SentimentalAnalyser',  # Name of your application
    username='SentimentalAnalyser1',  # Your Reddit username
    # password='abbasigang@61'  # Your Reddit password
)

def get_comments(submission):
    submission.comments.replace_more(limit=0)
    return [comment.body for comment in submission.comments.list()]

for submission in reddit.subreddit("SuicideWatch").stream.submissions():
    post_data = {
        "title": submission.title,
        "comments": get_comments(submission)
    }
    print(post_data)
    producer.send('reddit-stream', post_data)