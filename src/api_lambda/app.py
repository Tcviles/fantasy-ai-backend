import os
import json
import boto3

sqs = boto3.client("sqs")
QUEUE_URL = os.environ["QUEUE_URL"]

def lambda_handler(event, context):
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({"action": "sync_players"})
    )
    return {
        "statusCode": 202,
        "body": json.dumps({"message": "Player sync initiated"})
    }
