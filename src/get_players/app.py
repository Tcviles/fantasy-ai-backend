import os
import json
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}

def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    position = params.get("position", "All").upper()

    if position != "ALL" and position not in VALID_POSITIONS:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid position"})
        }

    if position == "ALL":
        # Scan returns everything — consider paginating for large results
        response = table.scan()
        players = response.get("Items", [])
    else:
        # Query the GSI
        response = table.query(
            IndexName="PositionIndex",
            KeyConditionExpression=boto3.dynamodb.conditions.Key("position").eq(position)
        )
        players = response.get("Items", [])

    return {
        "statusCode": 200,
        "body": json.dumps(players)
    }
