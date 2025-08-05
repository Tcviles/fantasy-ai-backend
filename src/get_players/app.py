import os
import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    params = event.get("queryStringParameters") or {}
    position = params.get("position", "All").upper()
    print(f"Requested position: {position}")

    if position != "ALL" and position not in VALID_POSITIONS:
        print("Invalid position:", position)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid position"})
        }

    try:
        if position == "ALL":
            print("Scanning full table...")
            response = table.scan()
        else:
            print(f"Querying for position: {position}")
            response = table.query(
                IndexName="PositionIndex",
                KeyConditionExpression=Key("position").eq(position)
            )

        players = response.get("Items", [])
        print(f"Retrieved {len(players)} players")

        # Sort by search_rank (defaulting to a high number if missing or invalid)
        def get_search_rank(player):
            try:
                return int(player.get("search_rank", 99999))
            except (ValueError, TypeError):
                return 99999

        players.sort(key=get_search_rank)
        print("Sorted players by search_rank")

        return {
            "statusCode": 200,
            "body": json.dumps(players)
        }

    except Exception as e:
        print("Error occurred:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"})
        }
