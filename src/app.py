import json
import os
import boto3
import requests

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

def lambda_handler(event, context):
    url = "https://api.sleeper.app/v1/players/nfl"
    response = requests.get(url)
    players = response.json()

    put_count = 0

    for player_id, data in players.items():
        if not data.get("search_full_name"): continue  # Skip unnamed players

        item = {
            "player_id": player_id,
            "first_name": data.get("first_name"),
            "last_name": data.get("last_name"),
            "search_full_name": data.get("search_full_name"),
            "team": data.get("team"),
            "position": data.get("position"),
            "injury_status": data.get("injury_status"),
            "status": data.get("status"),
            "depth_chart_position": data.get("depth_chart_position"),
        }

        table.put_item(Item=item)
        put_count += 1

    return {
        "statusCode": 200,
        "body": json.dumps({"players_loaded": put_count})
    }
