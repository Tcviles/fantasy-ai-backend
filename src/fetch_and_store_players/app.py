import json
import os
import boto3
import requests

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}

def lambda_handler(event, context):
    url = "https://api.sleeper.app/v1/players/nfl"
    response = requests.get(url)
    players = response.json()

    put_count = 0
    skipped_count = 0

    for player_id, data in players.items():
        position = data.get("position")
        team = data.get("team")
        full_name = data.get("search_full_name")

        # Filter out players with invalid position, missing team, or missing name
        if position not in VALID_POSITIONS or not team or not full_name:
            skipped_count += 1
            continue

        item = {
            "player_id": player_id,
            "first_name": data.get("first_name"),
            "last_name": data.get("last_name"),
            "search_full_name": full_name,
            "team": team,
            "position": position,
            "injury_status": data.get("injury_status"),
            "status": data.get("status"),
        }

        table.put_item(Item=item)
        put_count += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "players_loaded": put_count,
            "players_skipped": skipped_count
        })
    }
