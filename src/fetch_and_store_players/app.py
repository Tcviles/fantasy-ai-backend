import json
import os
import time
import boto3
import requests

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}

def lambda_handler(event, context):
    start_time = time.time()
    url = "https://api.sleeper.app/v1/players/nfl"
    response = requests.get(url)
    players = response.json()
    total_players = len(players)

    print(f"Fetched {total_players} total players from Sleeper")

    put_count = 0
    skipped_count = 0

    with table.batch_writer(overwrite_by_pkeys=["player_id"]) as batch:
        for player_id, data in players.items():
            position = data.get("position")
            team = data.get("team")
            full_name = data.get("search_full_name")

            # Filter out players with invalid data
            if (
                position not in VALID_POSITIONS
                or not team
                or not full_name
                or full_name.lower() == "duplicateplayer"
            ):
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
                "depth_chart_order": data.get("depth_chart_order"),
                "search_rank": data.get("search_rank")
            }

            batch.put_item(Item=item)
            put_count += 1

    elapsed = time.time() - start_time
    print(f"Inserted {put_count} players, skipped {skipped_count}, elapsed time: {elapsed:.2f} seconds")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total_players_fetched": total_players,
            "players_loaded": put_count,
            "players_skipped": skipped_count,
            "duration_seconds": round(elapsed, 2)
        })
    }
