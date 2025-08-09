import json
import os
import time
import boto3
import requests
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}

def to_int_or_default(val, default):
    try:
        # Sleeper sometimes returns strings; make sure we store NUMBER
        if val is None:
            return default
        if isinstance(val, Decimal):
            return int(val)
        return int(val)
    except Exception:
        return default

def lambda_handler(event, context):
    start_time = time.time()
    url = "https://api.sleeper.app/v1/players/nfl"
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    players = response.json()
    total_players = len(players)

    print(f"Fetched {total_players} total players from Sleeper")

    put_count = 0
    skipped_count = 0

    # Higher default rank so unknowns sort to bottom
    DEFAULT_SEARCH_RANK = 99999

    with table.batch_writer(overwrite_by_pkeys=["player_id"]) as batch:
        for player_id, data in players.items():
            position = data.get("position")
            team = data.get("team")
            first_name = data.get("first_name") or ""
            last_name = data.get("last_name") or ""
            full_name = data.get("search_full_name") or f"{first_name} {last_name}".strip()

            # Filter to only valid, usable records
            if (
                position not in VALID_POSITIONS
                or not team
                or not full_name
                or full_name.lower() == "duplicateplayer"
            ):
                skipped_count += 1
                continue

            # Normalize casing to match your UI (teams uppercase, positions uppercase)
            position = position.upper()
            team = team.upper()

            search_rank = to_int_or_default(data.get("search_rank"), DEFAULT_SEARCH_RANK)
            depth_chart_order = to_int_or_default(data.get("depth_chart_order"), None)

            item = {
                "player_id": str(player_id),  # ensure string PK
                "first_name": first_name,
                "last_name": last_name,
                "search_full_name": full_name,
                "team": team,
                "position": position,
                "injury_status": data.get("injury_status"),
                "search_rank": search_rank,           # NUMBER for GSI sort key
                # write pos_team for the new GSI
                "pos_team": f"{position}#{team}",
            }

            if depth_chart_order is not None:
                item["depth_chart_order"] = depth_chart_order

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
