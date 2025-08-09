import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    params = event.get("queryStringParameters") or {}
    position = (params.get("position") or "ALL").upper()
    team = (params.get("team") or "").upper().strip()

    print(f"Requested position: {position}, team: {team or '-'}")

    if position != "ALL" and position not in VALID_POSITIONS:
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid position"})}

    try:
        # Fast path: both position and team
        if position != "ALL" and team:
            pos_team = f"{position}#{team}"
            print(f"Query PosTeamRankIndex for {pos_team}")
            response = table.query(
                IndexName="PosTeamRankIndex",
                KeyConditionExpression=Key("pos_team").eq(pos_team),
                ProjectionExpression="player_id, first_name, last_name, team, position, search_rank"
            )

        # Position-only path (existing index)
        elif position != "ALL":
            print(f"Query PositionIndex for position={position}")
            response = table.query(
                IndexName="PositionIndex",
                KeyConditionExpression=Key("position").eq(position),
                ProjectionExpression="player_id, first_name, last_name, team, position, search_rank"
            )
            # Optional filter by team if provided but GSI not ready
            if team:
                print(f"Filtering in-memory for team={team}")
                items = [p for p in response.get("Items", []) if (p.get("team") or "").upper() == team]
                response["Items"] = items

        # Fallback: full scan (avoid when possible)
        else:
            print("Scanning full table (ALL). Avoid in prod.")
            response = table.scan(
                ProjectionExpression="player_id, first_name, last_name, team, position, search_rank"
            )

        players = response.get("Items", [])
        print(f"Retrieved {len(players)} players")

        # Sort by search_rank (ascending)
        def get_rank(p):
            try: return int(p.get("search_rank", 99999))
            except Exception: return 99999
        players.sort(key=get_rank)

        return {"statusCode": 200, "body": json.dumps(convert_decimals(players))}

    except Exception as e:
        print("Error occurred:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": "Internal server error"})}
