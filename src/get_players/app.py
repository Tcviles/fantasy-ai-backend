import os
import json
import boto3
from boto3.dynamodb.conditions import Key
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

# Alias reserved words in ProjectionExpression
PROJECTION_NAMES = {
    "#pid": "player_id",
    "#fn": "first_name",
    "#ln": "last_name",
    "#team": "team",
    "#pos": "position",
    "#rank": "search_rank",
}
PROJECTION_EXPR = "#pid, #fn, #ln, #team, #pos, #rank"

# Use the new ALL-projected GSI by default
POS_TEAM_INDEX = os.environ.get("POS_TEAM_INDEX", "PosTeamRankIndexV2")

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    params = event.get("queryStringParameters") or {}
    position = (params.get("position") or "").upper().strip()
    team = (params.get("team") or "").upper().strip()

    print(f"Requested position: {position or '-'}, team: {team or '-'}")

    # Require BOTH
    if not position or not team:
        return {"statusCode": 400, "body": json.dumps({"error": "Both 'position' and 'team' are required."})}
    if position not in VALID_POSITIONS:
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid position"})}

    try:
        pos_team = f"{position}#{team}"
        print(f"Query {POS_TEAM_INDEX} for {pos_team}")

        response = table.query(
            IndexName=POS_TEAM_INDEX,
            KeyConditionExpression=Key("pos_team").eq(pos_team),
            ProjectionExpression=PROJECTION_EXPR,
            ExpressionAttributeNames=PROJECTION_NAMES,
        )

        players = response.get("Items", [])
        print(f"Retrieved {len(players)} players")

        # Sort by search_rank (ascending)
        def get_rank(p):
            try:
                return int(p.get("search_rank", 99999))
            except Exception:
                return 99999
        players.sort(key=get_rank)

        return {"statusCode": 200, "body": json.dumps(convert_decimals(players))}

    except Exception as e:
        print("Error occurred:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": "Internal server error"})}
