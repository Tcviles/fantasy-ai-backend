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

def collect_all_pages(fetch_page, **kwargs):
    items = []
    last_evaluated_key = None

    while True:
        request_kwargs = dict(kwargs)
        if last_evaluated_key:
            request_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = fetch_page(**request_kwargs)
        items.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")

        if not last_evaluated_key:
            return items

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    params = event.get("queryStringParameters") or {}
    position = (params.get("position") or "").upper().strip()
    team = (params.get("team") or "").upper().strip()

    print(f"Requested position: {position or '-'}, team: {team or '-'}")

    try:
        players = []

        if position and team:
            if position not in VALID_POSITIONS:
                return {"statusCode": 400, "body": json.dumps({"error": "Invalid position"})}

            pos_team = f"{position}#{team}"
            print(f"Query {POS_TEAM_INDEX} for pos_team = {pos_team}")

            players = collect_all_pages(
                table.query,
                IndexName=POS_TEAM_INDEX,
                KeyConditionExpression=Key("pos_team").eq(pos_team),
                ProjectionExpression=PROJECTION_EXPR,
                ExpressionAttributeNames=PROJECTION_NAMES,
            )

        elif position:
            if position not in VALID_POSITIONS:
                return {"statusCode": 400, "body": json.dumps({"error": "Invalid position"})}

            print(f"Query GSI for position = {position}")
            players = collect_all_pages(
                table.query,
                IndexName="PositionIndex",  # <-- ⚠️ You must have a GSI on 'position'
                KeyConditionExpression=Key("position").eq(position),
                ProjectionExpression=PROJECTION_EXPR,
                ExpressionAttributeNames=PROJECTION_NAMES,
            )

        else:
            print("Scan all players (no filters)")
            players = collect_all_pages(
                table.scan,
                ProjectionExpression=PROJECTION_EXPR,
                ExpressionAttributeNames=PROJECTION_NAMES,
            )

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
