import json
import os
import hashlib
from datetime import datetime, timezone
from urllib.request import Request, urlopen

import boto3

SLEEPER_PLAYERS_URL = "https://api.sleeper.app/v1/players/nfl"
VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}
CATALOG_BUCKET = os.environ["CATALOG_BUCKET"]
CATALOG_KEY = os.environ.get("CATALOG_KEY", "players/v1/catalog.json")
PLAYERS_TABLE = os.environ["PLAYERS_TABLE"]

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(PLAYERS_TABLE)


def fetch_sleeper_players():
    request = Request(
        SLEEPER_PLAYERS_URL,
        headers={"Accept": "application/json", "User-Agent": "fantasy-draft-room/1.0"},
    )
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def optional_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def compact_player(player_id, source):
    position = (source.get("position") or "").upper()
    team = (source.get("team") or "").upper()
    first_name = (source.get("first_name") or "").strip()
    last_name = (source.get("last_name") or "").strip()

    if position not in VALID_POSITIONS or not team or not (first_name or last_name):
        return None

    player = {
        "player_id": str(player_id),
        "first_name": first_name,
        "last_name": last_name,
        "team": team,
        "position": position,
    }

    search_rank = optional_int(source.get("search_rank"))
    depth_chart_order = optional_int(source.get("depth_chart_order"))
    injury_status = source.get("injury_status")
    status = source.get("status")

    if search_rank is not None:
        player["search_rank"] = search_rank
    if depth_chart_order is not None:
        player["depth_chart_order"] = depth_chart_order
    if injury_status:
        player["injury_status"] = injury_status
    if status:
        player["status"] = status

    return player


def content_hash(player):
    content = json.dumps(player, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def scan_existing_players():
    existing = {}
    scan_kwargs = {
        "ProjectionExpression": "player_id, content_hash, catalog_version, active",
    }

    while True:
        result = table.scan(**scan_kwargs)
        for item in result.get("Items", []):
            existing[item["player_id"]] = item

        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            return existing
        scan_kwargs["ExclusiveStartKey"] = last_key


def sync_players(players, catalog_version, generated_at):
    existing = scan_existing_players()
    current_ids = {player["player_id"] for player in players}
    changed = 0
    deactivated = 0

    with table.batch_writer(overwrite_by_pkeys=["player_id"]) as batch:
        for player in players:
            player_id = player["player_id"]
            player_hash = content_hash(player)
            previous = existing.get(player_id)

            if (
                previous
                and previous.get("content_hash") == player_hash
                and previous.get("active") is True
            ):
                continue

            batch.put_item(
                Item={
                    **player,
                    "content_hash": player_hash,
                    "catalog_version": catalog_version,
                    "updated_at": generated_at,
                    "active": True,
                }
            )
            changed += 1

        for player_id, previous in existing.items():
            if player_id in current_ids or previous.get("active") is False:
                continue

            batch.put_item(
                Item={
                    **previous,
                    "active": False,
                    "catalog_version": catalog_version,
                    "updated_at": generated_at,
                }
            )
            deactivated += 1

    return changed, deactivated


def lambda_handler(event, context):
    source = fetch_sleeper_players()
    players = [
        compact
        for player_id, player in source.items()
        if (compact := compact_player(player_id, player)) is not None
    ]
    players.sort(key=lambda player: (player.get("search_rank", 999999), player["last_name"]))
    generated_at = datetime.now(timezone.utc).isoformat()
    catalog_version = hashlib.sha256(
        json.dumps(players, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    changed, deactivated = sync_players(players, catalog_version, generated_at)

    payload = json.dumps(
        {
            "generated_at": generated_at,
            "catalog_version": catalog_version,
            "source": "sleeper",
            "players": players,
        },
        separators=(",", ":"),
    ).encode("utf-8")

    s3.put_object(
        Bucket=CATALOG_BUCKET,
        Key=CATALOG_KEY,
        Body=payload,
        ContentType="application/json",
        CacheControl="public,max-age=3600,stale-while-revalidate=86400",
        ServerSideEncryption="AES256",
        Metadata={"catalog-version": catalog_version},
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "players": len(players),
                "changed": changed,
                "deactivated": deactivated,
                "catalog_version": catalog_version,
                "bytes": len(payload),
            }
        ),
    }
