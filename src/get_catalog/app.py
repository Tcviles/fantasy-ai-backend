import json
import os
import time

import boto3
from botocore.exceptions import ClientError

CATALOG_BUCKET = os.environ["CATALOG_BUCKET"]
CATALOG_KEY = os.environ.get("CATALOG_KEY", "players/v1/catalog.json")
CACHE_SECONDS = 300
DEFAULT_LIMIT = 300
MAX_LIMIT = 500

s3 = boto3.client("s3")
cache = {"loaded_at": 0.0, "catalog": None, "etag": None}


def response(status_code, body=None, etag=None):
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "public,max-age=300,stale-while-revalidate=3600",
    }
    if etag:
        headers["ETag"] = etag

    return {
        "statusCode": status_code,
        "headers": headers,
        "body": "" if body is None else json.dumps(body, separators=(",", ":")),
    }


def load_catalog():
    now = time.monotonic()
    if cache["catalog"] is not None and now - cache["loaded_at"] < CACHE_SECONDS:
        return cache["catalog"], cache["etag"]

    result = s3.get_object(Bucket=CATALOG_BUCKET, Key=CATALOG_KEY)
    catalog = json.load(result["Body"])
    etag = result.get("ETag")
    cache.update({"loaded_at": now, "catalog": catalog, "etag": etag})
    return catalog, etag


def lambda_handler(event, context):
    try:
        catalog, etag = load_catalog()
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "NoSuchKey":
            return response(503, {"error": "Player catalog has not been generated yet."})
        raise

    params = event.get("queryStringParameters") or {}
    position = (params.get("position") or "").upper().strip()
    team = (params.get("team") or "").upper().strip()
    query = (params.get("q") or "").lower().strip()
    requested_ids = {
        player_id.strip()
        for player_id in (params.get("ids") or "").split(",")
        if player_id.strip()
    }
    try:
        limit = min(max(int(params.get("limit") or DEFAULT_LIMIT), 1), MAX_LIMIT)
    except (TypeError, ValueError):
        return response(400, {"error": "limit must be a number"})

    all_players = catalog.get("players", [])
    players = all_players
    headers = {
        key.lower(): value for key, value in (event.get("headers") or {}).items()
    }
    if not position and not team and not query and not requested_ids and etag and headers.get("if-none-match") == etag:
        return response(304, etag=etag)

    if position:
        players = [player for player in players if player.get("position") == position]
    if team:
        players = [player for player in players if player.get("team") == team]
    if query:
        players = [
            player for player in players
            if query in " ".join((
                player.get("first_name", ""),
                player.get("last_name", ""),
                player.get("team", ""),
                player.get("position", ""),
            )).lower()
        ]

    players = [] if requested_ids and not position and not team and not query else players[:limit]
    if requested_ids:
        included_ids = {player.get("player_id") for player in players}
        players.extend(
            player for player in all_players
            if player.get("player_id") in requested_ids and player.get("player_id") not in included_ids
        )

    return response(
        200,
        {
            "generated_at": catalog.get("generated_at"),
            "catalog_version": catalog.get("catalog_version"),
            "source": catalog.get("source"),
            "total": len(all_players),
            "players": players,
        },
        etag=etag,
    )
