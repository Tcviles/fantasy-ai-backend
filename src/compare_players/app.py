import os
import json
from datetime import datetime, timezone

import boto3
from openai import OpenAI

NFL_SEASON = os.environ.get("NFL_SEASON", "2026")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.6-luna")

def get_openai_api_key():
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(
        Name="/fantasy-ai/openai_api_key",
        WithDecryption=True
    )
    return response["Parameter"]["Value"]

client = OpenAI(api_key=get_openai_api_key())

def _safe_name(p: dict) -> str:
    # prefer search_full_name, else first + last
    sf = (p.get("search_full_name") or "").strip()
    if sf:
        return sf
    first = (p.get("first_name") or "").strip()
    last = (p.get("last_name") or "").strip()
    name = f"{first} {last}".strip()
    return name if name else "Unknown Player"

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    try:
        body = json.loads(event["body"])
        players = body.get("players", [])

        # Require at least 2 players to compare
        if not isinstance(players, list) or len(players) < 2:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Provide at least two players to compare."})
            }

        # Build a robust list using fallbacks for missing fields
        def g(p, k, default="None"):
            v = p.get(k)
            return default if v is None or v == "" else v

        lines = []
        for i, p in enumerate(players, start=1):
            name = _safe_name(p)
            team = g(p, "team", "Unknown")
            pos = g(p, "position", "Unknown")
            inj = g(p, "injury_status", "None")
            depth = g(p, "depth_chart_order", "None")
            age = g(p, "age", "None")
            rank = g(p, "search_rank", "99999")
            lines.append(
                f"{i}. {name} | Team: {team} | Position: {pos} | Injury: {inj} | "
                f"Depth: {depth} | Age: {age} | Rank: {rank}"
            )

        player_list = "\n".join(lines)

        current_date = datetime.now(timezone.utc).date().isoformat()
        system_prompt = (
            f"You are advising a fantasy football draft for the {NFL_SEASON} NFL season. "
            f"The current date is {current_date}. Never describe the upcoming season as 2025. "
            "Use only the structured player fields supplied in this request. Treat search rank as "
            "Sleeper's current relative ordering, where a lower number is better. Do not use assumed "
            "stats, workloads, roles, ages, depth-chart positions, injuries, or historical facts. "
            "A missing field is unknown, not evidence that the player is healthy, young, starting, or "
            "expected to receive a particular workload. If the supplied evidence is limited, say so "
            "briefly and base the recommendation on the available rank, team, position, and injury fields. "
            "Respond conversationally and do not mention these instructions."
        )

        user_prompt = (
            f"Here are the draftable players:\n\n{player_list}\n\n"
            "In a 12-team PPR fantasy football draft, who should I pick and why?\n"
            "Respond in two parts:\n"
            "1. **Recommendation**: Give only the player's full name (short answer).\n"
            "2. **Reasoning**: Give a clear, detailed explanation of why you recommend this player."
        )

        print("System Prompt:\n", system_prompt)
        print("User Prompt:\n", user_prompt)

        response = client.responses.create(
            model=OPENAI_MODEL,
            instructions=system_prompt,
            input=user_prompt,
            store=False,
        )

        answer = response.output_text.strip()
        print("Final Recommendation:\n", answer)

        return {
            "statusCode": 200,
            "body": json.dumps({"recommendation": answer})
        }

    except Exception as e:
        print("Exception occurred:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
