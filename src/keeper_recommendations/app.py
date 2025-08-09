import os
import json
import boto3
from decimal import Decimal
from openai import OpenAI

# --- SSM secrets ---
def get_openai_api_key():
    ssm = boto3.client("ssm")
    resp = ssm.get_parameter(
        Name="/fantasy-ai/openai_api_key",
        WithDecryption=True
    )
    return resp["Parameter"]["Value"]

client = OpenAI(api_key=get_openai_api_key())

# --- Helpers ---
def to_round_pick_str(round_num: int, pick_num: int) -> str:
    try:
        return f"{int(round_num)}.{int(pick_num)}"
    except Exception:
        return "-.-"

def clean_decimals(obj):
    if isinstance(obj, list):
        return [clean_decimals(i) for i in obj]
    if isinstance(obj, dict):
        return {k: clean_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def bad_request(msg: str):
    return {"statusCode": 400, "body": json.dumps({"error": msg})}

# --- Lambda handler ---
def lambda_handler(event, context):
    print("Event received:", json.dumps(event)[:2000])  # avoid massive logs

    if "body" not in event or not event["body"]:
        return bad_request("Missing body.")

    try:
        body = json.loads(event["body"])
    except Exception:
        return bad_request("Body must be valid JSON.")

    league = body.get("league") or {}
    players = body.get("players") or []

    # Validate league
    required_league_keys = ["teams", "format", "qb_slots", "your_slot", "keepers_allowed"]
    for k in required_league_keys:
        if k not in league:
            return bad_request(f"league.{k} is required")

    try:
        teams = int(league["teams"])
        keepers_allowed = int(league["keepers_allowed"])
        your_slot = int(league["your_slot"])
    except Exception:
        return bad_request("league.teams, league.keepers_allowed, and league.your_slot must be integers.")

    if teams <= 0 or keepers_allowed < 0 or your_slot <= 0 or your_slot > teams:
        return bad_request("Invalid league values (teams > 0, 0<=keepers_allowed, 1<=your_slot<=teams).")

    if not isinstance(players, list) or len(players) == 0:
        return bad_request("players array is required and must be non-empty.")

    # Precompute useful keeper context
    # Expected opponent keepers (everyone except you keeps up to keepers_allowed).
    opponent_keepers = keepers_allowed * max(0, teams - 1)

    # Convert player list into a compact, consistent view for the model
    # Each item: {name, team, keep_overall, keep_round, keep_pick}
    compact_players = []
    for p in players:
        name = p.get("player") or ""
        meta = p.get("meta") or {}
        rd = meta.get("round")
        pk = meta.get("pick")
        keep_overall = p.get("keeper_overall")
        team_abbr = meta.get("team_abbr") or ""

        if not name or rd is None or pk is None or keep_overall is None:
            return bad_request("Each player needs: player (name), meta.round, meta.pick, keeper_overall.")

        compact_players.append({
            "name": name,
            "team": team_abbr,
            "keep_overall": int(keep_overall),
            "keep_round": int(rd),
            "keep_pick": int(pk),
            "keep_str": to_round_pick_str(rd, pk),
        })

    # --- System & User Prompts ---
    # We require strict JSON output so the app can parse it reliably.
    system_prompt = (
        "You are a sharp, up-to-date fantasy football analyst for the 2025 season. "
        "You evaluate keeper values in PPR/Half-PPR/Standard 1QB or 2QB/Superflex formats. "
        "Given a user's league settings and a list of potential keepers with their keeper costs, "
        "pick the best keepers based on value versus current ADP and role outlook. "
        "Incorporate the latest injuries/suspensions/roles you know about. "
        "Very important: Higher overall picks are more valuable than lower ones; draft capital matters. "
        "Assume other managers will also keep players (use the provided opponent_keepers hint). "
        "If any injury/status/ADP detail is uncertain, state that assumption in 'assumptions'. "
        "Return ONLY valid JSON that conforms to the requested schema. No extra text."
    )

    # JSON schema the model must return (kept small but useful)
    # - keep: up to keepers_allowed best options.
    # - bench: remaining evaluated players with brief notes.
    # - value_vs_adp: positive = good value, negative = reach; units are 'overall picks saved (+)' or 'lost (-)'.
    # - risk_notes: 1-2 bullets of risk/injury/suspension/role uncertainty.
    # - reasoning: 2-4 sentences summarizing the logic for that player.
    # - summary: short league-level TL;DR.
    schema_explanation = """
Return strict JSON with this shape:

{
  "assumptions": {
    "opponent_keepers": number,
    "notes": string
  },
  "recommendations": {
    "keep": [
      {
        "player": string,
        "team": string,
        "keep_round": number,
        "keep_pick": number,
        "keep_overall": number,
        "estimated_adp_overall": number,
        "value_vs_adp": number,
        "risk_notes": [string],
        "reasoning": string
      }
    ],
    "bench": [
      {
        "player": string,
        "team": string,
        "keep_round": number,
        "keep_pick": number,
        "keep_overall": number,
        "estimated_adp_overall": number,
        "value_vs_adp": number,
        "risk_notes": [string],
        "reasoning": string
      }
    ]
  },
  "summary": string
}

Rules:
- "keep" must contain at most LEAGUE_KEEPERS_ALLOWED players, ranked best to worst.
- "estimated_adp_overall" is your best current estimate for 2025 overall ADP (state uncertainty if needed).
- "value_vs_adp" = estimated_adp_overall - keep_overall (positive is good value, negative is bad).
- Prefer keeping elite players at a discount even if the discount is small; adjust for positional/format (QB slots, PPR/Half/Std).
- Consider that OPPORTUNITY COST increases sharply in early rounds.
- Keep the reasoning concise and focused on value, role, team situation, and risk.
"""

    league_summary = {
        "teams": teams,
        "format": league.get("format", "PPR"),
        "qb_slots": int(league.get("qb_slots", 1)),
        "your_slot": your_slot,
        "keepers_allowed": keepers_allowed,
        "opponent_keepers_hint": opponent_keepers
    }

    user_payload = {
        "league": league_summary,
        "players": compact_players
    }

    user_prompt = (
        "LEAGUE:\n"
        + json.dumps(league_summary, separators=(",", ":"))
        + "\n\nCANDIDATES:\n"
        + json.dumps(compact_players, separators=(",", ":"))
        + "\n\nTASK:\nEvaluate keeper value vs ADP under these league rules. "
          "Account for other teams also keeping players (opponent_keepers_hint). "
          "Output STRICT JSON per the schema below. Do not include backticks or any extra prose."
        + "\n\nSCHEMA:\n"
        + schema_explanation.strip()
    )

    print("Keeper request (condensed):", json.dumps(user_payload)[:1000])

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
        )
        content = resp.choices[0].message.content
        # Ensure JSON
        data = json.loads(content)

        # Optional: sanity trim "keep" to keepers_allowed
        keep_list = (data.get("recommendations", {}).get("keep") or [])[:keepers_allowed]
        data["recommendations"]["keep"] = keep_list

        return {
            "statusCode": 200,
            "body": json.dumps(clean_decimals(data))
        }

    except Exception as e:
        # If the model returns non-JSON or anything else goes wrong, bubble an error
        print("OpenAI error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to generate recommendations."})
        }
