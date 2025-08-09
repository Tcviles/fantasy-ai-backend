import os
import json
import boto3
from decimal import Decimal
from openai import OpenAI

# ---------- Secrets / Clients ----------

def get_openai_api_key():
    ssm = boto3.client("ssm")
    resp = ssm.get_parameter(Name="/fantasy-ai/openai_api_key", WithDecryption=True)
    return resp["Parameter"]["Value"]

client = OpenAI(api_key=get_openai_api_key())

# ---------- Helpers ----------

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

# ---------- Capital weighting ----------

def round_from_overall(overall: int, teams: int) -> int:
    """Map overall pick number to draft round (1-indexed)."""
    return ((max(1, int(overall)) - 1) // max(1, teams)) + 1

# Heavier weights up top; gentle taper later. Tune as desired.
ROUND_WEIGHTS = {
    1: 3.0,  2: 2.5,  3: 2.0,  4: 1.7,  5: 1.4,  6: 1.2,  7: 1.1,  8: 1.0,
    9: 0.95, 10: 0.90, 11: 0.85, 12: 0.80, 13: 0.76, 14: 0.72, 15: 0.70,
    16: 0.68, 17: 0.66, 18: 0.64, 19: 0.62, 20: 0.60
}

def capital_weight_for(overall: int, teams: int) -> float:
    r = round_from_overall(overall, teams)
    if r in ROUND_WEIGHTS:
        return ROUND_WEIGHTS[r]
    # beyond configured rounds, taper slowly
    return max(0.55, 0.60 - 0.01 * max(0, r - 20))

def normalize_value_and_sort_with_capital(data: dict, keepers_allowed: int, teams: int):
    """
    Enforce correct math: value_vs_adp = keep_overall - estimated_adp_overall (positive = good).
    Apply round-based capital weighting and sort by adjusted_value desc.
    """
    def fix_list(items):
        fixed = []
        for it in items or []:
            try:
                ko = int(it.get("keep_overall"))
                adp = int(it.get("estimated_adp_overall"))
            except Exception:
                # If the model omitted required numbers, skip this entry
                continue

            # Raw picks saved: positive is good (you pay a later/less valuable pick than market)
            raw = ko - adp

            # Anchor the capital weight at the earlier (more valuable) of keep/adp
            anchor_overall = min(ko, adp)
            w = capital_weight_for(anchor_overall, teams)

            adjusted = raw * w
            it["value_vs_adp"] = raw
            it["capital_weight"] = round(w, 2)
            it["adjusted_value"] = adjusted
            fixed.append(it)
        return fixed

    recs = data.get("recommendations", {}) or {}
    keep = fix_list(recs.get("keep"))
    bench = fix_list(recs.get("bench"))

    # Sort primarily by adjusted value (desc), tie-breaker: better (smaller) ADP
    keep.sort(key=lambda x: (x.get("adjusted_value", -1e9), -int(x.get("estimated_adp_overall", 99999))), reverse=True)
    bench.sort(key=lambda x: (x.get("adjusted_value", -1e9), -int(x.get("estimated_adp_overall", 99999))), reverse=True)

    # Cap keepers to league rule
    keep = keep[:keepers_allowed]

    data.setdefault("recommendations", {})
    data["recommendations"]["keep"] = keep
    data["recommendations"]["bench"] = bench
    return data

# ---------- Lambda Handler ----------

def lambda_handler(event, context):
    print("Event received:", json.dumps(event)[:2000])

    if "body" not in event or not event["body"]:
        return bad_request("Missing body.")

    try:
        body = json.loads(event["body"])
    except Exception:
        return bad_request("Body must be valid JSON.")

    league = body.get("league") or {}
    players = body.get("players") or []

    required_league_keys = ["teams", "format", "qb_slots", "your_slot", "keepers_allowed"]
    for k in required_league_keys:
        if k not in league:
            return bad_request(f"league.{k} is required")

    try:
        teams = int(league["teams"])
        keepers_allowed = int(league["keepers_allowed"])
        your_slot = int(league["your_slot"])
        qb_slots = int(league.get("qb_slots", 1))
    except Exception:
        return bad_request("league.teams, league.keepers_allowed, league.qb_slots, and league.your_slot must be integers.")

    if teams <= 0 or keepers_allowed < 0 or your_slot <= 0 or your_slot > teams:
        return bad_request("Invalid league values (teams > 0, 0<=keepers_allowed, 1<=your_slot<=teams).")

    if not isinstance(players, list) or len(players) == 0:
        return bad_request("players array is required and must be non-empty.")

    # Rough hint: how many players opponents keep
    opponent_keepers = keepers_allowed * max(0, teams - 1)

    # Normalize candidates for the model
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

    # ---------- Prompting ----------

    system_prompt = (
        "You are a sharp, up-to-date fantasy football analyst for the 2025 season. "
        "Evaluate keeper values for the given league and candidates. "
        "IMPORTANT MATH: value_vs_adp = keep_overall - estimated_adp_overall (positive = good). "
        "Because smaller overall numbers are more valuable, paying a later pick than market is positive value. "
        "Example: ADP 15, keep 23 → 23 - 15 = +8 (good). "
        "Example: ADP 45, keep 30 → 30 - 45 = -15 (bad). "
        "Prefer meaningful discounts at premium draft capital; early rounds matter more. "
        "Account for others keeping players too (opponent_keepers). "
        "If any injury/status/ADP detail is uncertain, include it in 'assumptions.notes'. "
        "Return ONLY valid JSON per the schema (no extra text)."
    )

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
        "value_vs_adp": number,  // MUST equal keep_overall - estimated_adp_overall (positive = good)
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
        "value_vs_adp": number,  // MUST equal keep_overall - estimated_adp_overall
        "risk_notes": [string],
        "reasoning": string
      }
    ]
  },
  "summary": string
}

Rules:
- 'keep' must include at most LEAGUE_KEEPERS_ALLOWED players, ranked best to worst.
- Compute value_vs_adp exactly as described (positive good).
- Consider league format (PPR/Half/Standard) and QB slots (1QB vs 2QB/Superflex) when weighing positions.
- Include concise risk/injury/role notes.
"""

    league_summary = {
        "teams": teams,
        "format": league.get("format", "PPR"),
        "qb_slots": qb_slots,
        "your_slot": your_slot,
        "keepers_allowed": keepers_allowed,
        "opponent_keepers_hint": opponent_keepers
    }

    user_payload = {"league": league_summary, "players": compact_players}

    user_prompt = (
        "LEAGUE:\n" + json.dumps(league_summary, separators=(",", ":")) +
        "\n\nCANDIDATES:\n" + json.dumps(compact_players, separators=(",", ":")) +
        "\n\nTASK:\nEvaluate keeper value vs ADP under these league rules using the math and rules above. "
        "Output STRICT JSON per the schema. No backticks or extra prose."
        "\n\nSCHEMA:\n" + schema_explanation.strip()
    )

    print("Keeper request (condensed):", json.dumps(user_payload)[:1000])

    # ---------- Model Call ----------

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
        data = json.loads(content)

        # Server-side guardrails: fix math, apply capital weighting, and sort.
        data = normalize_value_and_sort_with_capital(
            data,
            keepers_allowed=keepers_allowed,
            teams=teams
        )

        return {
            "statusCode": 200,
            "body": json.dumps(clean_decimals(data))
        }

    except Exception as e:
        print("OpenAI error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to generate recommendations."})
        }
