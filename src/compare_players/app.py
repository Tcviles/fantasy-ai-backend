import os
import json
import openai

openai.api_key = os.environ["OPENAI_API_KEY"]

def lambda_handler(event, context):
    try:
        body = json.loads(event["body"])
        players = body.get("players", [])

        if not players:
            return {"statusCode": 400, "body": json.dumps({"error": "No players provided."})}

        # Format structured player list
        player_list = "\n".join(
            [
                f"{i+1}. {p.get('search_full_name')} | Team: {p.get('team')} | Position: {p.get('position')} | "
                f"Injury: {p.get('injury_status') or 'None'} | Depth: {p.get('depth_chart_order')} | "
                f"Age: {p.get('age')} | Rank: {p.get('search_rank')}"
                for i, p in enumerate(players)
            ]
        )

        system_prompt = (
            "You are a fantasy football expert for the 2025 NFL season. "
            "Your job is to help users make the best PPR draft choices based on current data. "
            "Use only the structured data provided. "
            "Do NOT use stats or roles from before the 2024 season. "
            "Base your recommendation on team roles, injuries, age, and projected usage in 2025. "
            "Respond conversationally, as if advising a fantasy football player in a real draft."
        )

        user_prompt = (
            f"Here are the draftable players:\n\n{player_list}\n\n"
            "In a 12-team PPR draft, who should I pick and why? Respond with one recommendation and a clear reason."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )

        answer = response.choices[0].message["content"].strip()
        return {"statusCode": 200, "body": json.dumps({"recommendation": answer})}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
