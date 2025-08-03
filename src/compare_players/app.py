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

        player_list = "\n".join(
            [f"{i+1}. {p['search_full_name']} ({p['team']} - {p['position']})" for i, p in enumerate(players)]
        )

        prompt = (
            "You're a fantasy football expert. Help me decide who to draft.\n\n"
            f"Here are the players:\n{player_list}\n\n"
            "Who should I draft and why?"
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",  # or gpt-3.5-turbo
            messages=[{"role": "user", "content": prompt}]
        )

        answer = response.choices[0].message["content"].strip()
        return {"statusCode": 200, "body": json.dumps({"recommendation": answer})}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
