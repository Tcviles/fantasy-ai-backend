import os
import json
import boto3
from openai import OpenAI

def get_openai_api_key():
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(
        Name="/fantasy-ai/openai_api_key",
        WithDecryption=True
    )
    return response["Parameter"]["Value"]

client = OpenAI(api_key=get_openai_api_key())

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    try:
        body = json.loads(event["body"])
        players = body.get("players", [])

        if not players:
            print("No players provided in request body.")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No players provided."})
            }

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
            "In a 12-team PPR fantasy football draft, who should I pick and why?\n"
            "Respond in two parts:\n"
            "1. **Recommendation**: Give only the player's full name (short answer).\n"
            "2. **Reasoning**: Give a clear, detailed explanation of why you recommend this player."
        )

        print("System Prompt:\n", system_prompt)
        print("User Prompt:\n", user_prompt)

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
        )

        print("Raw OpenAI Response:", response)

        answer = response.choices[0].message.content.strip()
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
