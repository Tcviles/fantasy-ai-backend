# Fantasy AI backend

AWS SAM stack for a compact player catalog and the two AI decision endpoints.

## Architecture

- EventBridge runs `BuildPlayerCatalogFunction` once daily at 08:17 UTC.
- The builder fetches Sleeper once, normalizes fantasy-relevant players, and only writes changed records to DynamoDB.
- Missing players are retained as inactive records so trades, retirements, and roster cuts do not require destructive table resets.
- The same run writes a compact, versioned JSON snapshot to private S3 for fast mobile reads.
- `GET /players` returns a 300-player baseline by default. `position`, `team`, `q`, `ids`, and `limit` support deeper on-demand reads without transferring the full snapshot.
- `POST /compare` and `POST /keepers` retain the existing OpenAI-backed tools.

The app reads the compact snapshot instead of scanning DynamoDB. DynamoDB remains the normalized source of truth without requiring GSIs for mobile filtering.

## Deploy

```sh
sam build
sam deploy --guided
```

After the first deployment, invoke the catalog builder once using the function name in the stack output. Scheduled refreshes take over after that.
