Backfills
=========

Backfills are better handled locally. Just run a development server:

```bash
BRONZE_BUCKET=meatislife-bronze-bucket uv run functions-framework --source=src/synthesise.py --target=generate_and_upload
```
from the `synthetic-meat` directory.

Then, in another terminal, send a `curl` with the desired start and
end dates:

```bash
curl localhost:8080 -d '{"from_date":"2025-10-20", "to_date": "2025-10-25"}' -H "Content-Type: application/json"
```

There's little to be gained by automating that. But documentation is
better than memory!
