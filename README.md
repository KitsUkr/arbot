# arbot

Async scanner for complementary arbitrage between Polymarket and Predict.fun.

Sends Telegram alerts when it finds: `YES_ask_on_A + NO_ask_on_B + fees < 1`.
Does not auto-trade — you execute manually after verifying the alert.

## How it works

Every `POLL_INTERVAL_SECONDS`:

1. Pull active binary markets from both venues (REST).
2. Match them across venues by normalized title (rapidfuzz token_set_ratio)
   plus an expiration-proximity guard.
3. For each matched pair, check both directions:
   `Buy YES @ A.ask + Buy NO @ B.ask`. If `total + fees < 1 - MIN_PROFIT_THRESHOLD`,
   emit an opportunity.
4. Deduplicate so the same opp doesn't spam.
5. Send a Telegram alert.

## Run locally

```bash
cp .env.example .env  # fill TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
pip install -e ".[dev]"
pytest
python -m arbot
```

## Run in Docker

```bash
docker build -t arbot .
docker run -d --name arbot --restart unless-stopped --env-file .env arbot
docker logs -f arbot
```

## Notes / known limitations

- REST polling, not WebSocket. Sufficient for ~15s intervals.
- Predict.fun API field names in `clients/predictfun.py` are best-effort
  placeholders — verify against real responses and adjust
  `_to_market_quote` / `_extract_outcome` if needed.
- No persistence — dedup is in-memory and resets on restart.
- Fees are flat bps per venue.
- Title matching is fuzzy only. Different events with similar wording can
  produce false matches; tune `TITLE_SIMILARITY_THRESHOLD` and
  `MAX_EXPIRY_DELTA_HOURS`. Always check the `Title match` score in alerts.
- This is a **scanner**, not an executor. It alerts; you trade manually.
