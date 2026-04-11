# Hyblock Probe

Use this probe to verify how far back Hyblock can provide historical data for
the six tracked perpetual symbols before building a full importer.

Required:

- `HYBLOCK_API_KEY`

Optional:

- `HYBLOCK_API_HEADER` (default: `X-API-KEY`)
- `HYBLOCK_EXCHANGE` (default: `binance_perp_stable`)
- `HYBLOCK_AVAILABILITY_URL`

Run:

```bash
HYBLOCK_API_KEY=... \
.venv/bin/python scripts/probe_hyblock_availability.py
```

Example output:

```text
exchange=binance_perp_stable
BTCUSDT    2023-12-06T10:15:00Z    binance_perp_stable    btcusdt
...
```
