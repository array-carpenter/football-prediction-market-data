# Data sources

## Kalshi

The source database is `kalshi_football.sqlite`. It contains public markets,
trades, and scheduled market snapshots.

Historical trades recover Week 0 and Week 1 price paths and traded volume. Old
order-book states cannot be reconstructed when no collector recorded them.

## Polymarket

The source database is `polymarket_nfl.sqlite`. It contains NFL event metadata,
outcome-token mappings, public trades, changed market snapshots, and changed
full order books.

The public trade API has a deep-pagination limit. Some older, high-volume event
tails require an on-chain event-log backfill. The live collector prevents new
gaps while it runs.

## ESPN

The build downloads the ESPN college football and NFL team directories. ESPN
team IDs form the canonical team keys.

The build caches each response in `data/reference/`. Use `--refresh-teams` to
replace the cached response.

## Data terms

The repository code uses the MIT license. Source data remains subject to each
source provider's terms. The code license does not change the data terms.
