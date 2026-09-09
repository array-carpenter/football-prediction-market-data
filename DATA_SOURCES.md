# Data sources

## Kalshi

The source database is `kalshi_football.sqlite`. It contains public markets,
trades, and scheduled market snapshots.

Historical trades recover Week 0 and Week 1 price paths and traded volume. Old
order-book states cannot be reconstructed when no collector recorded them.

## Polymarket

The source databases are `polymarket_nfl.sqlite` and
`polymarket_cfb.sqlite`. They contain event metadata, outcome-token mappings,
public trades, changed market snapshots, and changed full order books.

The public trade API has a deep-pagination limit. Some older, high-volume event
tails require an on-chain event-log backfill. The live collector prevents new
gaps while it runs.

## Novig

The source database is `novig_football.sqlite`. The collector gets current
Novig moneyline, spread, and total prices from The Odds API.

Novig does not publish a documented public market-data API. The available feed
does not include trades, volume, or order books. It also does not provide a
historical backfill on the current account. The collector stores changed price
and line snapshots from the time collection starts.

The current account has 500 request credits each month. One full CFB and NFL
capture uses six credits. The scheduler runs two captures each day.

## ESPN

The build downloads the ESPN college football and NFL team directories. ESPN
team IDs form the canonical team keys.

The build caches each response in `data/reference/`. Use `--refresh-teams` to
replace the cached response.

## Data terms

The repository code uses the MIT license. Source data remains subject to each
source provider's terms. The code license does not change the data terms.
