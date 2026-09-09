# Dataset schema

## Identifier rules

`team_id` is the canonical team key.

ESPN identifiers use this format:

```text
espn:{league}:{espn_team_id}
```

Examples:

- `espn:cfb:2026` is App State.
- `espn:cfb:61` is Georgia.
- `espn:nfl:17` is New England.

Teams absent from ESPN use this format:

```text
kalshi:{league}:{kalshi_team_uuid}
polymarket:{league}:{polymarket_team_id}
novig:{league}:{normalized_team_name}
```

Source identifiers remain available. Do not replace them with display names.

## `teams/`

One row exists for each canonical team.

| Column | Meaning |
| --- | --- |
| `team_id` | Canonical join key. |
| `espn_team_id` | ESPN team ID when available. |
| `league` | `cfb` or `nfl`. |
| `display_name` | Canonical display name. |
| `short_name` | Short ESPN name. |
| `location` | Team location or school name. |
| `nickname` | Team nickname. |
| `abbreviation` | Canonical abbreviation. |
| `slug` | URL-safe team name. |
| `logo_url` | ESPN logo URL when available. |

## `team_aliases/`

This table documents every accepted source name.

`normalized_alias` is used for matching. `alias_type` records the source of the
alias. Manual aliases take precedence over ambiguous ESPN aliases.

## `source_teams/`

This table maps venue identifiers and names to `team_id`.

The key is `(venue, league, source_team_id)`. The table preserves the original
name and abbreviation. It also records the match method and score.

## `markets/`

One row exists for each tradeable outcome token or Kalshi market contract in a
source league. A Polymarket contract with both league tags has one row for each
tag. Use `(venue, league, coalesce(asset_id, market_id))` as the unique key.

Important columns are:

| Column | Meaning |
| --- | --- |
| `venue` | `kalshi`, `polymarket`, or `novig`. |
| `league` | `cfb` or `nfl`. |
| `season` | Football season year. |
| `market_id` | Venue market identifier. |
| `event_id` | Venue event identifier. |
| `asset_id` | Polymarket outcome-token ID. Null for Kalshi. |
| `series_ticker` | Original Kalshi series. Null for other venues. |
| `source_market_type` | Original venue market type or series. |
| `market_type` | `moneyline`, `spread`, `total`, or `other`. |
| `outcome_label` | Original venue outcome label. |
| `outcome_team_id` | Canonical team represented by the outcome. |
| `away_team_id` | Canonical away-team key. |
| `home_team_id` | Canonical home-team key. |
| `line` | Parsed spread or total. |
| `volume` | Venue-reported cumulative volume. |
| `liquidity` | Venue-reported liquidity. |
| `open_interest` | Venue-reported open interest. |
| `data_source` | API or archive source for the row. |
| `metadata_quality` | `source` or `synthetic_from_ticker`. |

## `trades/`

One row exists for each unique captured trade.

The common columns include `trade_id`, `venue`, `league`, `season`, `market_id`,
`event_id`, `asset_id`, `traded_at`, `price`, `size`, `side`, `market_type`, and
the canonical team identifiers.

Prices use decimal dollars from 0 through 1. Size is the number of contracts or
outcome tokens.

`data_source` identifies the source. Current values are `kalshi_api`,
`polymarket_api`, and `becker_archive`.

Becker trade IDs are stable synthetic IDs. The builder hashes the ticker,
contract count, prices, taker side, and trade time. The source file does not
include a trade ID.

## `market_snapshots/`

Rows record changed or scheduled market state. Common fields include best bid,
best ask, last price, volume, 24-hour volume, liquidity, and open interest.

Historical Becker snapshots convert cent prices to decimal prices. They use
`data_source = 'becker_archive'`.

For Novig, `decimal_odds` is the quoted decimal price. `last_price` is the
inverse decimal price. It is an implied probability. It is not a trade price.
The `line` field records the spread or total for that snapshot.

## `order_books/`

Rows contain changed Polymarket book states. `book_hash` and `asset_id` identify
the state. The table includes best prices, total bid and ask depth, and the full
JSON price levels.

## Null rules

Fields that a venue does not supply remain null. A null does not mean zero.

Every `moneyline`, `spread`, and `total` game market must have both
`away_team_id` and `home_team_id`. The build writes unresolved records to
`data/audit/` and the quality tests reject them.

## Manifests

`dataset_manifest.parquet` records each output file, partition, row count, file
size, schema version, and build time.

`source_manifest.parquet` records each SQLite source table and each Parquet
source file. It includes the row count, file size, modification time, schema
version, and build time.
