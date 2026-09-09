# Football Prediction Market Data

This repository builds a clean football prediction-market dataset from the
local Kalshi, Polymarket, and Novig collectors.

The curated data uses the same team identifier for all venues. ESPN team IDs
are the primary identifiers. A stable venue identifier is used only when ESPN
does not list a team.

## Current data

The build includes college football and NFL data from Kalshi, Polymarket, and
Novig. Novig supplies price snapshots. It does not supply public trades or
volume.

The build also includes a historical Kalshi NFL slice from the Becker archive.
This slice has 9,427,707 trades from July 16, 2025 through January 18, 2026.
It gives the dataset a previous season of NFL market movement.

The primary files are:

- `data/curated/teams/league=*/part-000.parquet`
- `data/curated/team_aliases/league=*/part-000.parquet`
- `data/curated/source_teams/league=*/part-000.parquet`
- `data/curated/markets/league=*/season=*/part-000.parquet`
- `data/curated/trades/**/*.parquet`
- `data/curated/market_snapshots/**/*.parquet`
- `data/curated/order_books/**/*.parquet`
- `data/curated/dataset_manifest.parquet`
- `data/curated/source_manifest.parquet`

The directory datasets use Hive partitions. The first partition is `league`.
Use `league=cfb` or `league=nfl` to load one league.

For example, these paths contain only CFB data:

```text
data/curated/markets/league=cfb/
data/curated/trades/league=cfb/
data/curated/market_snapshots/league=cfb/
```

`dataset_manifest.parquet` gives the row count and file size for each output.
`source_manifest.parquet` gives the source table row counts for each build.
Use `data_source` to separate live API records from archive records.

## Build

```sh
uv sync --extra dev
.venv/bin/python -m football_prediction_data.build --refresh-teams
```

Omit `--refresh-teams` to use the cached ESPN directories.

The default source directory is:

```text
~/Code/nfl-prediction-market/data
```

Use `--source` to select a different source directory.

The local scheduler runs this build each hour. The source collectors run every
five minutes. These jobs run only while the computer is awake.

## Run the collectors

Run one current capture for each source:

```sh
cd ../nfl-prediction-market
.venv/bin/python -m src.capture_kalshi_football capture
.venv/bin/python -m src.capture_polymarket_nfl capture
.venv/bin/python -m src.capture_polymarket_nfl --sport cfb capture
ODDS_API_KEY=... .venv/bin/python -m src.capture_novig
```

Return to this repository. Then rebuild the Parquet files:

```sh
cd ../football-prediction-market-data
.venv/bin/python -m football_prediction_data.build
```

## Load all trades

### DuckDB

```sql
SELECT *
FROM read_parquet(
  'data/curated/trades/**/*.parquet',
  hive_partitioning = true,
  union_by_name = true
);
```

Create reusable views:

```sh
duckdb football.duckdb < sql/create_views.sql
```

Then query all venues:

```sql
SELECT venue, league, count(*) AS trades, sum(size) AS contracts
FROM trades
GROUP BY ALL
ORDER BY venue, league;
```

Load only the Becker NFL trade slice:

```sql
SELECT *
FROM read_parquet(
  'data/curated/trades/league=nfl/venue=kalshi/season=2025/*.parquet',
  hive_partitioning = true,
  union_by_name = true
)
WHERE data_source = 'becker_archive';
```

### Python

```python
import duckdb

trades = duckdb.sql("""
  SELECT * FROM read_parquet(
    'data/curated/trades/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
  )
""").df()
```

For one small table:

```python
import pandas as pd

teams = pd.read_parquet("data/curated/teams")
markets = pd.read_parquet("data/curated/markets")
cfb_markets = pd.read_parquet("data/curated/markets/league=cfb")
```

### R

```r
library(arrow)
library(dplyr)

trades <- open_dataset(
  "data/curated/trades",
  partitioning = hive_partition()
)

summary <- trades |>
  group_by(venue, league) |>
  summarise(trades = n(), contracts = sum(size, na.rm = TRUE)) |>
  collect()
```

## Team joins

Use `team_id` for all joins. Do not join with a team name.

```sql
SELECT
  m.venue,
  away.display_name AS away_team,
  home.display_name AS home_team,
  m.market_type,
  m.line
FROM markets m
LEFT JOIN teams away ON m.away_team_id = away.team_id
LEFT JOIN teams home ON m.home_team_id = home.team_id;
```

See [SCHEMA.md](SCHEMA.md) for the full schema. See
[DATA_SOURCES.md](DATA_SOURCES.md) for source and recovery notes.
