# Football Prediction Market Data

This repository builds a clean football prediction-market dataset from the
local Kalshi and Polymarket collectors.

The curated data uses the same team identifier for both venues. ESPN team IDs
are the primary identifiers. A stable Kalshi UUID identifier is used only when
ESPN does not list a team.

## Current data

The build includes college football and NFL Kalshi data. It also includes NFL
Polymarket data.

The primary files are:

- `data/curated/teams.parquet`
- `data/curated/team_aliases.parquet`
- `data/curated/source_teams.parquet`
- `data/curated/markets.parquet`
- `data/curated/trades/**/*.parquet`
- `data/curated/market_snapshots/**/*.parquet`
- `data/curated/order_books/**/*.parquet`

The directory datasets use Hive partitions. The partition fields are `venue`,
`league`, and `season`.

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

Then query both venues:

```sql
SELECT venue, league, count(*) AS trades, sum(size) AS contracts
FROM trades
GROUP BY ALL
ORDER BY venue, league;
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

teams = pd.read_parquet("data/curated/teams.parquet")
markets = pd.read_parquet("data/curated/markets.parquet")
```

### R

```r
library(arrow)
library(dplyr)

trades <- open_dataset(
  "data/curated/trades",
  partitioning = c("venue", "league", "season")
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
