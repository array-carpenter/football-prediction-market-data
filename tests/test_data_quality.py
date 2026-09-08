from pathlib import Path

import duckdb


DATA=Path("data/curated")


def test_all_game_markets_have_two_teams():
    count=duckdb.sql(f"""SELECT count(*) FROM read_parquet('{DATA}/markets.parquet')
      WHERE market_type IN ('moneyline','spread','total')
      AND (away_team_id IS NULL OR home_team_id IS NULL)""").fetchone()[0]
    assert count==0


def test_game_team_ids_exist_in_registry():
    count=duckdb.sql(f"""WITH m AS (SELECT away_team_id id FROM read_parquet('{DATA}/markets.parquet')
      UNION ALL SELECT home_team_id FROM read_parquet('{DATA}/markets.parquet')),
      t AS (SELECT team_id FROM read_parquet('{DATA}/teams.parquet'))
      SELECT count(*) FROM m LEFT JOIN t ON m.id=t.team_id WHERE m.id IS NOT NULL AND t.team_id IS NULL""").fetchone()[0]
    assert count==0


def test_trade_prices_are_probabilities():
    count=duckdb.sql(f"""SELECT count(*) FROM read_parquet('{DATA}/trades/**/*.parquet',
      hive_partitioning=true,union_by_name=true) WHERE price<0 OR price>1""").fetchone()[0]
    assert count==0


def test_trade_ids_are_unique_within_venue():
    count=duckdb.sql(f"""SELECT count(*) FROM (SELECT venue,trade_id,count(*) n
      FROM read_parquet('{DATA}/trades/**/*.parquet',hive_partitioning=true,union_by_name=true)
      GROUP BY venue,trade_id HAVING n>1)""").fetchone()[0]
    assert count==0
