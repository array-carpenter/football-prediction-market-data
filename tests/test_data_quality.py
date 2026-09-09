from pathlib import Path

import duckdb


DATA=Path("data/curated")
MARKETS=f"{DATA}/markets/**/*.parquet"
TEAMS=f"{DATA}/teams/**/*.parquet"
SOURCE_TEAMS=f"{DATA}/source_teams/**/*.parquet"


def test_all_game_markets_have_two_teams():
    count=duckdb.sql(f"""SELECT count(*) FROM read_parquet('{MARKETS}',hive_partitioning=true)
      WHERE market_type IN ('moneyline','spread','total')
      AND (away_team_id IS NULL OR home_team_id IS NULL)""").fetchone()[0]
    assert count==0


def test_game_team_ids_exist_in_registry():
    count=duckdb.sql(f"""WITH m AS (SELECT away_team_id id FROM read_parquet('{MARKETS}',hive_partitioning=true)
      UNION ALL SELECT home_team_id FROM read_parquet('{MARKETS}',hive_partitioning=true)),
      t AS (SELECT team_id FROM read_parquet('{TEAMS}',hive_partitioning=true))
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


def test_game_markets_do_not_use_one_team_twice():
    count=duckdb.sql(f"""SELECT count(*) FROM read_parquet('{MARKETS}',hive_partitioning=true)
      WHERE market_type IN ('moneyline','spread','total')
      AND away_team_id=home_team_id""").fetchone()[0]
    assert count==0


def test_market_contract_ids_are_unique():
    count=duckdb.sql(f"""SELECT count(*) FROM (
      SELECT venue,league,coalesce(asset_id,market_id) contract_id,count(*) n
      FROM read_parquet('{MARKETS}',hive_partitioning=true)
      GROUP BY ALL HAVING n>1)""").fetchone()[0]
    assert count==0


def test_trades_reference_known_contracts():
    count=duckdb.sql(f"""WITH t AS (
      SELECT venue,league,market_id,asset_id
      FROM read_parquet('{DATA}/trades/**/*.parquet',hive_partitioning=true,union_by_name=true)
    ), m AS (
      SELECT venue,league,market_id,asset_id
      FROM read_parquet('{MARKETS}',hive_partitioning=true)
    )
    SELECT count(*) FROM t LEFT JOIN m
      ON t.venue=m.venue AND t.league=m.league
      AND CASE WHEN t.venue='kalshi' THEN t.market_id=m.market_id ELSE t.asset_id=m.asset_id END
    WHERE m.market_id IS NULL""").fetchone()[0]
    assert count==0


def test_source_team_keys_have_one_canonical_team():
    count=duckdb.sql(f"""SELECT count(*) FROM (
      SELECT venue,league,source_team_id,count(DISTINCT team_id) n
      FROM read_parquet('{SOURCE_TEAMS}',hive_partitioning=true)
      GROUP BY ALL HAVING n<>1)""").fetchone()[0]
    assert count==0


def test_manifest_row_counts_match_files():
    rows=duckdb.sql(f"""SELECT relative_path,row_count
      FROM read_parquet('{DATA}/dataset_manifest.parquet')""").fetchall()
    assert rows
    for relative_path,expected in rows:
        actual=duckdb.sql(
          f"SELECT count(*) FROM read_parquet('data/{relative_path}')"
        ).fetchone()[0]
        assert actual==expected, relative_path


def test_each_dataset_has_separate_league_partitions():
    for dataset in ("teams","team_aliases","source_teams","markets","trades",
                    "market_snapshots"):
        assert (DATA/dataset/"league=cfb").is_dir(), dataset
        assert (DATA/dataset/"league=nfl").is_dir(), dataset


def test_novig_prices_are_present_and_valid():
    count=duckdb.sql(f"""SELECT count(*)
      FROM read_parquet('{DATA}/market_snapshots/**/*.parquet',
        hive_partitioning=true,union_by_name=true)
      WHERE venue='novig' AND decimal_odds>1 AND last_price>0 AND last_price<1""").fetchone()[0]
    assert count>0
