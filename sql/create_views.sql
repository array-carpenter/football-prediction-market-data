CREATE OR REPLACE VIEW teams AS
SELECT * FROM read_parquet('data/curated/teams.parquet');

CREATE OR REPLACE VIEW team_aliases AS
SELECT * FROM read_parquet('data/curated/team_aliases.parquet');

CREATE OR REPLACE VIEW source_teams AS
SELECT * FROM read_parquet('data/curated/source_teams.parquet');

CREATE OR REPLACE VIEW markets AS
SELECT * FROM read_parquet('data/curated/markets.parquet');

CREATE OR REPLACE VIEW trades AS
SELECT * FROM read_parquet(
  'data/curated/trades/**/*.parquet',
  hive_partitioning = true,
  union_by_name = true
);

CREATE OR REPLACE VIEW market_snapshots AS
SELECT * FROM read_parquet(
  'data/curated/market_snapshots/**/*.parquet',
  hive_partitioning = true,
  union_by_name = true
);

CREATE OR REPLACE VIEW order_books AS
SELECT * FROM read_parquet(
  'data/curated/order_books/**/*.parquet',
  hive_partitioning = true,
  union_by_name = true
);
