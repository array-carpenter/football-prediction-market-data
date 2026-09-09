"""Build normalized Parquet datasets from the local capture databases."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .teams import TeamResolver, load_espn_json, normalize_name


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = Path.home()/"Code"/"nfl-prediction-market"/"data"
ESPN = {
    "cfb": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams?limit=1000",
    "nfl": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams?limit=100",
}
SCHEMA_VERSION = "1.2.0"


def download(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["curl","-fsS",url,"-o",str(path)],check=True)


def source_team_tables(kalshi: sqlite3.Connection, pm_sources: list[tuple[str,sqlite3.Connection]],
                       novig: sqlite3.Connection,
                       resolver: TeamResolver) -> tuple[pd.DataFrame, dict, dict]:
    rows = []
    kalshi_map = {}
    sql = """SELECT ticker,series_ticker,yes_sub_title,raw_json FROM markets
             WHERE series_ticker IN ('KXNCAAFGAME','KXNFLGAME')"""
    for ticker, series, name, raw in kalshi.execute(sql):
        obj = json.loads(raw)
        source_id = (obj.get("custom_strike") or {}).get("football_team")
        if not source_id:
            continue
        league = "cfb" if "NCAAF" in series else "nfl"
        suffix = ticker.rsplit("-",1)[-1]
        match = resolver.resolve(name,league,suffix)
        if not match.team_id:
            match = type(match)(f"kalshi:{league}:{source_id}","kalshi_uuid_fallback",None)
        prior = kalshi_map.get((league,source_id))
        if prior and prior != match.team_id:
            raise ValueError(f"Kalshi team UUID maps to two teams: {source_id}")
        kalshi_map[(league,source_id)] = match.team_id
        rows.append({"venue":"kalshi","league":league,"source_team_id":source_id,
                     "source_name":name,"source_abbreviation":suffix,"team_id":match.team_id,
                     "match_method":match.method,"match_score":match.score})
    pm_map = {}
    kalshi_name_lookup={(row["league"],normalize_name(row["source_name"])):row["team_id"]
                         for row in rows if row["venue"]=="kalshi"}
    for source_league,pm in pm_sources:
      for event_id, raw in pm.execute("SELECT id,raw_json FROM events"):
        obj=json.loads(raw)
        for team in obj.get("teams") or []:
            league=str(team.get("league") or source_league).lower()
            if league not in ("nfl","cfb"):
                continue
            source_id=str(team["id"])
            source_name=team.get("alias") or team.get("name")
            match=resolver.resolve(source_name,league,team.get("abbreviation"))
            if not match.team_id:
                shared=kalshi_name_lookup.get((league,normalize_name(source_name)))
                if shared:
                    match=type(match)(shared,"kalshi_name_crosswalk",1.0)
                else:
                    match=type(match)(f"polymarket:{league}:{source_id}","polymarket_id_fallback",None)
            pm_map[(league,source_id)]=match.team_id
            rows.append({"venue":"polymarket","league":league,"source_team_id":source_id,
                         "source_name":source_name,"source_abbreviation":team.get("abbreviation"),
                         "team_id":match.team_id,"match_method":match.method,"match_score":match.score})
    for league,away,home in novig.execute(
      "SELECT DISTINCT league,away_team,home_team FROM events"):
        for source_name in (away,home):
            if not source_name:
                continue
            match=resolver.resolve(source_name,league)
            source_id=normalize_name(source_name)
            if not match.team_id:
                match=type(match)(f"novig:{league}:{source_id}","novig_name_fallback",None)
            rows.append({"venue":"novig","league":league,"source_team_id":source_id,
                         "source_name":source_name,"source_abbreviation":None,
                         "team_id":match.team_id,"match_method":match.method,
                         "match_score":match.score})
    return pd.DataFrame(rows).drop_duplicates(),kalshi_map,pm_map


MATCHUP_RE = re.compile(r"(.+?) vs\.? (.+?) (?:college|professional|pro) football game",re.I)


def parse_matchup(rule: str) -> tuple[str,str] | None:
    match=MATCHUP_RE.search(rule or "")
    if not match: return None
    left=match.group(1).rsplit(" the ",1)[-1]
    return left.strip(),match.group(2).strip()


def market_type(question: str, source_type: str) -> str:
    text=(question or "").lower()
    source=(source_type or "").lower()
    if "spread" in text or "spread" in source: return "spread"
    if "o/u" in text or "total" in source: return "total"
    if text.startswith("will "): return "other"
    if "game" in source or re.fullmatch(r".+?\s+vs\.?\s+.+",text): return "moneyline"
    return "other"


def number_in(text: object) -> float | None:
    values=re.findall(r"[-+]?\d+(?:\.\d+)?",str(text or ""))
    return float(values[-1]) if values else None


def kalshi_league(series: str) -> str:
    """Map a Kalshi football series to its league."""
    return "cfb" if re.search(r"NCAAF|NCAA|CFB|CFP|HEISMAN",series or "",re.I) else "nfl"


BECKER_GAME_RE=re.compile(r"(.+?) (?:vs\.?|at) (.+?) Winner\?",re.I)
BECKER_TOTAL_RE=re.compile(r"(.+?) at (.+?):",re.I)


def build_becker_markets(source: Path,resolver: TeamResolver) -> pd.DataFrame:
    """Standardize the local Becker-derived Kalshi NFL market slice."""
    path=source/"kalshi_nfl_markets.parquet"
    if not path.exists(): return pd.DataFrame()
    frame=pd.read_parquet(path)
    game_map={}
    for row in frame[frame.ticker.str.startswith("KXNFLGAME-")].itertuples():
        match=BECKER_GAME_RE.fullmatch(str(row.title))
        if match:
            game_map[str(row.event_ticker).split("-",1)[-1]]=(match.group(1),match.group(2))
    rows=[]
    for row in frame.itertuples():
        series=str(row.ticker).split("-",1)[0]
        event_key=str(row.event_ticker).split("-",1)[-1]
        names=game_map.get(event_key)
        if not names:
            total_match=BECKER_TOTAL_RE.match(str(row.title))
            if total_match: names=(total_match.group(1),total_match.group(2))
        away=resolver.resolve(names[0],"nfl").team_id if names else None
        home=resolver.resolve(names[1],"nfl").team_id if names else None
        if series=="KXNFLGAME": kind="moneyline"
        elif series=="KXNFLSPREAD" and names: kind="spread"
        elif series=="KXNFLTOTAL" and names: kind="total"
        else: kind="other"
        suffix=str(row.ticker).rsplit("-",1)[-1]
        outcome_team=resolver.resolve(None,"nfl",suffix).team_id if kind=="moneyline" else None
        if kind=="spread":
            selected=str(row.title).split(" wins by",1)[0]
            outcome_team=resolver.resolve(selected,"nfl").team_id
        close=pd.to_datetime(row.close_time,errors="coerce")
        season=int(close.year-(close.month<=3)) if not pd.isna(close) else 2025
        rows.append({"venue":"kalshi","league":"nfl","season":season,
          "market_id":str(row.ticker),"event_id":str(row.event_ticker),"asset_id":None,
          "market_type":kind,"question":row.title,"outcome_label":suffix,
          "outcome_team_id":outcome_team,"away_team_id":away,"home_team_id":home,
          "line":number_in(row.title) if kind in ("spread","total") else None,
          "open_time":row.open_time,"close_time":row.close_time,"status":row.status,
          "result":row.result,"volume":row.volume,"liquidity":None,"open_interest":None,
          "source_team_id":suffix if outcome_team else None,
          "series_ticker":series,"source_market_type":series,
          "metadata_quality":"source",
          "data_source":"becker_archive"})
    trade_path=source/"kalshi_nfl_trades.parquet"
    if trade_path.exists():
        import duckdb
        trades=duckdb.sql(f"""SELECT ticker,min(created_time) open_time,
          max(created_time) close_time,sum(count) volume
          FROM read_parquet('{sql_path(trade_path)}') GROUP BY ticker""").df()
        known={row["market_id"] for row in rows}
        groups={}
        for ticker in trades.ticker:
            event=ticker.rsplit("-",1)[0]
            groups.setdefault(event,[]).append(ticker.rsplit("-",1)[-1])
        for trade in trades.itertuples():
            ticker=str(trade.ticker)
            if ticker in known: continue
            event=ticker.rsplit("-",1)[0]
            suffix=ticker.rsplit("-",1)[-1]
            code=re.sub(r"^KXNFLGAME-\d{2}[A-Z]{3}\d{2}","",event)
            sides=list(dict.fromkeys(groups[event]))
            away_suffix=home_suffix=None
            if len(sides)==2:
                if code==sides[0]+sides[1]: away_suffix,home_suffix=sides
                elif code==sides[1]+sides[0]: away_suffix,home_suffix=reversed(sides)
            away=resolver.resolve(None,"nfl",away_suffix).team_id
            home=resolver.resolve(None,"nfl",home_suffix).team_id
            outcome_team=resolver.resolve(None,"nfl",suffix).team_id
            rows.append({"venue":"kalshi","league":"nfl","season":2025,
              "market_id":ticker,"event_id":event,"asset_id":None,
              "market_type":"moneyline" if away and home else "other",
              "question":f"{away_suffix} at {home_suffix} Winner?",
              "outcome_label":suffix,"outcome_team_id":outcome_team,
              "away_team_id":away,"home_team_id":home,"line":None,
              "open_time":trade.open_time,"close_time":trade.close_time,
              "status":"closed","result":None,"volume":trade.volume,
              "liquidity":None,"open_interest":None,"source_team_id":suffix,
              "series_ticker":"KXNFLGAME","source_market_type":"KXNFLGAME",
              "metadata_quality":"synthetic_from_ticker",
              "data_source":"becker_archive"})
    return pd.DataFrame(rows)


def build_markets(kalshi: sqlite3.Connection, pm_sources: list[tuple[str,sqlite3.Connection]],
                  novig: sqlite3.Connection,
                  resolver: TeamResolver, kalshi_team_map: dict,
                  source_teams: pd.DataFrame, source: Path) -> pd.DataFrame:
    out=[]
    source_names={}
    for row in source_teams[source_teams.venue.eq("kalshi")].itertuples():
        source_names[(row.league,normalize_name(row.source_name))]=row.team_id
    def resolve_kalshi_name(name: str, league: str):
        return source_names.get((league,normalize_name(name))) or resolver.resolve(name,league).team_id
    for row in kalshi.execute("""SELECT ticker,series_ticker,event_ticker,title,yes_sub_title,
      open_time,close_time,status,result,raw_json FROM markets"""):
        ticker,series,event,title,outcome,opened,closed,status,result,raw=row
        league=kalshi_league(series)
        obj=json.loads(raw); rule=obj.get("rules_primary","")
        matchup=parse_matchup(rule)
        away=resolve_kalshi_name(matchup[0],league) if matchup else None
        home=resolve_kalshi_name(matchup[1],league) if matchup else None
        source_team=(obj.get("custom_strike") or {}).get("football_team")
        outcome_team=kalshi_team_map.get((league,source_team))
        core_type=market_type(title,series) if series in {
          "KXNCAAFGAME","KXNCAAFSPREAD","KXNCAAFTOTAL",
          "KXNFLGAME","KXNFLSPREAD","KXNFLTOTAL"} else "other"
        out.append({"venue":"kalshi","league":league,"season":2026,"market_id":ticker,
          "event_id":event,"asset_id":None,"market_type":core_type,
          "question":title,"outcome_label":outcome,"outcome_team_id":outcome_team,
          "away_team_id":away,"home_team_id":home,"line":number_in(title) if "GAME" not in series else None,
          "open_time":opened,"close_time":closed,"status":status,"result":result,
          "volume":obj.get("volume_fp"),"liquidity":obj.get("liquidity_dollars"),
          "open_interest":obj.get("open_interest_fp"),"source_team_id":source_team,
          "series_ticker":series,"source_market_type":series,
          "metadata_quality":"source",
          "data_source":"kalshi_api"})
    for source_league,pm in pm_sources:
      event_json={str(event_id):json.loads(raw) for event_id,raw in
                  pm.execute("SELECT id,raw_json FROM events")}
      for row in pm.execute("SELECT id,event_id,condition_id,question,slug,market_type,group_item_title,game_start_time,start_date,end_date,active,closed,volume,liquidity,open_interest,raw_json FROM markets"):
        mid,eid,condition,question,slug,stype,group,start_game,started,ended,active,closed,volume,liq,oi,raw=row
        teams=event_json.get(str(eid),{}).get("teams") or []
        league=source_league
        away=home=None
        for team in teams:
            resolved=resolver.resolve(team.get("alias") or team.get("name"),league,team.get("abbreviation")).team_id
            if team.get("ordering")=="away": away=resolved
            elif team.get("ordering")=="home": home=resolved
        event_title=event_json.get(str(eid),{}).get("title") or ""
        title_match=re.fullmatch(r"(.+?)\s+vs\.?\s+(.+)",event_title,re.I)
        if title_match:
            away=away or resolver.resolve(title_match.group(1),league).team_id
            home=home or resolver.resolve(title_match.group(2),league).team_id
        obj=json.loads(raw); outcomes=json.loads(obj.get("outcomes") or "[]"); assets=json.loads(obj.get("clobTokenIds") or "[]")
        for index,asset in enumerate(assets):
            label=outcomes[index] if index<len(outcomes) else None
            outcome_team=resolver.resolve(label,league).team_id
            out.append({"venue":"polymarket","league":league,"season":2026,"market_id":str(mid),
              "event_id":str(eid),"asset_id":str(asset),"market_type":market_type(question,stype or group),
              "question":question,"outcome_label":label,"outcome_team_id":outcome_team,
              "away_team_id":away,"home_team_id":home,"line":number_in(group),
              "open_time":started,"close_time":ended,"status":"closed" if closed else "open",
              "result":None,"volume":volume,"liquidity":liq,"open_interest":oi,
              "source_team_id":None,"series_ticker":None,
              "source_market_type":stype or group,"metadata_quality":"source",
              "data_source":"polymarket_api"})
    latest_novig={row[0]:row[1] for row in novig.execute("""SELECT market_id,line FROM (
      SELECT market_id,line,row_number() OVER (PARTITION BY market_id ORDER BY captured_at DESC) rank
      FROM market_snapshots) WHERE rank=1""")}
    novig_events={(row[0],row[1]):row[2:] for row in novig.execute(
      "SELECT event_id,league,commence_time,away_team,home_team,first_seen_at FROM events")}
    type_map={"h2h":"moneyline","spreads":"spread","totals":"total"}
    for mid,eid,league,stype,outcome,first_seen,last_seen in novig.execute(
      "SELECT market_id,event_id,league,market_type,outcome_name,first_seen_at,last_seen_at FROM markets"):
        commence,away_name,home_name,event_first=novig_events[(eid,league)]
        away=resolver.resolve(away_name,league).team_id
        home=resolver.resolve(home_name,league).team_id
        out.append({"venue":"novig","league":league,"season":2026,"market_id":mid,
          "event_id":eid,"asset_id":None,"market_type":type_map.get(stype,"other"),
          "question":f"{away_name} vs. {home_name} {stype}","outcome_label":outcome,
          "outcome_team_id":resolver.resolve(outcome,league).team_id,
          "away_team_id":away,"home_team_id":home,"line":latest_novig.get(mid),
          "open_time":event_first or first_seen,"close_time":commence,"status":"open",
          "result":None,"volume":None,"liquidity":None,"open_interest":None,
          "source_team_id":normalize_name(outcome) if outcome not in ("Over","Under") else None,
          "series_ticker":None,"source_market_type":stype,"metadata_quality":"source",
          "data_source":"the_odds_api"})
    frame=pd.DataFrame(out)
    frame["_event_key"]=frame.event_id.astype(str).str.split("-",n=1).str[-1]
    for column in ("away_team_id","home_team_id"):
        known=(frame.dropna(subset=[column]).drop_duplicates(["league","_event_key"])
               .set_index(["league","_event_key"])[column])
        missing=frame[column].isna()
        frame.loc[missing,column]=[
          known.get((league,key)) for league,key in
          zip(frame.loc[missing,"league"],frame.loc[missing,"_event_key"])
        ]
    frame=frame.drop(columns="_event_key")
    for col in ("line","volume","liquidity","open_interest"):
        frame[col]=pd.to_numeric(frame[col],errors="coerce")
    becker=build_becker_markets(source,resolver)
    if not becker.empty:
        frame=pd.concat([frame,becker],ignore_index=True,sort=False)
        frame["_contract_key"]=frame.asset_id.fillna(frame.market_id)
        frame=frame.drop_duplicates(
          ["venue","league","_contract_key"],keep="first").drop(columns="_contract_key")
    incomplete=(frame.market_type.isin(["moneyline","spread","total"]) &
                (frame.away_team_id.isna() | frame.home_team_id.isna()))
    frame.loc[incomplete,"market_type"]="other"
    for column in ("open_time","close_time"):
        frame[column]=pd.to_datetime(frame[column],errors="coerce",utc=True).astype("string")
    return frame


def write_trade_partitions(source: Path, output: Path, markets: pd.DataFrame) -> None:
    lookup=markets[["venue","league","market_id","event_id","asset_id","market_type",
                    "outcome_team_id","away_team_id","home_team_id"]].copy()
    for venue,league_filter,db_name,query,key in (
      ("kalshi",None,"kalshi_football.sqlite","SELECT trade_id,ticker market_id,NULL asset_id,created_time traded_at,yes_price_dollars price,count_fp size,taker_outcome_side side FROM trades","market_id"),
      ("polymarket","nfl","polymarket_nfl.sqlite","SELECT trade_key trade_id,condition_id,asset_id,datetime(timestamp,'unixepoch') || 'Z' traded_at,price,size,side FROM trades","asset_id"),
      ("polymarket","cfb","polymarket_cfb.sqlite","SELECT trade_key trade_id,condition_id,asset_id,datetime(timestamp,'unixepoch') || 'Z' traded_at,price,size,side FROM trades","asset_id")):
        con=sqlite3.connect(source/db_name)
        venue_lookup=lookup[lookup.venue==venue].drop_duplicates(key)
        if league_filter: venue_lookup=venue_lookup[venue_lookup.league.eq(league_filter)]
        writers={}
        for chunk in pd.read_sql_query(query,con,chunksize=200_000):
            chunk[key]=chunk[key].astype(str)
            merged=chunk.merge(venue_lookup,on=key,how="left",suffixes=("","_market"))
            missing=merged["league"].isna()
            if missing.any():
                examples=merged.loc[missing,key].drop_duplicates().head(5).tolist()
                raise ValueError(f"{venue} trades have unknown {key} values: {examples}")
            merged["venue"]=venue
            merged["data_source"]="kalshi_api" if venue=="kalshi" else "polymarket_api"
            merged["price"]=pd.to_numeric(merged.price,errors="coerce")
            if venue=="kalshi": merged["price"]=merged.price
            merged["size"]=pd.to_numeric(merged["size"],errors="coerce")
            for league,part in merged.groupby("league",dropna=False):
                if pd.isna(league): continue
                path=output/f"league={league}"/f"venue={venue}"/"season=2026"/"part-000.parquet"
                path.parent.mkdir(parents=True,exist_ok=True)
                stored=part.drop(columns=["venue","league","season"],errors="ignore")
                table=pa.Table.from_pandas(stored,preserve_index=False)
                if league not in writers: writers[league]=pq.ParquetWriter(path,table.schema,compression="zstd")
                writers[league].write_table(table)
        for writer in writers.values(): writer.close()
        con.close()


def sql_path(path: Path) -> str:
    """Escape a local path for a DuckDB SQL string literal."""
    return str(path).replace("'","''")


def write_becker_trade_partition(source: Path, output: Path,
                                  markets: pd.DataFrame) -> None:
    """Write the Becker NFL trade slice without loading it into memory."""
    path=source/"kalshi_nfl_trades.parquet"
    if not path.exists(): return
    import duckdb
    lookup=(markets[markets.venue.eq("kalshi") & markets.league.eq("nfl")]
      [["market_id","event_id","market_type","outcome_team_id",
        "away_team_id","home_team_id"]].drop_duplicates("market_id"))
    target=output/"league=nfl"/"venue=kalshi"/"season=2025"/"part-becker.parquet"
    target.parent.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect()
    con.register("market_lookup",lookup)
    con.execute(f"""COPY (
      SELECT
        'becker:' || sha256(concat_ws('|',t.ticker,CAST(t.count AS VARCHAR),
          CAST(t.yes_price AS VARCHAR),CAST(t.no_price AS VARCHAR),
          coalesce(t.taker_side,''),CAST(t.created_time AS VARCHAR))) AS trade_id,
        t.ticker AS market_id,
        NULL::VARCHAR AS asset_id,
        CAST(t.created_time AS VARCHAR) AS traded_at,
        CAST(t.yes_price AS DOUBLE)/100.0 AS price,
        CAST(t.count AS DOUBLE) AS size,
        t.taker_side AS side,
        m.event_id,m.market_type,m.outcome_team_id,m.away_team_id,m.home_team_id,
        'becker_archive' AS data_source
      FROM read_parquet('{sql_path(path)}') t
      INNER JOIN market_lookup m ON t.ticker=m.market_id
    ) TO '{sql_path(target)}' (FORMAT PARQUET,COMPRESSION ZSTD)""")
    source_count=con.execute(
      f"SELECT count(*) FROM read_parquet('{sql_path(path)}')").fetchone()[0]
    output_count=con.execute(
      f"SELECT count(*) FROM read_parquet('{sql_path(target)}')").fetchone()[0]
    con.close()
    if source_count!=output_count:
        raise ValueError(
          f"Becker trade join lost rows: source={source_count}, output={output_count}")


def write_becker_snapshot_partition(source: Path, curated: Path,
                                     markets: pd.DataFrame) -> None:
    """Standardize the Becker NFL market snapshot slice."""
    path=source/"kalshi_nfl_snapshots.parquet"
    if not path.exists(): return
    frame=pd.read_parquet(path)
    lookup=(markets[markets.venue.eq("kalshi") & markets.league.eq("nfl")]
      [["market_id","event_id","market_type","away_team_id","home_team_id"]]
      .drop_duplicates("market_id"))
    frame=frame.rename(columns={"ticker":"market_id","created_time":"captured_at"})
    frame=frame.merge(lookup,on="market_id",how="left")
    if frame.event_id.isna().any():
        raise ValueError("Becker snapshots contain unknown markets")
    for source_column,target_column in (
      ("yes_bid","best_bid"),("yes_ask","best_ask"),("last_price","last_price")):
        frame[target_column]=pd.to_numeric(frame[source_column],errors="coerce")/100.0
    frame["venue"]="kalshi"; frame["data_source"]="becker_archive"
    frame["season"]=2025; frame["status"]="closed"
    target=(curated/"market_snapshots"/"league=nfl"/"venue=kalshi"/
            "season=2025"/"part-becker.parquet")
    target.parent.mkdir(parents=True,exist_ok=True)
    frame.drop(columns=["venue","league","season"],errors="ignore").to_parquet(
      target,index=False,compression="zstd")


def write_frame_partitions(frame: pd.DataFrame, root: Path,
                           partition_columns: tuple[str,...]=("league","venue","season")) -> None:
    if root.exists(): shutil.rmtree(root)
    for keys,part in frame.groupby(list(partition_columns)):
        if not isinstance(keys,tuple): keys=(keys,)
        path=root
        for column,value in zip(partition_columns,keys):
            rendered=str(int(value)) if column=="season" else str(value)
            path=path/f"{column}={rendered}"
        path=path/"part-000.parquet"
        path.parent.mkdir(parents=True,exist_ok=True)
        stored=part.drop(columns=list(partition_columns),errors="ignore")
        stored.to_parquet(path,index=False,compression="zstd")


def write_state_partitions(source: Path, curated: Path, markets: pd.DataFrame) -> None:
    base=markets.drop_duplicates(["venue","market_id"])[
      ["venue","league","season","market_id","event_id","market_type",
       "away_team_id","home_team_id"]]
    kalshi=sqlite3.connect(source/"kalshi_football.sqlite")
    ks=pd.read_sql_query("""SELECT captured_at,ticker market_id,status,
      yes_bid_dollars best_bid,yes_ask_dollars best_ask,yes_bid_size_fp best_bid_size,
      yes_ask_size_fp best_ask_size,last_price_dollars last_price,volume_fp volume,
      volume_24h_fp volume_24h,open_interest_fp open_interest,raw_json FROM market_snapshots""",kalshi)
    ks["venue"]="kalshi"; ks["data_source"]="kalshi_api"
    ks["outcome_prices"]=None; ks["liquidity"]=None
    kalshi.close()
    pm_connections=[]; pm_snapshots=[]; pm_books=[]
    for league,db_name in (("nfl","polymarket_nfl.sqlite"),("cfb","polymarket_cfb.sqlite")):
      pm=sqlite3.connect(source/db_name); pm_connections.append(pm)
      ps=pd.read_sql_query("""SELECT captured_at,CAST(market_id AS TEXT) market_id,
      outcome_prices,volume,volume_24h,liquidity,open_interest,best_bid,best_ask,
      last_trade_price last_price,NULL best_bid_size,NULL best_ask_size,NULL status,raw_json
      FROM market_snapshots""",pm)
      ps["venue"]="polymarket"; ps["data_source"]="polymarket_api"; pm_snapshots.append(ps)
      book=pd.read_sql_query("SELECT * FROM order_books",pm)
      book["_league"]=league; book["data_source"]="polymarket_api"; pm_books.append(book)
    novig=sqlite3.connect(source/"novig_football.sqlite")
    ns=pd.read_sql_query("""SELECT captured_at,market_id,NULL status,NULL best_bid,NULL best_ask,
      NULL best_bid_size,NULL best_ask_size,implied_probability last_price,NULL volume,
      NULL volume_24h,NULL liquidity,NULL open_interest,NULL outcome_prices,
      decimal_odds,line,raw_json FROM market_snapshots""",novig)
    ns["venue"]="novig"; ns["data_source"]="the_odds_api"; novig.close()
    snapshots=pd.concat([ks,*pm_snapshots,ns],ignore_index=True,sort=False).merge(
      base,on=["venue","market_id"],how="left")
    for column in ("best_bid","best_ask","best_bid_size","best_ask_size","last_price",
                   "volume","volume_24h","liquidity","open_interest"):
        snapshots[column]=pd.to_numeric(snapshots[column],errors="coerce")
    write_frame_partitions(snapshots,curated/"market_snapshots")
    books=pd.concat(pm_books,ignore_index=True)
    for column in ("best_bid","best_ask","bid_depth","ask_depth","min_order_size",
                   "tick_size","last_trade_price"):
        books[column]=pd.to_numeric(books[column],errors="coerce")
    token_map=markets[markets.venue.eq("polymarket")].drop_duplicates("asset_id")[
      ["asset_id","league","season","market_id","event_id","market_type",
       "away_team_id","home_team_id"]]
    books.asset_id=books.asset_id.astype(str); token_map.asset_id=token_map.asset_id.astype(str)
    books=books.merge(token_map,on=["asset_id"],how="left"); books["venue"]="polymarket"
    books=books.drop(columns="_league")
    write_frame_partitions(books,curated/"order_books")
    for pm in pm_connections: pm.close()


def parquet_row_count(path: Path) -> int:
    """Return the row count without loading the Parquet data."""
    return pq.ParquetFile(path).metadata.num_rows


def write_manifests(source: Path, output: Path) -> None:
    """Write build and source inventories for reproducibility checks."""
    curated=output/"curated"
    generated_at=datetime.now(timezone.utc).isoformat()
    dataset_rows=[]
    for name in ("teams","team_aliases","source_teams","markets","trades",
                 "market_snapshots","order_books"):
        for path in sorted((curated/name).glob("**/*.parquet")):
            parts={part.split("=",1)[0]:part.split("=",1)[1]
                   for part in path.parts if "=" in part}
            dataset_rows.append({
              "schema_version":SCHEMA_VERSION,"generated_at":generated_at,
              "dataset":name,"venue":parts.get("venue"),"league":parts.get("league"),
              "season":int(float(parts["season"])) if parts.get("season") else None,
              "relative_path":str(path.relative_to(output)),
              "row_count":parquet_row_count(path),"size_bytes":path.stat().st_size,
            })
    pd.DataFrame(dataset_rows).to_parquet(
      curated/"dataset_manifest.parquet",index=False,compression="zstd")

    source_rows=[]
    for path in sorted(source.glob("*.sqlite")):
        con=sqlite3.connect(f"file:{path}?mode=ro",uri=True)
        tables=[row[0] for row in con.execute(
          "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        for table in tables:
            quoted=table.replace('"','""')
            count=con.execute(f'SELECT count(*) FROM "{quoted}"').fetchone()[0]
            source_rows.append({
              "schema_version":SCHEMA_VERSION,"generated_at":generated_at,
              "source_file":path.name,"source_table":table,"row_count":count,
              "file_size_bytes":path.stat().st_size,
              "modified_at":datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat(),
            })
        con.close()
    for path in sorted(source.glob("*.parquet")):
        row_count=parquet_row_count(path) if path.stat().st_size else 0
        source_rows.append({
          "schema_version":SCHEMA_VERSION,"generated_at":generated_at,
          "source_file":path.name,"source_table":"parquet",
          "row_count":row_count,"file_size_bytes":path.stat().st_size,
          "modified_at":datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat(),
        })
    pd.DataFrame(source_rows).to_parquet(
      curated/"source_manifest.parquet",index=False,compression="zstd")


def build(source: Path, output: Path, refresh_teams: bool) -> None:
    reference=output/"reference"
    for league,url in ESPN.items():
        path=reference/f"espn_{league}_teams.json"
        if refresh_teams or not path.exists(): download(url,path)
    team_frames=[]; alias_frames=[]
    for league in ESPN:
        t,a=load_espn_json(reference/f"espn_{league}_teams.json",league)
        team_frames.append(t); alias_frames.append(a)
    teams=pd.concat(team_frames,ignore_index=True); aliases=pd.concat(alias_frames,ignore_index=True)
    resolver=TeamResolver(teams,aliases)
    kalshi=sqlite3.connect(source/"kalshi_football.sqlite")
    pm_sources=[("nfl",sqlite3.connect(source/"polymarket_nfl.sqlite")),
                ("cfb",sqlite3.connect(source/"polymarket_cfb.sqlite"))]
    novig=sqlite3.connect(source/"novig_football.sqlite")
    source_teams,kalshi_map,_=source_team_tables(kalshi,pm_sources,novig,resolver)
    fallback=source_teams[source_teams.team_id.str.match(
      r"^(kalshi|polymarket|novig):",na=False)].drop_duplicates("team_id")
    if not fallback.empty:
        extra_teams=pd.DataFrame({
          "team_id":fallback.team_id,"espn_team_id":None,"league":fallback.league,
          "display_name":fallback.source_name,"short_name":fallback.source_name,
          "location":fallback.source_name,"nickname":None,"abbreviation":fallback.source_abbreviation,
          "slug":fallback.source_name.map(normalize_name).str.replace(" ","-"),"logo_url":None})
        extra_aliases=pd.concat([
          pd.DataFrame({"league":fallback.league,"team_id":fallback.team_id,"alias":fallback.source_name,
            "normalized_alias":fallback.source_name.map(normalize_name),"alias_type":"source_name"}),
          pd.DataFrame({"league":fallback.league,"team_id":fallback.team_id,"alias":fallback.source_abbreviation,
            "normalized_alias":fallback.source_abbreviation.map(normalize_name),"alias_type":"source_abbreviation"})
        ],ignore_index=True)
        teams=pd.concat([teams,extra_teams],ignore_index=True)
        aliases=pd.concat([aliases,extra_aliases],ignore_index=True).drop_duplicates()
    source_aliases=pd.concat([
      pd.DataFrame({"league":source_teams.league,"team_id":source_teams.team_id,
        "alias":source_teams.source_name,"normalized_alias":source_teams.source_name.map(normalize_name),
        "alias_type":source_teams.venue+"_source_name"}),
      pd.DataFrame({"league":source_teams.league,"team_id":source_teams.team_id,
        "alias":source_teams.source_abbreviation,
        "normalized_alias":source_teams.source_abbreviation.map(normalize_name),
        "alias_type":source_teams.venue+"_source_abbreviation"})
    ],ignore_index=True)
    aliases=pd.concat([aliases,source_aliases],ignore_index=True).drop_duplicates()
    resolver=TeamResolver(teams,aliases)
    markets=build_markets(kalshi,pm_sources,novig,resolver,kalshi_map,source_teams,source)
    game=markets[markets.market_type.isin(["moneyline","spread","total"])]
    unresolved=game[game.away_team_id.isna() | game.home_team_id.isna()]
    duplicate_sides=game[game.away_team_id.eq(game.home_team_id)]
    source_unresolved=source_teams[source_teams.team_id.isna()]
    audit=output/"audit"; audit.mkdir(parents=True,exist_ok=True)
    unresolved.to_csv(audit/"unresolved_game_markets.csv",index=False)
    duplicate_sides.to_csv(audit/"duplicate_side_game_markets.csv",index=False)
    source_unresolved.to_csv(audit/"unresolved_source_teams.csv",index=False)
    if len(unresolved) or len(duplicate_sides) or len(source_unresolved):
        raise ValueError(
          f"Team validation failed: unresolved games={len(unresolved)}, "
          f"same-team games={len(duplicate_sides)}, unresolved source teams={len(source_unresolved)}")
    curated=output/"curated"; curated.mkdir(parents=True,exist_ok=True)
    for old_name in ("teams.parquet","team_aliases.parquet","source_teams.parquet",
                     "markets.parquet"):
        (curated/old_name).unlink(missing_ok=True)
    write_frame_partitions(teams,curated/"teams",("league",))
    write_frame_partitions(aliases,curated/"team_aliases",("league",))
    write_frame_partitions(source_teams,curated/"source_teams",("league",))
    write_frame_partitions(markets,curated/"markets",("league","season"))
    trades_dir=curated/"trades"
    if trades_dir.exists(): shutil.rmtree(trades_dir)
    write_trade_partitions(source,trades_dir,markets)
    write_becker_trade_partition(source,trades_dir,markets)
    write_state_partitions(source,curated,markets)
    write_becker_snapshot_partition(source,curated,markets)
    write_manifests(source,output)
    kalshi.close()
    for _,pm in pm_sources: pm.close()
    novig.close()
    print(f"teams={len(teams):,} aliases={len(aliases):,} source_teams={len(source_teams):,}")
    print(f"markets={len(markets):,} unresolved_game_markets={len(unresolved):,}")


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",type=Path,default=DEFAULT_SOURCE)
    parser.add_argument("--output",type=Path,default=ROOT/"data")
    parser.add_argument("--refresh-teams",action="store_true")
    args=parser.parse_args(); build(args.source,args.output,args.refresh_teams)


if __name__=="__main__": main()
