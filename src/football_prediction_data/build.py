"""Build normalized Parquet datasets from the local capture databases."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import subprocess
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


def download(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["curl","-fsS",url,"-o",str(path)],check=True)


def source_team_tables(kalshi: sqlite3.Connection, pm: sqlite3.Connection,
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
    for event_id, raw in pm.execute("SELECT id,raw_json FROM events"):
        obj=json.loads(raw)
        for team in obj.get("teams") or []:
            league=str(team.get("league") or "nfl").lower()
            if league not in ("nfl","cfb"):
                continue
            source_id=str(team["id"])
            match=resolver.resolve(team.get("name") or team.get("alias"),league,team.get("abbreviation"))
            pm_map[(league,source_id)]=match.team_id
            rows.append({"venue":"polymarket","league":league,"source_team_id":source_id,
                         "source_name":team.get("name"),"source_abbreviation":team.get("abbreviation"),
                         "team_id":match.team_id,"match_method":match.method,"match_score":match.score})
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


def build_markets(kalshi: sqlite3.Connection, pm: sqlite3.Connection,
                  resolver: TeamResolver, kalshi_team_map: dict,
                  source_teams: pd.DataFrame) -> pd.DataFrame:
    out=[]
    source_names={}
    for row in source_teams[source_teams.venue.eq("kalshi")].itertuples():
        source_names[(row.league,normalize_name(row.source_name))]=row.team_id
    def resolve_kalshi_name(name: str, league: str):
        return source_names.get((league,normalize_name(name))) or resolver.resolve(name,league).team_id
    for row in kalshi.execute("""SELECT ticker,series_ticker,event_ticker,title,yes_sub_title,
      open_time,close_time,status,result,raw_json FROM markets"""):
        ticker,series,event,title,outcome,opened,closed,status,result,raw=row
        league="cfb" if "NCAAF" in series else "nfl"
        obj=json.loads(raw); rule=obj.get("rules_primary","")
        matchup=parse_matchup(rule)
        away=resolve_kalshi_name(matchup[0],league) if matchup else None
        home=resolve_kalshi_name(matchup[1],league) if matchup else None
        source_team=(obj.get("custom_strike") or {}).get("football_team")
        outcome_team=kalshi_team_map.get((league,source_team))
        out.append({"venue":"kalshi","league":league,"season":2026,"market_id":ticker,
          "event_id":event,"asset_id":None,"market_type":market_type(title,series),
          "question":title,"outcome_label":outcome,"outcome_team_id":outcome_team,
          "away_team_id":away,"home_team_id":home,"line":number_in(title) if "GAME" not in series else None,
          "open_time":opened,"close_time":closed,"status":status,"result":result,
          "volume":obj.get("volume_fp"),"liquidity":obj.get("liquidity_dollars"),
          "open_interest":obj.get("open_interest_fp"),"source_team_id":source_team})
    for row in pm.execute("SELECT id,event_id,condition_id,question,slug,market_type,group_item_title,game_start_time,start_date,end_date,active,closed,volume,liquidity,open_interest,raw_json FROM markets"):
        mid,eid,condition,question,slug,stype,group,start_game,started,ended,active,closed,volume,liq,oi,raw=row
        event_raw=pm.execute("SELECT raw_json FROM events WHERE id=?",(eid,)).fetchone()
        teams=(json.loads(event_raw[0]).get("teams") or []) if event_raw else []
        league="nfl"
        away=home=None
        for team in teams:
            resolved=resolver.resolve(team.get("name"),league,team.get("abbreviation")).team_id
            if team.get("ordering")=="away": away=resolved
            elif team.get("ordering")=="home": home=resolved
        obj=json.loads(raw); outcomes=json.loads(obj.get("outcomes") or "[]"); assets=json.loads(obj.get("clobTokenIds") or "[]")
        for index,asset in enumerate(assets):
            label=outcomes[index] if index<len(outcomes) else None
            outcome_team=resolver.resolve(label,league).team_id
            out.append({"venue":"polymarket","league":league,"season":2026,"market_id":str(mid),
              "event_id":str(eid),"asset_id":str(asset),"market_type":market_type(question,stype or group),
              "question":question,"outcome_label":label,"outcome_team_id":outcome_team,
              "away_team_id":away,"home_team_id":home,"line":number_in(group),
              "open_time":started,"close_time":ended,"status":"closed" if closed else "open",
              "result":None,"volume":volume,"liquidity":liq,"open_interest":oi,"source_team_id":None})
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
    return frame


def write_trade_partitions(source: Path, output: Path, markets: pd.DataFrame) -> None:
    lookup=markets[["venue","league","market_id","event_id","asset_id","market_type",
                    "outcome_team_id","away_team_id","home_team_id"]].copy()
    for venue,db_name,query,key in (
      ("kalshi","kalshi_football.sqlite","SELECT trade_id,ticker market_id,NULL asset_id,created_time traded_at,yes_price_dollars price,count_fp size,taker_outcome_side side FROM trades","market_id"),
      ("polymarket","polymarket_nfl.sqlite","SELECT trade_key trade_id,condition_id,asset_id,datetime(timestamp,'unixepoch') traded_at,price,size,side FROM trades","asset_id")):
        con=sqlite3.connect(source/db_name)
        venue_lookup=lookup[lookup.venue==venue].drop_duplicates(key)
        writers={}
        for chunk in pd.read_sql_query(query,con,chunksize=200_000):
            chunk[key]=chunk[key].astype(str)
            merged=chunk.merge(venue_lookup,on=key,how="left",suffixes=("","_market"))
            merged["venue"]=venue
            merged["price"]=pd.to_numeric(merged.price,errors="coerce")
            if venue=="kalshi": merged["price"]=merged.price
            merged["size"]=pd.to_numeric(merged["size"],errors="coerce")
            for league,part in merged.groupby("league",dropna=False):
                if pd.isna(league): continue
                path=output/f"venue={venue}"/f"league={league}"/"season=2026"/"part-000.parquet"
                path.parent.mkdir(parents=True,exist_ok=True)
                table=pa.Table.from_pandas(part, preserve_index=False)
                if league not in writers: writers[league]=pq.ParquetWriter(path,table.schema,compression="zstd")
                writers[league].write_table(table)
        for writer in writers.values(): writer.close()
        con.close()


def write_frame_partitions(frame: pd.DataFrame, root: Path) -> None:
    if root.exists(): shutil.rmtree(root)
    for (venue,league),part in frame.groupby(["venue","league"]):
        path=root/f"venue={venue}"/f"league={league}"/"season=2026"/"part-000.parquet"
        path.parent.mkdir(parents=True,exist_ok=True)
        part.to_parquet(path,index=False,compression="zstd")


def write_state_partitions(source: Path, curated: Path, markets: pd.DataFrame) -> None:
    base=markets.drop_duplicates(["venue","market_id"])[
      ["venue","league","season","market_id","event_id","market_type",
       "away_team_id","home_team_id"]]
    kalshi=sqlite3.connect(source/"kalshi_football.sqlite")
    ks=pd.read_sql_query("""SELECT captured_at,ticker market_id,status,
      yes_bid_dollars best_bid,yes_ask_dollars best_ask,yes_bid_size_fp best_bid_size,
      yes_ask_size_fp best_ask_size,last_price_dollars last_price,volume_fp volume,
      volume_24h_fp volume_24h,open_interest_fp open_interest,raw_json FROM market_snapshots""",kalshi)
    ks["venue"]="kalshi"; ks["outcome_prices"]=None; ks["liquidity"]=None
    kalshi.close()
    pm=sqlite3.connect(source/"polymarket_nfl.sqlite")
    ps=pd.read_sql_query("""SELECT captured_at,CAST(market_id AS TEXT) market_id,
      outcome_prices,volume,volume_24h,liquidity,open_interest,best_bid,best_ask,
      last_trade_price last_price,NULL best_bid_size,NULL best_ask_size,NULL status,raw_json
      FROM market_snapshots""",pm)
    ps["venue"]="polymarket"
    snapshots=pd.concat([ks,ps],ignore_index=True,sort=False).merge(base,on=["venue","market_id"],how="left")
    for column in ("best_bid","best_ask","best_bid_size","best_ask_size","last_price",
                   "volume","volume_24h","liquidity","open_interest"):
        snapshots[column]=pd.to_numeric(snapshots[column],errors="coerce")
    write_frame_partitions(snapshots,curated/"market_snapshots")
    books=pd.read_sql_query("SELECT * FROM order_books",pm)
    for column in ("best_bid","best_ask","bid_depth","ask_depth","min_order_size",
                   "tick_size","last_trade_price"):
        books[column]=pd.to_numeric(books[column],errors="coerce")
    token_map=markets[markets.venue.eq("polymarket")].drop_duplicates("asset_id")[
      ["asset_id","league","season","market_id","event_id","market_type",
       "away_team_id","home_team_id"]]
    books.asset_id=books.asset_id.astype(str); token_map.asset_id=token_map.asset_id.astype(str)
    books=books.merge(token_map,on="asset_id",how="left"); books["venue"]="polymarket"
    write_frame_partitions(books,curated/"order_books")
    pm.close()


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
    pm=sqlite3.connect(source/"polymarket_nfl.sqlite")
    source_teams,kalshi_map,_=source_team_tables(kalshi,pm,resolver)
    fallback=source_teams[source_teams.team_id.str.startswith("kalshi:",na=False)].drop_duplicates("team_id")
    if not fallback.empty:
        extra_teams=pd.DataFrame({
          "team_id":fallback.team_id,"espn_team_id":None,"league":fallback.league,
          "display_name":fallback.source_name,"short_name":fallback.source_name,
          "location":fallback.source_name,"nickname":None,"abbreviation":fallback.source_abbreviation,
          "slug":fallback.source_name.map(normalize_name).str.replace(" ","-"),"logo_url":None})
        extra_aliases=pd.concat([
          pd.DataFrame({"league":fallback.league,"team_id":fallback.team_id,"alias":fallback.source_name,
            "normalized_alias":fallback.source_name.map(normalize_name),"alias_type":"kalshi_source_name"}),
          pd.DataFrame({"league":fallback.league,"team_id":fallback.team_id,"alias":fallback.source_abbreviation,
            "normalized_alias":fallback.source_abbreviation.map(normalize_name),"alias_type":"kalshi_abbreviation"})
        ],ignore_index=True)
        teams=pd.concat([teams,extra_teams],ignore_index=True)
        aliases=pd.concat([aliases,extra_aliases],ignore_index=True).drop_duplicates()
        resolver=TeamResolver(teams,aliases)
    markets=build_markets(kalshi,pm,resolver,kalshi_map,source_teams)
    curated=output/"curated"; curated.mkdir(parents=True,exist_ok=True)
    teams.to_parquet(curated/"teams.parquet",index=False)
    aliases.to_parquet(curated/"team_aliases.parquet",index=False)
    source_teams.to_parquet(curated/"source_teams.parquet",index=False)
    markets.to_parquet(curated/"markets.parquet",index=False)
    trades_dir=curated/"trades"
    if trades_dir.exists(): shutil.rmtree(trades_dir)
    write_trade_partitions(source,trades_dir,markets)
    write_state_partitions(source,curated,markets)
    kalshi.close(); pm.close()
    game=markets[markets.market_type.isin(["moneyline","spread","total"])]
    unresolved=game[game.away_team_id.isna() | game.home_team_id.isna()]
    source_unresolved=source_teams[source_teams.team_id.isna()]
    audit=output/"audit"; audit.mkdir(parents=True,exist_ok=True)
    unresolved.to_csv(audit/"unresolved_game_markets.csv",index=False)
    source_unresolved.to_csv(audit/"unresolved_source_teams.csv",index=False)
    print(f"teams={len(teams):,} aliases={len(aliases):,} source_teams={len(source_teams):,}")
    print(f"markets={len(markets):,} unresolved_game_markets={len(unresolved):,}")


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",type=Path,default=DEFAULT_SOURCE)
    parser.add_argument("--output",type=Path,default=ROOT/"data")
    parser.add_argument("--refresh-teams",action="store_true")
    args=parser.parse_args(); build(args.source,args.output,args.refresh_teams)


if __name__=="__main__": main()
