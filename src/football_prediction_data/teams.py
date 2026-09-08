"""Canonical ESPN team registry and source-name resolution."""

from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher

import pandas as pd


MANUAL_ALIASES = {
    "cfb": {
        "appalachian state": "2026",
        "appalachian st": "2026",
        "app st": "2026",
        "uga": "61",
        "georgia": "61",
        "ga southern": "290",
        "georgia southern": "290",
        "ga state": "2247",
        "florida st": "52",
        "fla state": "52",
        "miami fl": "2390",
        "miami florida": "2390",
        "miami oh": "193",
        "ole miss": "145",
        "ucf": "2116",
        "usc": "30",
        "lsu": "99",
        "byu": "252",
        "smu": "2567",
        "tcu": "2628",
        "utsa": "2636",
        "utep": "2638",
        "uab": "5",
        "glenville state college pioneers": "2249",
        "glenville state": "2249",
        "university at albany": "399",
        "ualbany": "399",
        "georgetown college tigers": "2245",
        "georgetown college": "2245",
        "roosevelt": "599",
        "north carolina state": "152",
        "north carolina st": "152",
        "nc state": "152",
        "charlotte": "2429",
        "troy": "2653",
    },
    "nfl": {
        "ny giants": "19",
        "new york g": "19",
        "ny jets": "20",
        "new york j": "20",
        "la rams": "14",
        "los angeles r": "14",
        "la chargers": "24",
        "los angeles c": "24",
        "jac jaguars": "30",
    },
}


def normalize_name(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\bst\.?\b", "state", text)
    text = re.sub(r"\bmt\.?\b", "mount", text)
    text = re.sub(r"\buniv\.?\b|\buniversity\b", "", text)
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def registry_from_espn(payload: dict, league: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    teams = []
    raw_aliases = []
    for wrapper in payload["sports"][0]["leagues"][0]["teams"]:
        team = wrapper["team"]
        espn_id = str(team["id"])
        team_id = f"espn:{league}:{espn_id}"
        teams.append({
            "team_id": team_id, "espn_team_id": espn_id, "league": league,
            "display_name": team.get("displayName"), "short_name": team.get("shortDisplayName"),
            "location": team.get("location"), "nickname": team.get("name"),
            "abbreviation": team.get("abbreviation"), "slug": team.get("slug"),
            "logo_url": (team.get("logos") or [{}])[0].get("href"),
        })
        values = {
            "display_name": team.get("displayName"), "short_name": team.get("shortDisplayName"),
            "location": team.get("location"), "nickname": team.get("nickname"),
            "abbreviation": team.get("abbreviation"), "slug": str(team.get("slug", "")).replace("-", " "),
            "abbreviation_nickname": f"{team.get('abbreviation','')} {team.get('name','')}".strip(),
        }
        for alias_type, alias in values.items():
            if alias:
                raw_aliases.append({"league": league, "team_id": team_id,
                                    "alias": alias, "normalized_alias": normalize_name(alias),
                                    "alias_type": f"espn_{alias_type}"})
    for alias, espn_id in MANUAL_ALIASES.get(league, {}).items():
        raw_aliases.append({"league": league, "team_id": f"espn:{league}:{espn_id}",
                            "alias": alias, "normalized_alias": normalize_name(alias),
                            "alias_type": "manual"})
    return pd.DataFrame(teams), pd.DataFrame(raw_aliases).drop_duplicates()


@dataclass
class Match:
    team_id: str | None
    method: str
    score: float | None


class TeamResolver:
    def __init__(self, teams: pd.DataFrame, aliases: pd.DataFrame):
        self.teams = teams
        self.aliases = aliases
        self.lookup: dict[str, dict[str, set[str]]] = {}
        self.manual_lookup: dict[str, dict[str, str]] = {}
        for league, group in aliases.groupby("league"):
            index: dict[str, set[str]] = defaultdict(set)
            for row in group.itertuples():
                if row.normalized_alias:
                    index[row.normalized_alias].add(row.team_id)
            self.lookup[league] = index
            manual = group[group.alias_type.eq("manual")]
            self.manual_lookup[league] = dict(zip(manual.normalized_alias,manual.team_id))

    def resolve(self, value: object, league: str, abbreviation: object = None) -> Match:
        index = self.lookup.get(league, {})
        manual = self.manual_lookup.get(league, {})
        for candidate, method in ((value, "name_exact"), (abbreviation, "abbreviation_exact")):
            key = normalize_name(candidate)
            if key in manual:
                return Match(manual[key], "manual_alias", 1.0)
            hits = index.get(key, set())
            if len(hits) == 1:
                return Match(next(iter(hits)), method, 1.0)
        key = normalize_name(value)
        if not key:
            return Match(None, "unresolved", None)
        scored = []
        for alias, hits in index.items():
            if len(hits) != 1:
                continue
            scored.append((SequenceMatcher(None, key, alias).ratio(), next(iter(hits))))
        scored.sort(reverse=True)
        if scored and scored[0][0] >= 0.92 and (len(scored) == 1 or scored[0][0] - scored[1][0] >= 0.04):
            return Match(scored[0][1], "name_fuzzy", round(scored[0][0], 4))
        return Match(None, "unresolved", scored[0][0] if scored else None)


def load_espn_json(path, league: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    return registry_from_espn(json.loads(path.read_text()), league)
