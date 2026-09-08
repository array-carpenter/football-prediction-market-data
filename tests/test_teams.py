import pandas as pd

from football_prediction_data.teams import TeamResolver, normalize_name


def resolver():
    return TeamResolver(
        pd.read_parquet("data/curated/teams.parquet"),
        pd.read_parquet("data/curated/team_aliases.parquet"),
    )


def test_name_normalization():
    assert normalize_name("Appalachian St.") == "appalachian state"


def test_app_state_aliases_share_one_team():
    r=resolver()
    expected="espn:cfb:2026"
    assert r.resolve("App State","cfb").team_id==expected
    assert r.resolve("Appalachian State","cfb").team_id==expected
    assert r.resolve("APP","cfb").team_id==expected


def test_georgia_aliases_share_one_team():
    r=resolver()
    assert r.resolve("Georgia","cfb").team_id=="espn:cfb:61"
    assert r.resolve("UGA","cfb").team_id=="espn:cfb:61"
